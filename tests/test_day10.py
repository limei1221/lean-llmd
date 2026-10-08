"""Day 10 - flow control.

Oracles: hand-worked saturation values, the ordering rules (strict priority,
round-robin between flows, FCFS within one), a real gRPC stream for the
in-flight accounting, and on kind, the clock: under saturation, premium
requests must finish before the batch traffic that arrived ahead of them.
"""

import json
import threading
import time
from concurrent import futures

import grpc
import pytest
from envoy.config.core.v3 import base_pb2
from envoy.service.ext_proc.v3 import external_processor_pb2 as pb
from envoy.service.ext_proc.v3 import external_processor_pb2_grpc as pb_grpc

import k8s


@pytest.fixture
def fc(mod):
    return mod("flowcontrol")


@pytest.fixture
def make(mod):
    ds, metrics = mod("datastore"), mod("metrics")

    def endpoint(queue=0, kv=0.0, updated=100.0):
        meta = ds.EndpointMetadata(ds.NamespacedName("ns", "e"), "e", "10.0.0.1", 8000, {})
        m = metrics.Metrics(
            waiting_queue_size=queue, kv_cache_usage_percent=kv, update_time=updated
        )
        return ds.Endpoint(meta, m)

    return endpoint


def req(mod, rid, headers=None):
    return mod("scheduling").InferenceRequest(rid, "m", {"model": "m"}, dict(headers or {}))


class Fixed:
    def __init__(self, value):
        self.value = value

    def saturation(self, endpoints, now):
        return self.value


def test_utilization_detector(fc, make):
    d = fc.UtilizationDetector(queue_threshold=4, kv_threshold=0.5, staleness_s=2.0)
    assert d.saturation([make(queue=2, kv=0.1)], now=100.5) == pytest.approx(0.5)
    assert d.saturation([make(queue=0, kv=0.4)], now=100.5) == pytest.approx(0.8)
    mixed = [make(queue=4), make(queue=0), make(updated=90.0)]
    assert d.saturation(mixed, now=100.5) == pytest.approx((1 + 0 + 1) / 3), "stale counts as full"
    assert d.saturation([], now=100.5) == 1.0


def test_concurrency_detector(fc, make):
    load = [3]
    d = fc.ConcurrencyDetector(lambda: load[0], max_concurrency=2)
    assert d.saturation([make(), make()], now=0) == pytest.approx(0.75)
    load[0] = 4
    assert d.saturation([make(), make()], now=0) == 1.0
    assert d.saturation([], now=0) == 1.0


def test_round_robin_takes_turns(fc):
    rr = fc.RoundRobinFairness()
    a, b, c = (fc.FlowKey(x, 0) for x in "abc")
    assert [rr.pick(0, [c, a, b]) for _ in range(4)] == [a, b, c, a]
    assert rr.pick(0, [a, c]) == c, "skips the flow that has nothing queued"
    assert rr.pick(5, [b]) == b, "each band has its own cursor"


def controller(fc, saturation=0.0, ttl=60.0, inflight=None):
    clock = [1000.0]
    detector = Fixed(saturation)
    c = fc.FlowController(
        detector, lambda: ["endpoint"], ttl, clock=lambda: clock[0], inflight=inflight
    )
    return c, detector, clock


def test_strict_priority_then_fcfs(mod, fc):
    c, _, _ = controller(fc)
    low = fc.FlowKey("t", 0)
    high = fc.FlowKey("t", 10)
    first, second = c.enqueue(req(mod, "l1"), low), c.enqueue(req(mod, "l2"), low)
    urgent = c.enqueue(req(mod, "h1"), high)
    assert c.queue_size() == 3
    assert [c.dispatch_once() for _ in range(3)] == [urgent, first, second]
    assert c.dispatch_once() is None
    assert all(i.released.is_set() and not i.expired for i in (first, second, urgent))


def test_flows_in_a_band_take_turns(mod, fc):
    c, _, _ = controller(fc)
    noisy = [c.enqueue(req(mod, f"n{i}"), fc.FlowKey("noisy", 0)) for i in range(3)]
    quiet = c.enqueue(req(mod, "q"), fc.FlowKey("quiet", 0))
    order = [c.dispatch_once() for _ in range(4)]
    assert order.index(quiet) <= 1, "a quiet tenant must not wait behind a noisy one"
    assert [i for i in order if i is not quiet] == noisy


def test_nothing_leaves_a_saturated_pool(mod, fc):
    c, detector, _ = controller(fc, saturation=1.0)
    item = c.enqueue(req(mod, "r"), fc.FlowKey("t", 0))
    assert c.dispatch_once() is None and not item.released.is_set()
    detector.value = 0.99
    assert c.dispatch_once() is item


def test_a_burst_cannot_slip_through(mod, fc, make):
    """Released requests count at once, so the gate closes before their threads run."""
    inflight = fc.InflightCounter()
    detector = fc.ConcurrencyDetector(lambda: inflight.value, max_concurrency=2)
    c = fc.FlowController(detector, lambda: [make(), make()], inflight=inflight)
    items = [c.enqueue(req(mod, f"r{i}"), fc.FlowKey("t", 0)) for i in range(10)]
    while c.dispatch_once() is not None:
        pass
    assert sum(i.released.is_set() for i in items) == 4 and inflight.value == 4


def test_requests_expire(mod, fc):
    c, _, clock = controller(fc, saturation=1.0, ttl=5.0)
    old = c.enqueue(req(mod, "old"), fc.FlowKey("t", 0))
    clock[0] += 3
    young = c.enqueue(req(mod, "young"), fc.FlowKey("t", 0))
    clock[0] += 3
    assert c.expire() == 1
    assert old.released.is_set() and old.expired
    assert not young.released.is_set()


def test_objectives_and_flow_keys(mod, fc):
    store = fc.ObjectiveStore()
    store.update(
        [
            fc.inference_objective("premium", "ns", "pool", 100),
            {"metadata": {"name": "nopriority"}, "spec": {"poolRef": {"name": "pool"}}},
        ]
    )
    assert store.priority("premium") == 100 and store.priority("nopriority") == 0
    assert store.priority("unknown") == 0 and store.priority(None) == 0
    headers = {fc.FAIRNESS_ID_HEADER: "tenant-a", fc.OBJECTIVE_HEADER: "premium"}
    assert fc.flow_key(req(mod, "r", headers), store) == fc.FlowKey("tenant-a", 100)
    assert fc.flow_key(req(mod, "r"), store) == fc.FlowKey("default", 0)


class Inner:
    def __init__(self):
        self.calls = []

    def handle_request(self, request):
        self.calls.append(request.request_id)
        return "10.0.0.9:8000"

    def any_endpoint(self):
        return "10.0.0.9:8000"


def test_admission_blocks_then_counts_in_flight(mod, fc):
    inflight = fc.InflightCounter()
    c, detector, clock = controller(fc, saturation=1.0, ttl=60.0, inflight=inflight)
    inner = Inner()
    director = fc.FlowControlledDirector(inner, c, fc.ObjectiveStore(), inflight)
    result = {}
    t = threading.Thread(
        target=lambda: result.setdefault("target", director.handle_request(req(mod, "r")))
    )
    t.start()
    time.sleep(0.1)
    assert inner.calls == [] and "target" not in result, "a saturated pool must hold the request"
    detector.value = 0.0
    c.dispatch_once()
    t.join(2)
    assert result["target"] == "10.0.0.9:8000" and inflight.value == 1


def test_an_expired_request_is_a_429(mod, fc):
    epx = mod("extproc")
    c, _, clock = controller(fc, saturation=1.0, ttl=1.0)
    director = fc.FlowControlledDirector(Inner(), c, fc.ObjectiveStore(), fc.InflightCounter())
    threading.Timer(0.1, lambda: (clock.__setitem__(0, clock[0] + 5), c.expire())).start()
    with pytest.raises(epx.RequestRejected) as rejected:
        director.handle_request(req(mod, "r"))
    assert rejected.value.status == 429


def test_the_stream_closing_releases_the_slot(mod, fc):
    inflight = fc.InflightCounter()
    c, _, _ = controller(fc, inflight=inflight)
    director = fc.FlowControlledDirector(Inner(), c, fc.ObjectiveStore(), inflight)
    threading.Thread(target=c.run, args=(threading.Event(),), daemon=True).start()
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
    tracking = fc.TrackingServer(mod("extproc").StreamingServer(director), director)
    pb_grpc.add_ExternalProcessorServicer_to_server(tracking, server)
    port = server.add_insecure_port("127.0.0.1:0")
    server.start()
    seen = []

    def stream():
        hdrs = base_pb2.HeaderMap(
            headers=[base_pb2.HeaderValue(key=":path", raw_value=b"/v1/completions")]
        )
        yield pb.ProcessingRequest(request_headers=pb.HttpHeaders(headers=hdrs))
        yield pb.ProcessingRequest(
            request_body=pb.HttpBody(body=b'{"model": "m"}', end_of_stream=True)
        )
        time.sleep(0.3)  # the response is still streaming to the client
        seen.append(inflight.value)

    try:
        with grpc.insecure_channel(f"127.0.0.1:{port}") as channel:
            responses = list(pb_grpc.ExternalProcessorStub(channel).Process(stream()))
        time.sleep(0.1)
        assert responses[-1].WhichOneof("response") == "request_body"
        assert seen == [1], "in flight while the stream is open"
        assert inflight.value == 0, "released when it closes"
    finally:
        server.stop(0)


@pytest.mark.kind
def test_the_api_server_accepts_objectives(fc):
    obj = fc.inference_objective("premium", "default", "pool", 100)
    assert "created (server dry run)" in k8s.apply([obj], dry_run=True)


@pytest.mark.kind
def test_premium_overtakes_batch_under_saturation(mod, fc, package):
    if not k8s.image_present_in_kind():
        pytest.skip("the lean-llmd:dev image is not in kind; run: uv run lean-llmd image")
    manifests, disc, deploy = mod("manifests"), mod("discovery"), mod("deploy")
    config = manifests.SimConfig(max_num_seqs=2, inter_token_latency_ms=20)
    flags = ("--feature", "flowcontrol", "--max-concurrency", "2", "--prefix-weight", "0")
    with k8s.temp_namespace("day10") as ns:
        k8s.apply(
            [
                manifests.sim_deployment(namespace=ns, replicas=2, config=config),
                disc.inference_pool("pool", ns, {"app": "sim"}, manifests.SIM_PORT, "epp"),
                fc.inference_objective("premium", ns, "pool", 100),
                *deploy.epp_objects("epp", ns, "pool", package=package, extra_args=flags),
                *fc.objective_rbac("epp", ns),
            ]
        )
        k8s.kubectl("rollout", "status", "deployment/sim", "-n", ns, "--timeout=240s")
        k8s.kubectl("rollout", "status", "deployment/epp", "-n", ns, "--timeout=240s")
        time.sleep(6)  # the EPP lists objectives every 5 s
        out = k8s.run_python_in_cluster(
            ns,
            """
            import httpx, json, threading, time
            done = {}
            def send(name, objective):
                t = time.time()
                r = httpx.post("http://epp/v1/completions", timeout=120,
                    headers={"x-llm-d-inference-objective": objective},
                    json={"model": "mini/sim", "prompt": "hi", "max_tokens": 20,
                          "ignore_eos": True})
                done[name] = (r.status_code, time.time() - t)
            threads = [threading.Thread(target=send, args=(f"b{i}", "batch")) for i in range(16)]
            for t in threads: t.start()
            time.sleep(0.3)
            more = [threading.Thread(target=send, args=(f"p{i}", "premium")) for i in range(4)]
            for t in more: t.start()
            for t in threads + more: t.join()
            print(json.dumps(done))
        """,
        )
    done = json.loads(out.strip().splitlines()[-1])
    assert {code for code, _ in done.values()} == {200}
    premium = [s for name, (_, s) in done.items() if name.startswith("p")]
    batch = sorted(s for name, (_, s) in done.items() if name.startswith("b"))
    assert max(premium) < batch[-4], f"premium {premium} vs batch {batch}"
