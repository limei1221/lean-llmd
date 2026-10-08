"""Day 12 - EPP metrics and autoscaling.

Oracles: ``prometheus_client``'s parser for your text output, a real gRPC
stream for the running-request count, and on kind the whole loop: a real
Prometheus must scrape your EPP, and KEDA's HPA must scale the simulators
when your metric says demand is high.
"""

import json
import threading
import time
import urllib.parse
from concurrent import futures

import grpc
import httpx
import pytest
from envoy.config.core.v3 import base_pb2
from envoy.service.ext_proc.v3 import external_processor_pb2 as pb
from envoy.service.ext_proc.v3 import external_processor_pb2_grpc as pb_grpc
from prometheus_client.parser import text_string_to_metric_families

import k8s


@pytest.fixture
def ob(mod):
    return mod("observability")


def parsed(text):
    return {
        f.name: (f.type, {tuple(sorted(s.labels.items())): s.value for s in f.samples})
        for f in text_string_to_metric_families(text)
    }


def test_render_is_valid_prometheus_text(ob):
    r = ob.Registry()
    r.register("queue", "gauge", "Queued.")
    r.register("served_total", "counter", "Served.")
    r.set("queue", 3, {"pool": 'we"ird\\na\nme'})
    r.set("queue", 1.5)
    r.add("served_total", 2, {"model": "m"})
    r.add("served_total", 1, {"model": "m"})
    families = parsed(r.render())
    assert families["queue"] == ("gauge", {(): 1.5, (("pool", 'we"ird\\na\nme'),): 3.0})
    assert families["served"] == ("counter", {(("model", "m"),): 3.0}), "the parser drops _total"
    assert r.render() == r.render(), "stable output"


def test_the_epp_families(ob):
    families = parsed(ob.new_epp_registry().render())
    assert families[ob.REQUEST_RUNNING][0] == "gauge"
    assert families["llm_d_epp_request"][0] == "counter"  # the parser strips _total
    for name in (ob.READY_ENDPOINTS, ob.AVERAGE_QUEUE_SIZE, ob.FLOW_CONTROL_QUEUE_SIZE):
        assert families[name][0] == "gauge"


def test_pool_gauges(ob, mod):
    ds, metrics = mod("datastore"), mod("metrics")

    class Lister:
        def list_pods(self, ns, sel):
            return [
                {
                    "metadata": {"name": n, "namespace": "ns", "labels": {"app": "s"}},
                    "status": {
                        "podIP": f"10.0.0.{i}",
                        "conditions": [{"type": "Ready", "status": "True"}],
                    },
                }
                for i, n in enumerate(["a", "b"])
            ]

    store = ds.Datastore()
    store.pool_set(ds.EndpointPool("pool", "ns", {"app": "s"}, (8000,)), Lister())
    for e, (q, kv) in zip(store.pod_list(), [(2, 0.2), (4, 0.6)], strict=True):
        e.update_metrics(metrics.Metrics(waiting_queue_size=q, kv_cache_usage_percent=kv))
    r = ob.new_epp_registry()
    ob.refresh_pool_metrics(r, store, "pool")
    assert r.get(ob.READY_ENDPOINTS, {"name": "pool"}) == 2
    assert r.get(ob.AVERAGE_QUEUE_SIZE, {"name": "pool"}) == 3
    assert r.get(ob.AVERAGE_KV_CACHE, {"name": "pool"}) == pytest.approx(0.4)


class Fixed:
    def handle_request(self, request):
        return "10.0.0.5:8000"

    def any_endpoint(self):
        return "10.0.0.5:8000"


def test_running_counts_open_streams(ob, mod):
    registry = ob.new_epp_registry()
    counting = ob.CountingServer(mod("extproc").StreamingServer(Fixed()), registry)
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=2))
    pb_grpc.add_ExternalProcessorServicer_to_server(counting, server)
    port = server.add_insecure_port("127.0.0.1:0")
    server.start()
    seen = []
    labels = {"model_name": "mini/sim"}

    def stream():
        hdrs = base_pb2.HeaderMap(headers=[base_pb2.HeaderValue(key=":path", raw_value=b"/x")])
        yield pb.ProcessingRequest(request_headers=pb.HttpHeaders(headers=hdrs))
        yield pb.ProcessingRequest(
            request_body=pb.HttpBody(body=b'{"model": "mini/sim"}', end_of_stream=True)
        )
        time.sleep(0.3)
        seen.append(registry.get(ob.REQUEST_RUNNING, labels))

    try:
        with grpc.insecure_channel(f"127.0.0.1:{port}") as channel:
            list(pb_grpc.ExternalProcessorStub(channel).Process(stream()))
        time.sleep(0.1)
    finally:
        server.stop(0)
    assert seen == [1]
    assert registry.get(ob.REQUEST_RUNNING, labels) == 0
    assert registry.get(ob.REQUEST_TOTAL, labels) == 1


def test_the_metrics_endpoint(ob):
    registry = ob.new_epp_registry()
    registry.set(ob.READY_ENDPOINTS, 3, {"name": "pool"})
    server = ob.MetricsServer(0, registry)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    base = f"http://127.0.0.1:{server.server_address[1]}"
    try:
        r = httpx.get(base + "/metrics")
        assert r.status_code == 200 and r.headers["content-type"].startswith("text/plain")
        assert parsed(r.text)[ob.READY_ENDPOINTS][1] == {(("name", "pool"),): 3.0}
        assert httpx.get(base + "/other").status_code == 404
    finally:
        server.shutdown()


def test_manifests(ob, mod):
    deploy = mod("deploy")
    original = deploy.epp_deployment("epp", "ns", "pool")
    annotated = ob.scrape_annotations(original, port=9191)
    ann = annotated["spec"]["template"]["metadata"]["annotations"]
    assert ann == {
        "prometheus.io/scrape": "true",
        "prometheus.io/port": "9191",
        "prometheus.io/path": "/metrics",
    }
    assert {"name": "metrics", "containerPort": 9191, "protocol": "TCP"} in (
        annotated["spec"]["template"]["spec"]["containers"][0]["ports"]
    )
    assert "annotations" not in original["spec"]["template"]["metadata"]
    so = ob.scaled_object("sim", "ns", "sim", "http://prom", "sum(x)", 2, 1, 5)
    assert so["spec"]["scaleTargetRef"]["name"] == "sim"
    assert (so["spec"]["minReplicaCount"], so["spec"]["maxReplicaCount"]) == (1, 5)
    (trigger,) = so["spec"]["triggers"]
    assert trigger["type"] == "prometheus" and trigger["metricType"] == "AverageValue"
    assert trigger["metadata"] == {
        "serverAddress": "http://prom",
        "query": "sum(x)",
        "threshold": "2",
    }


PROMETHEUS = "/api/v1/namespaces/monitoring/services/prometheus-server:80/proxy"


def prometheus_query(promql):
    raw = k8s.kubectl(
        "get", "--raw", f"{PROMETHEUS}/api/v1/query?query={urllib.parse.quote(promql)}"
    )
    return json.loads(raw)["data"]["result"]


def addons_unavailable():
    if "keda" not in k8s.kubectl("get", "crd", "scaledobjects.keda.sh", check=False):
        return "KEDA is not installed; run: uv run lean-llmd addons monitoring"
    if "prometheus-server" not in k8s.kubectl("get", "svc", "-n", "monitoring", check=False):
        return "Prometheus is not installed; run: uv run lean-llmd addons monitoring"
    return None


@pytest.mark.kind
def test_keda_scales_on_your_metric(mod, ob, package):
    if reason := addons_unavailable():
        pytest.skip(reason)
    if not k8s.image_present_in_kind():
        pytest.skip("the lean-llmd:dev image is not in kind; run: uv run lean-llmd image")
    manifests, disc, deploy = mod("manifests"), mod("discovery"), mod("deploy")
    config = manifests.SimConfig(max_num_seqs=2, inter_token_latency_ms=50)
    flags = ("--feature", "observability")
    with k8s.temp_namespace("day12") as ns:
        epp = deploy.epp_deployment("epp", ns, "pool", package=package, extra_args=flags)
        k8s.apply(
            [
                manifests.sim_deployment(namespace=ns, replicas=1, config=config),
                disc.inference_pool("pool", ns, {"app": "sim"}, manifests.SIM_PORT, "epp"),
                *deploy.epp_rbac("epp", ns),
                deploy.envoy_config_map("epp", ns),
                ob.scrape_annotations(epp),
                deploy.epp_service("epp", ns),
            ]
        )
        k8s.kubectl("rollout", "status", "deployment/epp", "-n", ns, "--timeout=240s")
        k8s.wait_until(
            lambda: prometheus_query(f'{ob.READY_ENDPOINTS}{{namespace="{ns}"}}'),
            timeout=90,
            interval=3,
            what="Prometheus to scrape your EPP",
        )
        query = f'sum({ob.REQUEST_RUNNING}{{namespace="{ns}"}})'
        k8s.apply(
            [
                ob.scaled_object(
                    "sim",
                    ns,
                    "sim",
                    "http://prometheus-server.monitoring.svc",
                    query,
                    2,
                    min_replicas=1,
                    max_replicas=3,
                    stabilization_seconds=5,
                )
            ]
        )
        load = threading.Thread(
            target=k8s.run_python_in_cluster,
            daemon=True,
            args=(
                ns,
                """
            import httpx, threading
            def loop():
                for _ in range(40):
                    httpx.post("http://epp/v1/completions", timeout=60,
                               json={"model": "mini/sim", "prompt": "hi", "max_tokens": 40,
                                     "ignore_eos": True})
            ts = [threading.Thread(target=loop) for _ in range(8)]
            for t in ts: t.start()
            for t in ts: t.join()
        """,
            ),
        )
        load.start()
        k8s.wait_until(
            lambda: k8s.get_json("deployment", "sim", "-n", ns)["spec"]["replicas"] > 1,
            timeout=150,
            interval=3,
            what="KEDA's HPA to scale the simulators up",
        )
