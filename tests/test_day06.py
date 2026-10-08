"""Day 6 - the endpoint picker behind Envoy.

Oracles: the ext-proc protocol as Envoy defines it (a real gRPC round trip
with Envoy's own protobufs), the API server's authorizer for RBAC, and on
kind, real Envoy in front of real simulators whose own counters say which
replica served each request.
"""

import json
from concurrent import futures

import grpc
import httpx
import pytest
from envoy.config.core.v3 import base_pb2
from envoy.service.ext_proc.v3 import external_processor_pb2 as pb
from envoy.service.ext_proc.v3 import external_processor_pb2_grpc as pb_grpc

import k8s


def pod(name, ip, labels=None):
    return {
        "metadata": {"name": name, "namespace": "ns", "labels": labels or {"app": "sim"}},
        "status": {"podIP": ip, "conditions": [{"type": "Ready", "status": "True"}]},
    }


class Lister:
    def __init__(self, pods):
        self.pods = pods

    def list_pods(self, namespace, selector):
        return self.pods


@pytest.fixture
def x(mod):
    return mod("extproc")


@pytest.fixture
def store(mod):
    ds = mod("datastore")
    store = ds.Datastore()
    store.pool_set(
        ds.EndpointPool("pool", "ns", {"app": "sim"}, (8000,)),
        Lister([pod("a", "10.0.0.1"), pod("b", "10.0.0.2")]),
    )
    return store


@pytest.fixture
def director(mod, x, store):
    s, p = mod("scheduling"), mod("plugins")
    for e in store.pod_list():
        e.metrics.waiting_queue_size = 5 if e.metadata.pod_name == "a" else 0
    profile = s.SchedulerProfile(
        scorers=[s.WeightedScorer(p.QueueScorer(), 1.0)], picker=p.MaxScorePicker()
    )
    return x.Director(store, s.Scheduler(p.SingleProfileHandler(), {"default": profile}), [])


def test_parse_request(x):
    r = x.parse_request({"x-request-id": "abc"}, b'{"model": "m", "prompt": "hi"}')
    assert (r.request_id, r.target_model, r.body["prompt"]) == ("abc", "m", "hi")
    assert x.parse_request({}, b'{"model": "m"}').request_id, "a request needs an id"
    for bad in (b"not json", b"[1, 2]", b'{"prompt": "no model"}', b'{"model": 3}'):
        with pytest.raises(x.BadRequestError):
            x.parse_request({}, bad)


def test_the_routing_answer(x):
    for phase in ("request_headers", "request_body"):
        response = x.routing_response("10.0.0.7:8000", phase)
        assert response.WhichOneof("response") == phase
        common = getattr(response, phase).response
        assert common.clear_route_cache, "without it Envoy keeps the route it already picked"
        (header,) = common.header_mutation.set_headers
        assert header.header.key == "x-gateway-destination-endpoint"
        assert header.header.raw_value == b"10.0.0.7:8000"
        lb = response.dynamic_metadata.fields["envoy.lb"].struct_value
        assert lb.fields["x-gateway-destination-endpoint"].string_value == "10.0.0.7:8000"


def test_the_director_returns_the_best_ip_port(mod, x, director):
    s = mod("scheduling")
    request = s.InferenceRequest("r", "m", {"model": "m", "prompt": "hi"})
    assert director.handle_request(request) == "10.0.0.2:8000"


def test_the_director_runs_producers_around_scheduling(mod, x, store):
    s, p = mod("scheduling"), mod("plugins")
    calls = []

    class Spy:
        def produce(self, request, endpoints):
            calls.append(("produce", len(endpoints)))

        def pre_request(self, request, result):
            calls.append(("pre_request", result.primary().target_endpoints[0].metadata.pod_name))

    profile = s.SchedulerProfile(picker=p.MaxScorePicker())
    director = x.Director(store, s.Scheduler(p.SingleProfileHandler(), {"d": profile}), [Spy()])
    director.handle_request(s.InferenceRequest("r", "m", {"model": "m"}))
    assert calls == [("produce", 2), ("pre_request", "a")]


def test_an_empty_pool_is_no_endpoints(mod, x):
    s, p, ds = mod("scheduling"), mod("plugins"), mod("datastore")
    profile = s.SchedulerProfile(picker=p.MaxScorePicker())
    director = x.Director(ds.Datastore(), s.Scheduler(p.SingleProfileHandler(), {"d": profile}), [])
    with pytest.raises(s.NoEndpointsError):
        director.handle_request(s.InferenceRequest("r", "m", {"model": "m"}))
    with pytest.raises(s.NoEndpointsError):
        director.any_endpoint()


@pytest.fixture
def ext_proc(x, director):
    """Your StreamingServer behind a real gRPC server; yields a client stub."""
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
    pb_grpc.add_ExternalProcessorServicer_to_server(x.StreamingServer(director), server)
    port = server.add_insecure_port("127.0.0.1:0")
    server.start()
    with grpc.insecure_channel(f"127.0.0.1:{port}") as channel:
        yield pb_grpc.ExternalProcessorStub(channel)
    server.stop(0)


def headers_message(path, end_of_stream=False):
    pairs = [(":method", "POST"), (":path", path), ("content-type", "application/json")]
    return pb.ProcessingRequest(
        request_headers=pb.HttpHeaders(
            headers=base_pb2.HeaderMap(
                headers=[base_pb2.HeaderValue(key=k, raw_value=v.encode()) for k, v in pairs]
            ),
            end_of_stream=end_of_stream,
        )
    )


def body_message(body, end_of_stream=True):
    return pb.ProcessingRequest(request_body=pb.HttpBody(body=body, end_of_stream=end_of_stream))


def destination(response, phase):
    (header,) = getattr(response, phase).response.header_mutation.set_headers
    return header.header.raw_value.decode()


def test_a_post_is_routed_when_its_body_arrives(ext_proc):
    body = json.dumps({"model": "m", "prompt": "hello"}).encode()
    responses = list(
        ext_proc.Process(iter([headers_message("/v1/completions"), body_message(body)]))
    )
    assert [r.WhichOneof("response") for r in responses] == ["request_headers", "request_body"]
    assert not responses[0].request_headers.response.header_mutation.set_headers
    assert destination(responses[1], "request_body") == "10.0.0.2:8000"


def test_a_body_split_across_messages_is_reassembled(ext_proc):
    body = json.dumps({"model": "m", "prompt": "hello"}).encode()
    stream = [
        headers_message("/v1/completions"),
        body_message(body[:7], False),
        body_message(body[7:]),
    ]
    responses = list(ext_proc.Process(iter(stream)))
    assert destination(responses[-1], "request_body") == "10.0.0.2:8000"


def test_a_request_without_a_body_is_routed_at_its_headers(ext_proc):
    (response,) = list(ext_proc.Process(iter([headers_message("/v1/models", end_of_stream=True)])))
    assert destination(response, "request_headers") in {"10.0.0.1:8000", "10.0.0.2:8000"}


def test_a_bad_body_is_answered_with_400(ext_proc):
    responses = list(
        ext_proc.Process(iter([headers_message("/v1/completions"), body_message(b"{")]))
    )
    assert responses[-1].WhichOneof("response") == "immediate_response"
    assert responses[-1].immediate_response.status.code == 400


def test_no_endpoints_is_answered_with_503(mod, x):
    s, p, ds = mod("scheduling"), mod("plugins"), mod("datastore")
    profile = s.SchedulerProfile(picker=p.MaxScorePicker())
    empty = x.Director(ds.Datastore(), s.Scheduler(p.SingleProfileHandler(), {"d": profile}), [])
    stream = [headers_message("/v1/completions"), body_message(b'{"model": "m"}')]
    responses = list(x.StreamingServer(empty).Process(iter(stream), None))
    assert responses[-1].immediate_response.status.code == 503


def test_build_scheduler_follows_the_weights(mod):
    epp = mod("epp")
    _, producers, prefix = epp.build_scheduler(epp.Options("pool", "ns"))
    assert prefix is not None and producers == [prefix]
    _, producers, prefix = epp.build_scheduler(epp.Options("pool", "ns", prefix_weight=0))
    assert prefix is None and producers == []


def test_refresh_keeps_stale_metrics_when_a_scrape_fails(mod, store):
    epp = mod("epp")

    def handler(request):
        if request.url.host == "10.0.0.1":
            return httpx.Response(500)
        return httpx.Response(200, text='vllm:num_requests_waiting{model_name="m"} 4\n')

    for e in store.pod_list():
        e.metrics.waiting_queue_size = 9
    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        assert epp.refresh_metrics_once(store, client) == 1
    queues = {e.metadata.pod_name: e.metrics.waiting_queue_size for e in store.pod_list()}
    assert queues == {"a": 9, "b": 4}


def test_rbac_is_read_only(mod):
    deploy = mod("deploy")
    sa, role, binding = deploy.epp_rbac("epp", "ns")
    assert (sa["kind"], role["kind"], binding["kind"]) == ("ServiceAccount", "Role", "RoleBinding")
    grants = {
        (group, resource): set(rule["verbs"])
        for rule in role["rules"]
        for group in rule["apiGroups"]
        for resource in rule["resources"]
    }
    assert grants == {
        ("", "pods"): {"get", "list", "watch"},
        ("inference.networking.k8s.io", "inferencepools"): {"get", "list", "watch"},
    }


def test_envoy_and_the_epp_share_a_pod(mod):
    deploy = mod("deploy")
    d = deploy.epp_deployment("epp", "ns", "pool", package="pkg")
    spec = d["spec"]["template"]["spec"]
    containers = {c["name"]: c for c in spec["containers"]}
    assert set(containers) == {"epp", "envoy"}
    assert spec["serviceAccountName"] == "epp"
    epp = containers["epp"]
    assert epp["command"][-1] == "pkg.epp"
    assert epp["args"][:4] == ["--pool-name", "pool", "--pool-namespace", "ns"]
    assert epp["readinessProbe"]["grpc"]["port"] == 9002
    config_volume = next(v for v in spec["volumes"] if "configMap" in v)
    assert (
        config_volume["configMap"]["name"]
        == deploy.envoy_config_map("epp", "ns")["metadata"]["name"]
    )
    service = deploy.epp_service("epp", "ns")
    assert {p["targetPort"] for p in service["spec"]["ports"]} == {"http", "grpc"}


@pytest.fixture
def deployed(mod, package):
    """Simulators, a pool and your EPP (with Envoy) in a fresh namespace."""
    if not k8s.image_present_in_kind():
        pytest.skip("the lean-llmd:dev image is not in kind; run: uv run lean-llmd image")
    manifests, disc, deploy = mod("manifests"), mod("discovery"), mod("deploy")
    with k8s.temp_namespace("day06") as ns:
        k8s.apply(
            [
                manifests.sim_deployment(namespace=ns, replicas=3),
                disc.inference_pool("pool", ns, {"app": "sim"}, manifests.SIM_PORT, "epp"),
                *deploy.epp_objects("epp", ns, "pool", package=package),
            ]
        )
        k8s.kubectl("rollout", "status", "deployment/sim", "-n", ns, "--timeout=240s")
        k8s.kubectl("rollout", "status", "deployment/epp", "-n", ns, "--timeout=240s")
        yield ns


def served_per_pod(ns):
    """Requests each simulator says it completed, from its own /metrics."""
    return {pod: c["served"] for pod, c in k8s.sim_counters(ns).items()}


@pytest.mark.kind
def test_the_api_server_only_lets_the_epp_read(deployed):
    who = f"--as=system:serviceaccount:{deployed}:epp"
    can = lambda *a: k8s.kubectl("auth", "can-i", *a, "-n", deployed, who, check=False).strip()  # noqa: E731
    assert can("watch", "pods") == "yes"
    assert can("list", "inferencepools.inference.networking.k8s.io") == "yes"
    assert can("delete", "pods") == "no"
    assert can("get", "secrets") == "no"


@pytest.mark.kind
def test_envoy_sends_each_request_where_the_epp_says(deployed):
    before = served_per_pod(deployed)
    out = k8s.run_python_in_cluster(
        deployed,
        """
        import httpx, json
        shared = "the same long system prompt " * 40
        codes = []
        for i in range(10):
            r = httpx.post("http://epp/v1/completions", timeout=30,
                           json={"model": "mini/sim", "prompt": shared + str(i), "max_tokens": 2})
            codes.append(r.status_code)
        models = httpx.get("http://epp/v1/models", timeout=30).status_code
        bad = httpx.post("http://epp/v1/completions", content=b"{", timeout=30).status_code
        print(json.dumps({"codes": codes, "models": models, "bad": bad}))
        """,
    )
    result = json.loads(out.strip().splitlines()[-1])
    assert result["codes"] == [200] * 10
    assert result["models"] == 200
    assert result["bad"] == 400
    after = served_per_pod(deployed)
    delta = sorted(after[p] - before.get(p, 0) for p in after)
    assert delta[-1] == 10, f"one shared prefix should stick to one replica, got {delta}"


def test_parse_options_keeps_feature_flags(mod):
    epp = mod("epp")
    opts = epp.parse_options(
        [
            "--pool-name",
            "p",
            "--pool-namespace",
            "n",
            "--refresh-interval",
            "0.2",
            "--feature",
            "a",
            "--feature",
            "b",
            "--their-flag",
            "1",
        ]
    )
    assert (opts.pool_name, opts.pool_namespace, opts.refresh_interval) == ("p", "n", 0.2)
    assert opts.grpc_port == 9002 and opts.prefix_weight == 1.0
    assert opts.features == ("a", "b")
    assert opts.feature_args == ("--their-flag", "1")


def test_build_installs_features_by_module_name(mod):
    epp = mod("epp")
    rt = epp.build(epp.Options("pool", "ns"))
    assert set(rt.profiles) == {"default"} and len(rt.producers) == 1
    with pytest.raises(ModuleNotFoundError):
        epp.build(epp.Options("pool", "ns", features=("no_such_feature",)))
