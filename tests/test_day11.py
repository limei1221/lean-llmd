"""Day 11 - gateway mode.

Oracles: the ``cryptography`` package and gRPC's own TLS stack for the
certificate, Envoy's protobufs over a real gRPC stream for the full-duplex
protocol, the API server for the Gateway API objects, and on kind a real
gateway (agentgateway) that must route by your EPP's decision.
"""

import datetime
import json
from concurrent import futures

import grpc
import pytest
from cryptography import x509
from cryptography.hazmat.primitives import serialization
from envoy.config.core.v3 import base_pb2
from envoy.service.ext_proc.v3 import external_processor_pb2 as pb
from envoy.service.ext_proc.v3 import external_processor_pb2_grpc as pb_grpc
from grpc_health.v1 import health, health_pb2, health_pb2_grpc

import k8s


@pytest.fixture
def gw(mod):
    return mod("gateway")


def test_the_certificate_is_self_signed_and_current(gw):
    key_pem, cert_pem = gw.self_signed_certificate("epp.ns.svc", days=10)
    cert = x509.load_pem_x509_certificate(cert_pem)
    key = serialization.load_pem_private_key(key_pem, password=None)
    assert cert.subject == cert.issuer
    cn = cert.subject.get_attributes_for_oid(x509.oid.NameOID.COMMON_NAME)[0].value
    assert cn == "epp.ns.svc"
    san = cert.extensions.get_extension_for_class(x509.SubjectAlternativeName).value
    assert san.get_values_for_type(x509.DNSName) == ["epp.ns.svc"]
    now = datetime.datetime.now(datetime.UTC)
    assert cert.not_valid_before_utc <= now < cert.not_valid_after_utc
    assert cert.not_valid_after_utc - now <= datetime.timedelta(days=10)
    assert key.public_key().public_numbers() == cert.public_key().public_numbers()


def test_grpc_speaks_tls_with_it(gw):
    key, cert = gw.self_signed_certificate("epp")
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=2))
    health_pb2_grpc.add_HealthServicer_to_server(health.HealthServicer(), server)
    port = server.add_secure_port("127.0.0.1:0", grpc.ssl_server_credentials([(key, cert)]))
    server.start()
    try:
        creds = grpc.ssl_channel_credentials(root_certificates=cert)
        options = [("grpc.ssl_target_name_override", "epp")]
        with grpc.secure_channel(f"127.0.0.1:{port}", creds, options) as channel:
            reply = health_pb2_grpc.HealthStub(channel).Check(health_pb2.HealthCheckRequest())
        assert reply.status == health_pb2.HealthCheckResponse.SERVING
        with grpc.insecure_channel(f"127.0.0.1:{port}") as plain, pytest.raises(grpc.RpcError):
            health_pb2_grpc.HealthStub(plain).Check(health_pb2.HealthCheckRequest(), timeout=2)
    finally:
        server.stop(0)


def test_streamed_body_chunks(gw):
    chunks = gw.streamed_body(b"abcdefg", "request_body", limit=3)
    parts = [c.request_body.response.body_mutation.streamed_response for c in chunks]
    assert [(p.body, p.end_of_stream) for p in parts] == [
        (b"abc", False),
        (b"def", False),
        (b"g", True),
    ]
    (empty,) = gw.streamed_body(b"", "response_body")
    sr = empty.response_body.response.body_mutation.streamed_response
    assert (sr.body, sr.end_of_stream) == (b"", True)


class Fixed:
    def handle_request(self, request):
        request.set_headers["x-extra"] = "yes"
        return "10.0.0.5:8000"

    def any_endpoint(self):
        return "10.0.0.6:8000"


def headers(eos=False):
    h = base_pb2.HeaderMap(headers=[base_pb2.HeaderValue(key=":path", raw_value=b"/v1/x")])
    return pb.ProcessingRequest(request_headers=pb.HttpHeaders(headers=h, end_of_stream=eos))


def body(data, eos, response=False):
    msg = pb.HttpBody(body=data, end_of_stream=eos)
    return (
        pb.ProcessingRequest(response_body=msg)
        if response
        else pb.ProcessingRequest(request_body=msg)
    )


@pytest.fixture
def stub(gw):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=2))
    pb_grpc.add_ExternalProcessorServicer_to_server(gw.FullDuplexStreamingServer(Fixed()), server)
    port = server.add_insecure_port("127.0.0.1:0")
    server.start()
    with grpc.insecure_channel(f"127.0.0.1:{port}") as channel:
        yield pb_grpc.ExternalProcessorStub(channel)
    server.stop(0)


def test_the_full_duplex_conversation(stub):
    request = json.dumps({"model": "m", "prompt": "hello"}).encode()
    messages = [
        headers(),
        body(request[:5], False),
        body(request[5:], True),
        pb.ProcessingRequest(response_headers=pb.HttpHeaders()),
        body(b"data: one\n\n", False, response=True),
        body(b"data: [DONE]\n\n", True, response=True),
        pb.ProcessingRequest(response_trailers=pb.HttpTrailers()),
    ]
    out = list(stub.Process(iter(messages)))
    kinds = [r.WhichOneof("response") for r in out]
    assert kinds == [
        "request_headers",
        "request_body",
        "response_headers",
        "response_body",
        "response_body",
        "response_trailers",
    ], "nothing is answered until the body is complete"
    mutated = {
        h.header.key: h.header.raw_value
        for h in out[0].request_headers.response.header_mutation.set_headers
    }
    assert mutated["x-gateway-destination-endpoint"] == b"10.0.0.5:8000"
    assert mutated["x-extra"] == b"yes"
    assert out[0].request_headers.response.clear_route_cache
    echoed = out[1].request_body.response.body_mutation.streamed_response
    assert (echoed.body, echoed.end_of_stream) == (request, True), (
        "the EPP held the body; send it on"
    )
    first, last = (r.response_body.response.body_mutation.streamed_response for r in out[3:5])
    assert (first.body, first.end_of_stream) == (b"data: one\n\n", False)
    assert (last.body, last.end_of_stream) == (b"data: [DONE]\n\n", True)


def test_a_bodiless_request_routes_at_its_headers(stub):
    (out,) = list(stub.Process(iter([headers(eos=True)])))
    (h,) = out.request_headers.response.header_mutation.set_headers
    assert h.header.raw_value == b"10.0.0.6:8000"


def test_manifests(gw, mod):
    g = gw.gateway("gw", "ns")
    assert g["spec"]["gatewayClassName"] == "agentgateway"
    assert g["spec"]["listeners"] == [{"name": "http", "protocol": "HTTP", "port": 80}]
    route = gw.http_route("llm", "ns", "gw", "pool")
    assert route["spec"]["parentRefs"] == [{"name": "gw"}]
    (rule,) = route["spec"]["rules"]
    assert rule["backendRefs"] == [
        {"group": "inference.networking.k8s.io", "kind": "InferencePool", "name": "pool"}
    ]
    deploy = mod("deploy")
    original = deploy.epp_deployment("epp", "ns", "pool")
    converted = gw.gateway_mode(original)
    (epp,) = converted["spec"]["template"]["spec"]["containers"]
    assert epp["name"] == "epp" and epp["args"][-2:] == ["--feature", "gateway"]
    assert epp["readinessProbe"]["grpc"]["port"] == 9003
    assert len(original["spec"]["template"]["spec"]["containers"]) == 2, "input left alone"


def test_install_turns_on_tls_and_full_duplex(gw, mod):
    epp = mod("epp")
    rt = epp.build(epp.Options("pool", "ns", features=("gateway",)))
    assert rt.servicer_factory is gw.FullDuplexStreamingServer
    assert rt.server_credentials is not None
    plain = epp.build(
        epp.Options("pool", "ns", features=("gateway",), feature_args=("--no-secure-serving",))
    )
    assert plain.server_credentials is None


def agentgateway_unavailable():
    out = k8s.kubectl("get", "gatewayclass", "agentgateway", check=False)
    return "agentgateway" not in out


@pytest.mark.kind
def test_the_api_server_accepts_the_gateway_objects(gw):
    objs = [gw.gateway("gw", "default"), gw.http_route("llm", "default", "gw", "pool")]
    out = k8s.apply(objs, dry_run=True)
    assert out.count("created (server dry run)") == 2


@pytest.mark.kind
def test_agentgateway_routes_by_your_epp(mod, gw, package):
    if agentgateway_unavailable():
        pytest.skip("agentgateway is not installed; run: uv run lean-llmd addons gateway")
    if not k8s.image_present_in_kind():
        pytest.skip("the lean-llmd:dev image is not in kind; run: uv run lean-llmd image")
    manifests, disc, deploy = mod("manifests"), mod("discovery"), mod("deploy")
    with k8s.temp_namespace("day11") as ns:
        pool = disc.inference_pool("pool", ns, {"app": "sim"}, manifests.SIM_PORT, "epp")
        pool["spec"]["endpointPickerRef"]["failureMode"] = "FailClose"  # no silent random fallback
        k8s.apply(
            [
                manifests.sim_deployment(namespace=ns, replicas=3),
                pool,
                *deploy.epp_rbac("epp", ns),
                gw.gateway_mode(deploy.epp_deployment("epp", ns, "pool", package=package)),
                deploy.epp_service("epp", ns),
                gw.gateway("gw", ns),
                gw.http_route("llm", ns, "gw", "pool"),
            ]
        )
        for name in ("sim", "epp", "gw"):
            k8s.wait_until(
                lambda n=name: k8s.kubectl("get", "deployment", n, "-n", ns, check=False),
                timeout=120,
                what=f"deployment {name}",
            )
            k8s.kubectl("rollout", "status", f"deployment/{name}", "-n", ns, "--timeout=240s")
        before = k8s.sim_counters(ns)
        out = k8s.run_python_in_cluster(
            ns,
            """
            import httpx, json
            shared = "a long shared system prompt " * 40
            codes = [httpx.post("http://gw/v1/completions", timeout=30,
                                json={"model": "mini/sim", "prompt": shared + str(i),
                                      "max_tokens": 2}).status_code for i in range(10)]
            with httpx.stream("POST", "http://gw/v1/completions", timeout=30,
                              json={"model": "mini/sim", "prompt": "hi", "max_tokens": 5,
                                    "stream": True, "ignore_eos": True}) as r:
                lines = [l for l in r.iter_lines() if l.startswith("data: ")]
            print(json.dumps({"codes": codes, "stream_end": lines[-1], "chunks": len(lines)}))
            """,
        )
        after = k8s.sim_counters(ns)
    result = json.loads(out.strip().splitlines()[-1])
    assert result["codes"] == [200] * 10
    assert result["stream_end"] == "data: [DONE]" and result["chunks"] >= 6
    delta = sorted(after[p]["served"] - before.get(p, {}).get("served", 0) for p in after)
    assert delta[-1] >= 10, f"the shared prefix must stick to one replica: {delta}"
