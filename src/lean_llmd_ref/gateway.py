"""Day 11 - gateway mode: a shared Gateway calls your EPP.

Shaped like llm-d-router's ``pkg/epp/handlers`` in ``FULL_DUPLEX_STREAMED``
mode and its self-signed serving certificate. A gateway (agentgateway here)
owns the ext-proc configuration: it streams the request *and the response*
through the EPP, and it dials the EPP over TLS. Run it with
``--feature gateway``.
"""

import argparse
import copy
import datetime
from collections.abc import Iterator
from typing import Any

import grpc
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import NameOID
from envoy.service.ext_proc.v3 import external_processor_pb2 as pb

from lean_llmd_ref.epp import Runtime
from lean_llmd_ref.extproc import (
    BadRequestError,
    RequestRejected,
    StreamingServer,
    header_map,
    immediate_response,
    parse_request,
    routing_response,
)
from lean_llmd_ref.scheduling import NoEndpointsError

Obj = dict[str, Any]

BODY_BYTE_LIMIT = 62000
GATEWAY_GROUP = "gateway.networking.k8s.io"


def self_signed_certificate(common_name: str, days: int = 365) -> tuple[bytes, bytes]:
    """A fresh EC P-256 key and a self-signed certificate for ``common_name``, as PEM.

    The certificate names ``common_name`` as its subject, issuer and DNS SAN,
    and is valid from now for ``days``. Gateways dial the EPP with TLS but
    skip verification, so a self-signed one is enough (upstream does the
    same when no certificate is mounted).
    """
    key = ec.generate_private_key(ec.SECP256R1())
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, common_name)])
    now = datetime.datetime.now(datetime.UTC)
    cert = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - datetime.timedelta(minutes=1))
        .not_valid_after(now + datetime.timedelta(days=days))
        .add_extension(x509.SubjectAlternativeName([x509.DNSName(common_name)]), critical=False)
        .sign(key, hashes.SHA256())
    )
    key_pem = key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    )
    return key_pem, cert.public_bytes(serialization.Encoding.PEM)


def streamed_body(
    body: bytes, phase: str, limit: int = BODY_BYTE_LIMIT
) -> list[pb.ProcessingResponse]:
    """Send ``body`` back to the proxy as ``StreamedBodyResponse`` chunks.

    ``phase`` is ``"request_body"`` or ``"response_body"``. Chunks are at most
    ``limit`` bytes; only the last has ``end_of_stream``. An empty body is
    one empty chunk with ``end_of_stream``.
    """
    pieces = [body[i : i + limit] for i in range(0, len(body), limit)] or [b""]
    out = []
    for i, piece in enumerate(pieces):
        common = pb.CommonResponse(
            body_mutation=pb.BodyMutation(
                streamed_response=pb.StreamedBodyResponse(
                    body=piece, end_of_stream=i == len(pieces) - 1
                )
            )
        )
        response = pb.ProcessingResponse()
        getattr(response, phase).CopyFrom(pb.BodyResponse(response=common))
        out.append(response)
    return out


class FullDuplexStreamingServer(StreamingServer):
    """The ext-proc protocol in ``FULL_DUPLEX_STREAMED`` mode.

    In this mode the proxy does not wait for an answer per message: it keeps
    streaming, and the EPP sends its answers when it has them. The request
    body is *held by the EPP*, so after routing it must stream the body back.
    """

    def Process(  # noqa: N802 - the gRPC method name
        self, request_iterator: Iterator[pb.ProcessingRequest], context: grpc.ServicerContext
    ) -> Iterator[pb.ProcessingResponse]:
        """Answer one HTTP request's stream.

        - ``request_headers``: remember them, answer nothing yet. If
          ``end_of_stream`` (no body), route to ``any_endpoint()`` now.
        - ``request_body``: buffer; at ``end_of_stream`` parse and schedule,
          then answer the *headers* with ``routing_response(target,
          "request_headers", request.set_headers)`` followed by the whole
          body via ``streamed_body(..., "request_body")``.
        - ``response_headers``: an empty ``HeadersResponse``.
        - ``response_body``: echo each chunk unchanged with
          ``streamed_body(chunk, "response_body")``, keeping its
          ``end_of_stream``.
        - ``request_trailers`` / ``response_trailers``: an empty
          ``TrailersResponse`` in the matching field.
        - Errors: 400 / 503 / the rejection's status / 500, as on day 6.
        """
        headers: dict[str, str] = {}
        body = bytearray()
        for message in request_iterator:
            kind = message.WhichOneof("request")
            try:
                if kind == "request_headers":
                    headers = header_map(message.request_headers)
                    if message.request_headers.end_of_stream:
                        yield routing_response(self.director.any_endpoint(), "request_headers")
                elif kind == "request_body":
                    body += message.request_body.body
                    if message.request_body.end_of_stream:
                        request = parse_request(headers, bytes(body))
                        target = self.director.handle_request(request)
                        yield routing_response(target, "request_headers", request.set_headers)
                        yield from streamed_body(bytes(body), "request_body")
                elif kind == "response_headers":
                    yield pb.ProcessingResponse(response_headers=pb.HeadersResponse())
                elif kind == "response_body":
                    chunk = message.response_body
                    pieces = streamed_body(chunk.body, "response_body")
                    last = pieces[-1].response_body.response.body_mutation.streamed_response
                    last.end_of_stream = chunk.end_of_stream
                    yield from pieces
                elif kind == "request_trailers":
                    yield pb.ProcessingResponse(request_trailers=pb.TrailersResponse())
                elif kind == "response_trailers":
                    yield pb.ProcessingResponse(response_trailers=pb.TrailersResponse())
            except BadRequestError as e:
                yield immediate_response(400, str(e))
            except RequestRejected as e:
                yield immediate_response(e.status, str(e))
            except NoEndpointsError as e:
                yield immediate_response(503, str(e))
            except Exception as e:
                yield immediate_response(500, f"endpoint picker failed: {e}")


def gateway(name: str, namespace: str, class_name: str = "agentgateway", port: int = 80) -> Obj:
    """A ``gateway.networking.k8s.io/v1`` Gateway with one HTTP listener on ``port``."""
    return {
        "apiVersion": f"{GATEWAY_GROUP}/v1",
        "kind": "Gateway",
        "metadata": {"name": name, "namespace": namespace},
        "spec": {
            "gatewayClassName": class_name,
            "listeners": [{"name": "http", "protocol": "HTTP", "port": port}],
        },
    }


def http_route(name: str, namespace: str, gateway_name: str, pool: str) -> Obj:
    """An HTTPRoute attached to ``gateway_name`` whose only backend is InferencePool ``pool``.

    The backendRef names group ``inference.networking.k8s.io`` and kind
    ``InferencePool``: that is what tells the gateway to ask the pool's EPP.
    """
    return {
        "apiVersion": f"{GATEWAY_GROUP}/v1",
        "kind": "HTTPRoute",
        "metadata": {"name": name, "namespace": namespace},
        "spec": {
            "parentRefs": [{"name": gateway_name}],
            "rules": [
                {
                    "matches": [{"path": {"type": "PathPrefix", "value": "/"}}],
                    "backendRefs": [
                        {
                            "group": "inference.networking.k8s.io",
                            "kind": "InferencePool",
                            "name": pool,
                        }
                    ],
                }
            ],
        },
    }


def gateway_mode(epp_deployment: Obj) -> Obj:
    """A copy of day 6's EPP Deployment for gateway mode.

    Drop the ``envoy`` container (and its config volume): the shared gateway
    is the proxy now. Append ``--feature gateway`` to the ``epp`` container's
    args, and point its readiness probe at the plaintext health port 9003
    (the kubelet's gRPC probe cannot do TLS). Do not modify the input.
    """
    out = copy.deepcopy(epp_deployment)
    spec = out["spec"]["template"]["spec"]
    spec["containers"] = [c for c in spec["containers"] if c["name"] != "envoy"]
    spec["volumes"] = [v for v in spec.get("volumes", []) if "configMap" not in v]
    for c in spec["containers"]:
        if c["name"] == "epp":
            c["args"] = [*c.get("args", []), "--feature", "gateway"]
            c["readinessProbe"] = {"grpc": {"port": 9003}, "periodSeconds": 2}
    return out


def install(rt: Runtime, argv: list[str]) -> None:
    """The ``gateway`` feature: full-duplex streaming and, by default, TLS.

    ``--secure-serving`` / ``--no-secure-serving`` (default on);
    ``--cert-common-name`` (default ``epp``).
    """
    p = argparse.ArgumentParser(prog="gateway")
    p.add_argument("--secure-serving", action=argparse.BooleanOptionalAction, default=True)
    p.add_argument("--cert-common-name", default="epp")
    a, _ = p.parse_known_args(argv)
    rt.servicer_factory = FullDuplexStreamingServer
    if a.secure_serving:
        key, cert = self_signed_certificate(a.cert_common_name)
        rt.server_credentials = grpc.ssl_server_credentials([(key, cert)])
