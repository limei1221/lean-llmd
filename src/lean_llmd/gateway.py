"""Day 11 - gateway mode: TLS and the full-duplex protocol.

Your implementation. Every function below raises until you write it.

    uv run lean-llmd test --day 11         # your code
    uv run lean-llmd test --day 11 --ref   # proves the tests are passable
    uv run lean-llmd diff --day 11         # only after you have a passing run
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

from lean_llmd.epp import Runtime
from lean_llmd.extproc import (
    BadRequestError,
    RequestRejected,
    StreamingServer,
    header_map,
    immediate_response,
    parse_request,
    routing_response,
)
from lean_llmd.scheduling import NoEndpointsError

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
    raise NotImplementedError("day 11: implement self_signed_certificate()")


def streamed_body(
    body: bytes, phase: str, limit: int = BODY_BYTE_LIMIT
) -> list[pb.ProcessingResponse]:
    """Send ``body`` back to the proxy as ``StreamedBodyResponse`` chunks.

    ``phase`` is ``"request_body"`` or ``"response_body"``. Chunks are at most
    ``limit`` bytes; only the last has ``end_of_stream``. An empty body is
    one empty chunk with ``end_of_stream``.
    """
    raise NotImplementedError("day 11: implement streamed_body()")


class FullDuplexStreamingServer(StreamingServer):
    """The ext-proc protocol in ``FULL_DUPLEX_STREAMED`` mode.

    In this mode the proxy does not wait for an answer per message: it keeps
    streaming, and the EPP sends its answers when it has them. The request
    body is *held by the EPP*, so after routing it must stream the body back.
    """

    def Process(
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
        raise NotImplementedError("day 11: implement Process()")


def gateway(name: str, namespace: str, class_name: str = "agentgateway", port: int = 80) -> Obj:
    """A ``gateway.networking.k8s.io/v1`` Gateway with one HTTP listener on ``port``."""
    raise NotImplementedError("day 11: implement gateway()")


def http_route(name: str, namespace: str, gateway_name: str, pool: str) -> Obj:
    """An HTTPRoute attached to ``gateway_name`` whose only backend is InferencePool ``pool``.

    The backendRef names group ``inference.networking.k8s.io`` and kind
    ``InferencePool``: that is what tells the gateway to ask the pool's EPP.
    """
    raise NotImplementedError("day 11: implement http_route()")


def gateway_mode(epp_deployment: Obj) -> Obj:
    """A copy of day 6's EPP Deployment for gateway mode.

    Drop the ``envoy`` container (and its config volume): the shared gateway
    is the proxy now. Append ``--feature gateway`` to the ``epp`` container's
    args, and point its readiness probe at the plaintext health port 9003
    (the kubelet's gRPC probe cannot do TLS). Do not modify the input.
    """
    raise NotImplementedError("day 11: implement gateway_mode()")


def install(rt: Runtime, argv: list[str]) -> None:
    """The ``gateway`` feature: full-duplex streaming and, by default, TLS.

    ``--secure-serving`` / ``--no-secure-serving`` (default on);
    ``--cert-common-name`` (default ``epp``).
    """
    raise NotImplementedError("day 11: implement install()")
