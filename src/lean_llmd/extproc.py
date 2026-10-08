"""Day 6 - the ext-proc server: Envoy asks, the EPP answers.

Your implementation. Every function below raises until you write it.

    uv run lean-llmd test --day 6         # your code
    uv run lean-llmd test --day 6 --ref   # proves the tests are passable
    uv run lean-llmd diff --day 6         # only after you have a passing run
"""

import json
import threading
import uuid
from collections.abc import Iterator
from typing import Any, Protocol

import grpc
from envoy.config.core.v3 import base_pb2
from envoy.service.ext_proc.v3 import external_processor_pb2 as pb
from envoy.service.ext_proc.v3 import external_processor_pb2_grpc as pb_grpc
from envoy.type.v3 import http_status_pb2
from google.protobuf import struct_pb2

from lean_llmd.datastore import Datastore
from lean_llmd.scheduling import InferenceRequest, NoEndpointsError, Scheduler, SchedulingResult

DESTINATION_ENDPOINT_KEY = "x-gateway-destination-endpoint"
DESTINATION_ENDPOINT_NAMESPACE = "envoy.lb"
REQUEST_ID_HEADER = "x-request-id"


class BadRequestError(Exception):
    pass


class RequestRejected(Exception):
    """Answer the client with ``status`` instead of routing (429 for backpressure, say)."""

    def __init__(self, status: int, message: str) -> None:
        raise NotImplementedError("day 6: implement __init__()")


class RequestHandler(Protocol):
    """What the gRPC server needs: a ``Director``, or anything wrapping one (day 10)."""

    def handle_request(self, request: InferenceRequest) -> str: ...

    def any_endpoint(self) -> str: ...


class Producer(Protocol):
    """A plugin that writes per-request data before scheduling and learns after it."""

    def produce(self, request: InferenceRequest, endpoints: list[Any]) -> None: ...

    def pre_request(self, request: InferenceRequest, result: SchedulingResult) -> None: ...


def parse_request(headers: dict[str, str], body: bytes) -> InferenceRequest:
    """Build an ``InferenceRequest`` from lower-cased headers and a JSON body.

    The request id is the ``x-request-id`` header, or a fresh UUID. The
    target model is the body's ``model``. Raise ``BadRequestError`` if the
    body is not a JSON object or has no string ``model``.
    """
    raise NotImplementedError("day 6: implement parse_request()")


class Director:
    """Candidates, producers, scheduler, then pre-request hooks. Upstream: ``Director``.

    Scheduling is serialised with a lock: producers write onto the shared
    ``Endpoint`` objects, and two requests must not see each other's data.
    """

    def __init__(
        self, datastore: Datastore, scheduler: Scheduler, producers: list[Producer]
    ) -> None:
        raise NotImplementedError("day 6: implement __init__()")

    def handle_request(self, request: InferenceRequest) -> str:
        """Pick an endpoint for ``request``; return its ``"ip:port"``.

        Raise ``NoEndpointsError`` if the datastore is empty. Run every
        producer's ``produce``, schedule, then every producer's
        ``pre_request``, and return the primary profile's first endpoint.
        """
        raise NotImplementedError("day 6: implement handle_request()")

    def any_endpoint(self) -> str:
        """Round-robin over the datastore, for bodiless requests. ``NoEndpointsError`` if empty."""
        raise NotImplementedError("day 6: implement any_endpoint()")


def destination_metadata(target: str) -> struct_pb2.Struct:
    """``{"envoy.lb": {"x-gateway-destination-endpoint": target}}`` as a protobuf Struct."""
    raise NotImplementedError("day 6: implement destination_metadata()")


def routing_response(
    target: str, phase: str, extra_headers: dict[str, str] | None = None
) -> pb.ProcessingResponse:
    """Tell Envoy to send this request to ``target``.

    ``phase`` is ``"request_headers"`` or ``"request_body"``: the response
    must answer the message Envoy sent. It sets the destination header (then
    any ``extra_headers``), clears the route cache (the route was chosen
    before the header existed) and carries ``destination_metadata``.
    """
    raise NotImplementedError("day 6: implement routing_response()")


def continue_response() -> pb.ProcessingResponse:
    """Let request headers through unchanged; the decision comes with the body."""
    raise NotImplementedError("day 6: implement continue_response()")


def immediate_response(status: int, message: str) -> pb.ProcessingResponse:
    """Answer the client with ``status`` and ``message``; nothing reaches a model server."""
    raise NotImplementedError("day 6: implement immediate_response()")


def header_map(message: pb.HttpHeaders) -> dict[str, str]:
    """Lower-cased header names to values. Envoy fills ``raw_value``, older ones ``value``."""
    raise NotImplementedError("day 6: implement header_map()")


class StreamingServer(pb_grpc.ExternalProcessorServicer):
    """The gRPC service Envoy's ext_proc filter calls. Upstream: ``StreamingServer``."""

    def __init__(self, director: RequestHandler) -> None:
        raise NotImplementedError("day 6: implement __init__()")

    def Process(
        self, request_iterator: Iterator[pb.ProcessingRequest], context: grpc.ServicerContext
    ) -> Iterator[pb.ProcessingResponse]:
        """Answer each message of one HTTP request's stream.

        - ``request_headers``: remember them. If ``end_of_stream`` is set
          there is no body (a GET): route to ``any_endpoint()`` now.
          Otherwise ``continue_response()``.
        - ``request_body``: accumulate; at ``end_of_stream`` parse, schedule,
          and answer with ``routing_response(target, "request_body",
          request.set_headers)``.
        - A bad body answers 400, no endpoints 503, a ``RequestRejected`` its
          own status, any other failure 500, all via ``immediate_response``.
        - Ignore every other message type.
        """
        raise NotImplementedError("day 6: implement Process()")
