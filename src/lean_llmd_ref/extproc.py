"""Day 6 - the ext-proc server: Envoy asks, the EPP answers with an ip:port.

Shaped like llm-d-router's ``pkg/epp/handlers`` (the gRPC stream) and
``pkg/epp/requestcontrol`` (the ``Director``). One gRPC stream carries one
HTTP request. Envoy runs this filter in BUFFERED body mode here, so the whole
JSON body arrives in one message; upstream streams it (FULL_DUPLEX_STREAMED).
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

from lean_llmd_ref.datastore import Datastore
from lean_llmd_ref.scheduling import (
    InferenceRequest,
    NoEndpointsError,
    Scheduler,
    SchedulingResult,
)

DESTINATION_ENDPOINT_KEY = "x-gateway-destination-endpoint"
DESTINATION_ENDPOINT_NAMESPACE = "envoy.lb"
REQUEST_ID_HEADER = "x-request-id"


class BadRequestError(Exception):
    pass


class RequestRejected(Exception):
    """Answer the client with ``status`` instead of routing (429 for backpressure, say)."""

    def __init__(self, status: int, message: str) -> None:
        super().__init__(message)
        self.status = status


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
    try:
        parsed = json.loads(body)
    except (json.JSONDecodeError, UnicodeDecodeError) as e:
        raise BadRequestError(f"body is not JSON: {e}") from e
    if not isinstance(parsed, dict) or not isinstance(parsed.get("model"), str):
        raise BadRequestError("body must be a JSON object with a string 'model'")
    request_id = headers.get(REQUEST_ID_HEADER) or str(uuid.uuid4())
    return InferenceRequest(request_id, parsed["model"], parsed, dict(headers))


class Director:
    """Candidates, producers, scheduler, then pre-request hooks. Upstream: ``Director``.

    Scheduling is serialised with a lock: producers write onto the shared
    ``Endpoint`` objects, and two requests must not see each other's data.
    """

    def __init__(
        self, datastore: Datastore, scheduler: Scheduler, producers: list[Producer]
    ) -> None:
        self.datastore = datastore
        self.scheduler = scheduler
        self.producers = producers
        self._lock = threading.Lock()
        self._fallback = 0

    def handle_request(self, request: InferenceRequest) -> str:
        """Pick an endpoint for ``request``; return its ``"ip:port"``.

        Raise ``NoEndpointsError`` if the datastore is empty. Run every
        producer's ``produce``, schedule, then every producer's
        ``pre_request``, and return the primary profile's first endpoint.
        """
        with self._lock:
            candidates = self.datastore.pod_list()
            if not candidates:
                raise NoEndpointsError("no endpoints in the pool")
            for producer in self.producers:
                producer.produce(request, candidates)
            result = self.scheduler.schedule(request, candidates)
            for producer in self.producers:
                producer.pre_request(request, result)
            return result.primary().target_endpoints[0].metadata.host_port

    def any_endpoint(self) -> str:
        """Round-robin over the datastore, for bodiless requests. ``NoEndpointsError`` if empty."""
        with self._lock:
            candidates = self.datastore.pod_list()
            if not candidates:
                raise NoEndpointsError("no endpoints in the pool")
            self._fallback += 1
            return candidates[self._fallback % len(candidates)].metadata.host_port


def destination_metadata(target: str) -> struct_pb2.Struct:
    """``{"envoy.lb": {"x-gateway-destination-endpoint": target}}`` as a protobuf Struct."""
    metadata = struct_pb2.Struct()
    metadata.update({DESTINATION_ENDPOINT_NAMESPACE: {DESTINATION_ENDPOINT_KEY: target}})
    return metadata


def routing_response(
    target: str, phase: str, extra_headers: dict[str, str] | None = None
) -> pb.ProcessingResponse:
    """Tell Envoy to send this request to ``target``.

    ``phase`` is ``"request_headers"`` or ``"request_body"``: the response
    must answer the message Envoy sent. It sets the destination header (then
    any ``extra_headers``), clears the route cache (the route was chosen
    before the header existed) and carries ``destination_metadata``.
    """
    headers = {DESTINATION_ENDPOINT_KEY: target, **(extra_headers or {})}
    common = pb.CommonResponse(
        header_mutation=pb.HeaderMutation(
            set_headers=[
                base_pb2.HeaderValueOption(
                    header=base_pb2.HeaderValue(key=key, raw_value=value.encode())
                )
                for key, value in headers.items()
            ]
        ),
        clear_route_cache=True,
    )
    response = pb.ProcessingResponse(dynamic_metadata=destination_metadata(target))
    if phase == "request_headers":
        response.request_headers.CopyFrom(pb.HeadersResponse(response=common))
    else:
        response.request_body.CopyFrom(pb.BodyResponse(response=common))
    return response


def continue_response() -> pb.ProcessingResponse:
    """Let request headers through unchanged; the decision comes with the body."""
    return pb.ProcessingResponse(request_headers=pb.HeadersResponse())


def immediate_response(status: int, message: str) -> pb.ProcessingResponse:
    """Answer the client with ``status`` and ``message``; nothing reaches a model server."""
    return pb.ProcessingResponse(
        immediate_response=pb.ImmediateResponse(
            status=http_status_pb2.HttpStatus(code=status), body=message.encode()
        )
    )


def header_map(message: pb.HttpHeaders) -> dict[str, str]:
    """Lower-cased header names to values. Envoy fills ``raw_value``, older ones ``value``."""
    return {
        h.key.lower(): (h.raw_value.decode() if h.raw_value else h.value)
        for h in message.headers.headers
    }


class StreamingServer(pb_grpc.ExternalProcessorServicer):  # type: ignore[misc]
    """The gRPC service Envoy's ext_proc filter calls. Upstream: ``StreamingServer``."""

    def __init__(self, director: RequestHandler) -> None:
        self.director = director

    def Process(  # noqa: N802 - the gRPC method name
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
        headers: dict[str, str] = {}
        body = bytearray()
        for message in request_iterator:
            kind = message.WhichOneof("request")
            try:
                if kind == "request_headers":
                    headers = header_map(message.request_headers)
                    if message.request_headers.end_of_stream:
                        target = self.director.any_endpoint()
                        yield routing_response(target, "request_headers")
                    else:
                        yield continue_response()
                elif kind == "request_body":
                    body += message.request_body.body
                    if message.request_body.end_of_stream:
                        request = parse_request(headers, bytes(body))
                        target = self.director.handle_request(request)
                        yield routing_response(target, "request_body", request.set_headers)
            except BadRequestError as e:
                yield immediate_response(400, str(e))
            except RequestRejected as e:
                yield immediate_response(e.status, str(e))
            except NoEndpointsError as e:
                yield immediate_response(503, str(e))
            except Exception as e:
                yield immediate_response(500, f"endpoint picker failed: {e}")
