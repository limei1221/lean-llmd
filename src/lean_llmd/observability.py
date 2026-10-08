"""Day 12 - EPP metrics, Prometheus and KEDA.

Your implementation. Every function below raises until you write it.

    uv run lean-llmd test --day 12         # your code
    uv run lean-llmd test --day 12 --ref   # proves the tests are passable
    uv run lean-llmd diff --day 12         # only after you have a passing run
"""

import argparse
import copy
import json
import threading
from collections.abc import Iterator
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

import grpc
from envoy.service.ext_proc.v3 import external_processor_pb2 as pb
from envoy.service.ext_proc.v3 import external_processor_pb2_grpc as pb_grpc

from lean_llmd.datastore import Datastore
from lean_llmd.epp import Runtime
from lean_llmd.extproc import DESTINATION_ENDPOINT_KEY, RequestHandler

Obj = dict[str, Any]
Labels = tuple[tuple[str, str], ...]
METRICS_PORT = 9090
REQUEST_RUNNING = "llm_d_epp_request_running"
REQUEST_TOTAL = "llm_d_epp_request_total"
READY_ENDPOINTS = "llm_d_epp_ready_endpoints"
AVERAGE_QUEUE_SIZE = "llm_d_epp_average_queue_size"
AVERAGE_KV_CACHE = "llm_d_epp_average_kv_cache_utilization"
FLOW_CONTROL_QUEUE_SIZE = "llm_d_epp_flow_control_queue_size"
FLOW_CONTROL_POOL_SATURATION = "llm_d_epp_flow_control_pool_saturation"


def escape_label_value(value: str) -> str:
    """Escape ``\\``, ``"`` and newlines for the text format, in that order."""
    raise NotImplementedError("day 12: implement escape_label_value()")


class Registry:
    """Gauges and counters, rendered in the Prometheus text format. Thread-safe."""

    def __init__(self) -> None:
        raise NotImplementedError("day 12: implement __init__()")

    def register(self, name: str, kind: str, help_text: str) -> None:
        """Declare a family; ``kind`` is ``"gauge"`` or ``"counter"``. Idempotent."""
        raise NotImplementedError("day 12: implement register()")

    def set(self, name: str, value: float, labels: dict[str, str] | None = None) -> None:
        raise NotImplementedError("day 12: implement set()")

    def add(self, name: str, delta: float, labels: dict[str, str] | None = None) -> None:
        raise NotImplementedError("day 12: implement add()")

    def get(self, name: str, labels: dict[str, str] | None = None) -> float:
        raise NotImplementedError("day 12: implement get()")

    def render(self) -> str:
        """Every family as ``# HELP``, ``# TYPE`` and one line per label set.

        The sample name is the family name (so name counters ``..._total``,
        as Prometheus' Go client does). Labels are sorted by name; values are
        escaped. Families and samples come out sorted, so the output is stable.
        """
        raise NotImplementedError("day 12: implement render()")


def new_epp_registry() -> Registry:
    """A registry with every ``llm_d_epp_*`` family this EPP exports."""
    raise NotImplementedError("day 12: implement new_epp_registry()")


def refresh_pool_metrics(registry: Registry, datastore: Datastore, pool: str) -> None:
    """Set the pool gauges (label ``name=<pool>``) from the datastore.

    Ready endpoints is the count; the averages are over endpoints, 0 when
    there are none.
    """
    raise NotImplementedError("day 12: implement refresh_pool_metrics()")


def is_routing_response(response: pb.ProcessingResponse) -> bool:
    """Does this response set the destination header (in either request phase)?"""
    raise NotImplementedError("day 12: implement is_routing_response()")


class CountingServer(pb_grpc.ExternalProcessorServicer):
    """Wraps a servicer; counts requests from routing until their stream closes."""

    def __init__(self, inner: Any, registry: Registry) -> None:
        raise NotImplementedError("day 12: implement __init__()")

    def Process(
        self, request_iterator: Iterator[pb.ProcessingRequest], context: grpc.ServicerContext
    ) -> Iterator[pb.ProcessingResponse]:
        """Pass everything through; learn the model from the request body on the way.

        On the first routing response, add 1 to ``llm_d_epp_request_running``
        and ``llm_d_epp_request_total`` with label ``model_name``; when the
        stream ends (normally or not), subtract the 1 from running.
        """
        raise NotImplementedError("day 12: implement Process()")


class MetricsServer(ThreadingHTTPServer):
    """``GET /metrics`` serves the registry; anything else is 404."""

    daemon_threads = True

    def __init__(self, port: int, registry: Registry) -> None:
        raise NotImplementedError("day 12: implement __init__()")


def scrape_annotations(deployment: Obj, port: int = METRICS_PORT) -> Obj:
    """A copy of a Deployment whose Pods Prometheus will scrape.

    Pod-template annotations ``prometheus.io/scrape: "true"``,
    ``prometheus.io/port: "<port>"`` and ``prometheus.io/path: /metrics``
    (the Prometheus chart's ``kubernetes-pods`` job reads them), and a
    container port named ``metrics`` on the first container. Do not modify
    the input.
    """
    raise NotImplementedError("day 12: implement scrape_annotations()")


def scaled_object(
    name: str,
    namespace: str,
    deployment: str,
    prometheus_url: str,
    query: str,
    threshold: float,
    min_replicas: int = 1,
    max_replicas: int = 4,
    stabilization_seconds: int = 30,
) -> Obj:
    """A KEDA ``ScaledObject`` scaling ``deployment`` on one Prometheus query.

    One ``prometheus`` trigger with ``metricType: AverageValue``: the HPA
    KEDA creates aims for ``threshold`` per replica. Poll every 5 s; both
    stabilization windows ``stabilization_seconds`` (llm-d uses 300 for real
    models that take minutes to load; simulators start in a second).
    """
    raise NotImplementedError("day 12: implement scaled_object()")


def install(rt: Runtime, argv: list[str]) -> None:
    """The ``observability`` feature: export ``llm_d_epp_*`` on ``--metrics-port`` (9090).

    Counts running requests by wrapping the servicer, refreshes pool gauges
    every tick, and, when day 10's flow controller is in ``rt.extras``,
    exports its queue size and saturation too.
    """
    raise NotImplementedError("day 12: implement install()")
