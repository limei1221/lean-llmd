"""Day 12 - EPP metrics, and autoscaling on them with Prometheus and KEDA.

Shaped like llm-d-router's ``pkg/epp/metrics`` (the ``llm_d_epp_*`` series)
and llm-d's ``keda-epp`` autoscaling guide. The EPP sees demand before the
model servers do: requests running, requests queued in flow control, how
saturated the pool is. Prometheus scrapes it; a KEDA ``ScaledObject`` turns
a PromQL query into an HPA that scales the model servers. Run it with
``--feature observability``.
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

from lean_llmd_ref.datastore import Datastore
from lean_llmd_ref.epp import Runtime
from lean_llmd_ref.extproc import DESTINATION_ENDPOINT_KEY, RequestHandler

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
    return value.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")


class Registry:
    """Gauges and counters, rendered in the Prometheus text format. Thread-safe."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._families: dict[str, tuple[str, str, dict[Labels, float]]] = {}

    def register(self, name: str, kind: str, help_text: str) -> None:
        """Declare a family; ``kind`` is ``"gauge"`` or ``"counter"``. Idempotent."""
        with self._lock:
            self._families.setdefault(name, (kind, help_text, {}))

    def set(self, name: str, value: float, labels: dict[str, str] | None = None) -> None:
        with self._lock:
            self._families[name][2][tuple(sorted((labels or {}).items()))] = value

    def add(self, name: str, delta: float, labels: dict[str, str] | None = None) -> None:
        with self._lock:
            samples = self._families[name][2]
            key = tuple(sorted((labels or {}).items()))
            samples[key] = samples.get(key, 0.0) + delta

    def get(self, name: str, labels: dict[str, str] | None = None) -> float:
        with self._lock:
            return self._families[name][2].get(tuple(sorted((labels or {}).items())), 0.0)

    def render(self) -> str:
        """Every family as ``# HELP``, ``# TYPE`` and one line per label set.

        The sample name is the family name (so name counters ``..._total``,
        as Prometheus' Go client does). Labels are sorted by name; values are
        escaped. Families and samples come out sorted, so the output is stable.
        """
        lines: list[str] = []
        with self._lock:
            for name in sorted(self._families):
                kind, help_text, samples = self._families[name]
                lines.append(f"# HELP {name} {help_text}")
                lines.append(f"# TYPE {name} {kind}")
                for labels in sorted(samples):
                    rendered = ",".join(f'{k}="{escape_label_value(v)}"' for k, v in labels)
                    block = f"{{{rendered}}}" if rendered else ""
                    lines.append(f"{name}{block} {samples[labels]:g}")
        return "\n".join(lines) + "\n"


def new_epp_registry() -> Registry:
    """A registry with every ``llm_d_epp_*`` family this EPP exports."""
    registry = Registry()
    registry.register(REQUEST_RUNNING, "gauge", "Requests routed and not yet finished.")
    registry.register(REQUEST_TOTAL, "counter", "Requests routed.")
    registry.register(READY_ENDPOINTS, "gauge", "Ready endpoints in the pool.")
    registry.register(AVERAGE_QUEUE_SIZE, "gauge", "Average waiting queue across the pool.")
    registry.register(AVERAGE_KV_CACHE, "gauge", "Average KV-cache utilization across the pool.")
    registry.register(FLOW_CONTROL_QUEUE_SIZE, "gauge", "Requests held in flow control.")
    registry.register(FLOW_CONTROL_POOL_SATURATION, "gauge", "Pool saturation, 1.0 = full.")
    return registry


def refresh_pool_metrics(registry: Registry, datastore: Datastore, pool: str) -> None:
    """Set the pool gauges (label ``name=<pool>``) from the datastore.

    Ready endpoints is the count; the averages are over endpoints, 0 when
    there are none.
    """
    endpoints = datastore.pod_list()
    labels = {"name": pool}
    registry.set(READY_ENDPOINTS, len(endpoints), labels)
    n = len(endpoints) or 1
    registry.set(
        AVERAGE_QUEUE_SIZE, sum(e.metrics.waiting_queue_size for e in endpoints) / n, labels
    )
    registry.set(
        AVERAGE_KV_CACHE, sum(e.metrics.kv_cache_usage_percent for e in endpoints) / n, labels
    )


def is_routing_response(response: pb.ProcessingResponse) -> bool:
    """Does this response set the destination header (in either request phase)?"""
    for phase in ("request_headers", "request_body"):
        if response.HasField(phase):
            mutation = getattr(response, phase).response.header_mutation
            if any(h.header.key == DESTINATION_ENDPOINT_KEY for h in mutation.set_headers):
                return True
    return False


class CountingServer(pb_grpc.ExternalProcessorServicer):  # type: ignore[misc]
    """Wraps a servicer; counts requests from routing until their stream closes."""

    def __init__(self, inner: Any, registry: Registry) -> None:
        self.inner = inner
        self.registry = registry

    def Process(  # noqa: N802
        self, request_iterator: Iterator[pb.ProcessingRequest], context: grpc.ServicerContext
    ) -> Iterator[pb.ProcessingResponse]:
        """Pass everything through; learn the model from the request body on the way.

        On the first routing response, add 1 to ``llm_d_epp_request_running``
        and ``llm_d_epp_request_total`` with label ``model_name``; when the
        stream ends (normally or not), subtract the 1 from running.
        """
        body = bytearray()
        model = [""]

        def tap() -> Iterator[pb.ProcessingRequest]:
            for message in request_iterator:
                if message.WhichOneof("request") == "request_body":
                    body.extend(message.request_body.body)
                    if message.request_body.end_of_stream:
                        try:
                            model[0] = str(json.loads(bytes(body)).get("model", ""))
                        except (ValueError, AttributeError):
                            model[0] = ""
                yield message

        labels: dict[str, str] | None = None
        try:
            for response in self.inner.Process(tap(), context):
                if labels is None and is_routing_response(response):
                    labels = {"model_name": model[0]}
                    self.registry.add(REQUEST_RUNNING, 1, labels)
                    self.registry.add(REQUEST_TOTAL, 1, labels)
                yield response
        finally:
            if labels is not None:
                self.registry.add(REQUEST_RUNNING, -1, labels)


class MetricsServer(ThreadingHTTPServer):
    """``GET /metrics`` serves the registry; anything else is 404."""

    daemon_threads = True

    def __init__(self, port: int, registry: Registry) -> None:
        self.registry = registry

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, format: str, *args: Any) -> None:
                pass

            def do_GET(self) -> None:  # noqa: N802
                if self.path.split("?", 1)[0] != "/metrics":
                    self.send_error(404)
                    return
                body = registry.render().encode()
                self.send_response(200)
                self.send_header("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

        super().__init__(("0.0.0.0", port), Handler)


def scrape_annotations(deployment: Obj, port: int = METRICS_PORT) -> Obj:
    """A copy of a Deployment whose Pods Prometheus will scrape.

    Pod-template annotations ``prometheus.io/scrape: "true"``,
    ``prometheus.io/port: "<port>"`` and ``prometheus.io/path: /metrics``
    (the Prometheus chart's ``kubernetes-pods`` job reads them), and a
    container port named ``metrics`` on the first container. Do not modify
    the input.
    """
    out = copy.deepcopy(deployment)
    meta = out["spec"]["template"]["metadata"]
    meta.setdefault("annotations", {}).update(
        {
            "prometheus.io/scrape": "true",
            "prometheus.io/port": str(port),
            "prometheus.io/path": "/metrics",
        }
    )
    container = out["spec"]["template"]["spec"]["containers"][0]
    container.setdefault("ports", []).append(
        {"name": "metrics", "containerPort": port, "protocol": "TCP"}
    )
    return out


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
    return {
        "apiVersion": "keda.sh/v1alpha1",
        "kind": "ScaledObject",
        "metadata": {"name": name, "namespace": namespace},
        "spec": {
            "scaleTargetRef": {"apiVersion": "apps/v1", "kind": "Deployment", "name": deployment},
            "pollingInterval": 5,
            "minReplicaCount": min_replicas,
            "maxReplicaCount": max_replicas,
            "advanced": {
                "horizontalPodAutoscalerConfig": {
                    "behavior": {
                        "scaleUp": {"stabilizationWindowSeconds": stabilization_seconds},
                        "scaleDown": {"stabilizationWindowSeconds": stabilization_seconds},
                    }
                }
            },
            "triggers": [
                {
                    "type": "prometheus",
                    "metricType": "AverageValue",
                    "metadata": {
                        "serverAddress": prometheus_url,
                        "query": query,
                        "threshold": str(threshold),
                    },
                }
            ],
        },
    }


def install(rt: Runtime, argv: list[str]) -> None:
    """The ``observability`` feature: export ``llm_d_epp_*`` on ``--metrics-port`` (9090).

    Counts running requests by wrapping the servicer, refreshes pool gauges
    every tick, and, when day 10's flow controller is in ``rt.extras``,
    exports its queue size and saturation too.
    """
    p = argparse.ArgumentParser(prog="observability")
    p.add_argument("--metrics-port", type=int, default=METRICS_PORT)
    a, _ = p.parse_known_args(argv)
    registry = new_epp_registry()
    rt.extras["registry"] = registry

    def count(servicer: Any, director: RequestHandler) -> Any:
        return CountingServer(servicer, registry)

    rt.servicer_wrappers.append(count)

    def tick() -> None:
        refresh_pool_metrics(registry, rt.datastore, rt.opts.pool_name)
        controller = rt.extras.get("flow_controller")
        if controller is not None:
            labels = {"name": rt.opts.pool_name}
            registry.set(FLOW_CONTROL_QUEUE_SIZE, controller.queue_size(), labels)
            saturation = controller.detector.saturation(rt.datastore.pod_list(), controller.clock())
            registry.set(FLOW_CONTROL_POOL_SATURATION, saturation, labels)

    rt.ticks.append(tick)
    server = MetricsServer(a.metrics_port, registry)
    threading.Thread(target=server.serve_forever, daemon=True).start()
