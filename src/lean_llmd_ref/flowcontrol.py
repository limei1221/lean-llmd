"""Day 10 - flow control: queue in the EPP, not in the model servers.

Shaped like llm-d-router's ``pkg/epp/flowcontrol`` with the
``concurrency-detector`` (and ``utilization-detector``),
``round-robin-fairness-policy`` and FCFS ordering.
When the pool is saturated, requests wait in the EPP, one queue per flow
``(fairness id, priority)``. Bands dispatch in strict priority order, flows
within a band take turns, and a request that waits too long gets a 429.
Run it with ``--feature flowcontrol``.
"""

import argparse
import collections
import threading
import time
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from typing import Any, Protocol

import grpc
from envoy.service.ext_proc.v3 import external_processor_pb2 as pb
from envoy.service.ext_proc.v3 import external_processor_pb2_grpc as pb_grpc

from lean_llmd_ref.datastore import Endpoint
from lean_llmd_ref.epp import Runtime
from lean_llmd_ref.extproc import RequestHandler, RequestRejected
from lean_llmd_ref.scheduling import InferenceRequest

Obj = dict[str, Any]

FAIRNESS_ID_HEADER = "x-llm-d-inference-fairness-id"
OBJECTIVE_HEADER = "x-llm-d-inference-objective"
OBJECTIVE_GROUP, OBJECTIVE_VERSION, OBJECTIVE_PLURAL = "llm-d.ai", "v1alpha2", "inferenceobjectives"
DEFAULT_FAIRNESS_ID = "default"


@dataclass(frozen=True, order=True)
class FlowKey:
    fairness_id: str
    priority: int


class SaturationDetector(Protocol):
    def saturation(self, endpoints: list[Endpoint], now: float) -> float: ...


class InflightCounter:
    """Requests the pool is serving right now, counted from the EPP's side.

    Envoy keeps a request's ext-proc stream open until the response is done,
    so "routed and stream not yet closed" is exactly "in flight".
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.value = 0

    def add(self, delta: int) -> None:
        with self._lock:
            self.value += delta


class ConcurrencyDetector:
    """Saturation as in-flight requests over capacity. Upstream: ``concurrency-detector``.

    Unlike scraped metrics, the count moves the instant a request is
    released, so a burst cannot all slip through before the next scrape.
    """

    name = "concurrency-detector"

    def __init__(self, load: Callable[[], int], max_concurrency: int = 4) -> None:
        self.load = load
        self.max_concurrency = max_concurrency

    def saturation(self, endpoints: list[Endpoint], now: float) -> float:
        """``load() / (len(endpoints) * max_concurrency)``; 1.0 with no endpoints."""
        if not endpoints:
            return 1.0
        return self.load() / (len(endpoints) * self.max_concurrency)


class UtilizationDetector:
    """How saturated the pool is, from scraped metrics. Upstream: ``utilization-detector``."""

    name = "utilization-detector"

    def __init__(
        self, queue_threshold: int = 5, kv_threshold: float = 0.8, staleness_s: float = 2.0
    ) -> None:
        self.queue_threshold = queue_threshold
        self.kv_threshold = kv_threshold
        self.staleness_s = staleness_s

    def saturation(self, endpoints: list[Endpoint], now: float) -> float:
        """Average over endpoints of ``max(queue / queue_threshold, kv / kv_threshold)``.

        An endpoint whose metrics are older than ``staleness_s`` (or never
        scraped) scores 1.0. No endpoints at all scores 1.0. A result at or
        above 1.0 means "saturated: hold requests".
        """
        if not endpoints:
            return 1.0
        total = 0.0
        for e in endpoints:
            m = e.metrics
            if m.update_time <= 0 or now - m.update_time > self.staleness_s:
                total += 1.0
                continue
            total += max(
                m.waiting_queue_size / self.queue_threshold,
                m.kv_cache_usage_percent / self.kv_threshold,
            )
        return total / len(endpoints)


class RoundRobinFairness:
    """Within a band, flows take turns. Upstream: ``round-robin-fairness-policy``."""

    def __init__(self) -> None:
        self._cursor: dict[int, FlowKey] = {}

    def pick(self, priority: int, nonempty: list[FlowKey]) -> FlowKey:
        """The first flow after the last one picked in this band, in sorted order (wrapping).

        ``nonempty`` lists the band's flows that have queued requests; it is
        never empty.
        """
        flows = sorted(nonempty)
        last = self._cursor.get(priority)
        chosen = flows[0]
        if last is not None:
            later = [f for f in flows if f > last]
            chosen = later[0] if later else flows[0]
        self._cursor[priority] = chosen
        return chosen


@dataclass
class QueuedRequest:
    request: InferenceRequest
    key: FlowKey
    enqueued: float
    released: threading.Event = field(default_factory=threading.Event)
    expired: bool = False


class FlowController:
    """Per-flow FCFS queues, dispatched by priority, fairness and saturation."""

    def __init__(
        self,
        detector: SaturationDetector,
        endpoints: Callable[[], list[Endpoint]],
        request_ttl_s: float = 60.0,
        clock: Callable[[], float] = time.time,
        inflight: "InflightCounter | None" = None,
    ) -> None:
        self.detector = detector
        self.endpoints = endpoints
        self.request_ttl_s = request_ttl_s
        self.clock = clock
        self.inflight = inflight
        self.fairness = RoundRobinFairness()
        self._lock = threading.Condition()
        self._queues: dict[FlowKey, collections.deque[QueuedRequest]] = {}

    def enqueue(self, request: InferenceRequest, key: FlowKey) -> QueuedRequest:
        """Append to the flow's queue (creating it) and wake the dispatcher."""
        item = QueuedRequest(request, key, self.clock())
        with self._lock:
            self._queues.setdefault(key, collections.deque()).append(item)
            self._lock.notify_all()
        return item

    def queue_size(self) -> int:
        with self._lock:
            return sum(len(q) for q in self._queues.values())

    def expire(self) -> int:
        """Release every request older than ``request_ttl_s`` with ``expired=True``; count them."""
        now = self.clock()
        count = 0
        with self._lock:
            for queue in self._queues.values():
                while queue and now - queue[0].enqueued > self.request_ttl_s:
                    item = queue.popleft()
                    item.expired = True
                    item.released.set()
                    count += 1
        return count

    def dispatch_once(self) -> QueuedRequest | None:
        """Release one request if the pool has room; return it (or ``None``).

        1. Saturation ``>= 1.0``: release nothing.
        2. Band: the highest priority with a non-empty queue.
        3. Flow: ``fairness.pick`` among that band's non-empty flows.
        4. Request: the oldest in that flow (FCFS).

        Count the request in ``inflight`` (if set) *before* releasing it: the
        next call must already see it, or a burst all slips through before
        the released threads wake up.
        """
        if self.detector.saturation(self.endpoints(), self.clock()) >= 1.0:
            return None
        with self._lock:
            nonempty = [k for k, q in self._queues.items() if q]
            if not nonempty:
                return None
            priority = max(k.priority for k in nonempty)
            band = [k for k in nonempty if k.priority == priority]
            item = self._queues[self.fairness.pick(priority, band)].popleft()
            if self.inflight is not None:
                self.inflight.add(1)
        item.released.set()
        return item

    def run(self, stop: threading.Event, interval_s: float = 0.005) -> None:
        """Dispatch until ``stop``: expire, then release as many as the pool takes."""
        while not stop.is_set():
            self.expire()
            while self.dispatch_once() is not None:
                pass
            with self._lock:
                self._lock.wait(interval_s)


class ObjectiveStore:
    """InferenceObjective name to priority, refreshed from the API server."""

    def __init__(self) -> None:
        self._priorities: dict[str, int] = {}

    def update(self, objects: list[Obj]) -> None:
        """Replace the table from listed objects (``spec.priority``, default 0)."""
        self._priorities = {
            o["metadata"]["name"]: int((o.get("spec") or {}).get("priority") or 0) for o in objects
        }

    def priority(self, name: str | None) -> int:
        """An unknown or missing objective is priority 0."""
        return self._priorities.get(name or "", 0)


def flow_key(request: InferenceRequest, objectives: ObjectiveStore) -> FlowKey:
    """The request's flow: its fairness id header (else ``"default"``) and objective's priority."""
    fairness_id = request.headers.get(FAIRNESS_ID_HEADER) or DEFAULT_FAIRNESS_ID
    return FlowKey(fairness_id, objectives.priority(request.headers.get(OBJECTIVE_HEADER)))


class FlowControlledDirector:
    """Admission in front of a ``Director``: wait for a dispatch slot, then schedule.

    The controller counts a request in ``inflight`` when it releases it;
    ``TrackingServer`` takes it out when the stream closes.
    """

    def __init__(
        self,
        inner: RequestHandler,
        controller: FlowController,
        objectives: ObjectiveStore,
        inflight: InflightCounter,
    ) -> None:
        self.inner = inner
        self.controller = controller
        self.objectives = objectives
        self.inflight = inflight
        self.admitted = threading.local()

    def handle_request(self, request: InferenceRequest) -> str:
        """Enqueue under ``flow_key``, block until released, then schedule.

        An expired request raises ``RequestRejected(429, ...)``: capacity
        exists but is contended, so the client should back off and retry.
        Otherwise mark it admitted on this thread (``self.admitted.value``)
        so ``TrackingServer`` releases its in-flight count when the stream ends.
        """
        item = self.controller.enqueue(request, flow_key(request, self.objectives))
        item.released.wait()
        if item.expired:
            raise RequestRejected(429, "request timed out in the flow-control queue")
        self.admitted.value = True
        return self.inner.handle_request(request)

    def any_endpoint(self) -> str:
        return self.inner.any_endpoint()


class TrackingServer(pb_grpc.ExternalProcessorServicer):  # type: ignore[misc]
    """Wraps another servicer; releases a request's in-flight count when its stream ends.

    gRPC runs one stream's generator on one thread, so the director's
    thread-local mark belongs to this stream.
    """

    def __init__(self, inner: Any, director: FlowControlledDirector) -> None:
        self.inner = inner
        self.flow = director

    def Process(  # noqa: N802
        self, request_iterator: Iterator[pb.ProcessingRequest], context: grpc.ServicerContext
    ) -> Iterator[pb.ProcessingResponse]:
        """Yield everything the inner servicer yields; on the way out, if this
        stream was admitted, subtract it from ``inflight`` (exactly once)."""
        self.flow.admitted.value = False
        try:
            yield from self.inner.Process(request_iterator, context)
        finally:
            if getattr(self.flow.admitted, "value", False):
                self.flow.admitted.value = False
                self.flow.inflight.add(-1)


def inference_objective(name: str, namespace: str, pool: str, priority: int) -> Obj:
    """An ``llm-d.ai/v1alpha2`` InferenceObjective pointing at InferencePool ``pool``."""
    return {
        "apiVersion": f"{OBJECTIVE_GROUP}/{OBJECTIVE_VERSION}",
        "kind": "InferenceObjective",
        "metadata": {"name": name, "namespace": namespace},
        "spec": {"priority": priority, "poolRef": {"name": pool}},
    }


def objective_rbac(name: str, namespace: str) -> list[Obj]:
    """A Role ``<name>-objectives`` (get/list/watch InferenceObjectives) bound to SA ``name``.

    RBAC is additive: this grant sits beside day 6's Role, which stays as it was.
    """
    role = f"{name}-objectives"
    return [
        {
            "apiVersion": "rbac.authorization.k8s.io/v1",
            "kind": "Role",
            "metadata": {"name": role, "namespace": namespace},
            "rules": [
                {
                    "apiGroups": [OBJECTIVE_GROUP],
                    "resources": [OBJECTIVE_PLURAL],
                    "verbs": ["get", "list", "watch"],
                }
            ],
        },
        {
            "apiVersion": "rbac.authorization.k8s.io/v1",
            "kind": "RoleBinding",
            "metadata": {"name": role, "namespace": namespace},
            "subjects": [{"kind": "ServiceAccount", "name": name, "namespace": namespace}],
            "roleRef": {"apiGroup": "rbac.authorization.k8s.io", "kind": "Role", "name": role},
        },
    ]


def install(rt: Runtime, argv: list[str]) -> None:
    """The ``flowcontrol`` feature.

    Flags: ``--saturation`` (``concurrency`` or ``utilization``),
    ``--max-concurrency`` per endpoint (4), ``--queue-depth-threshold`` (5),
    ``--kv-cache-threshold`` (0.8), ``--request-ttl`` seconds (60). Wraps
    the director and the gRPC servicer, runs the dispatcher on its own
    thread, re-lists InferenceObjectives every 5 s, and leaves the controller
    in ``rt.extras["flow_controller"]`` and the counter in
    ``rt.extras["inflight"]``.
    """
    p = argparse.ArgumentParser(prog="flowcontrol")
    p.add_argument("--saturation", choices=["concurrency", "utilization"], default="concurrency")
    p.add_argument("--max-concurrency", type=int, default=4)
    p.add_argument("--queue-depth-threshold", type=int, default=5)
    p.add_argument("--kv-cache-threshold", type=float, default=0.8)
    p.add_argument("--request-ttl", type=float, default=60.0)
    a, _ = p.parse_known_args(argv)
    inflight = InflightCounter()
    detector: SaturationDetector
    if a.saturation == "concurrency":
        detector = ConcurrencyDetector(lambda: inflight.value, a.max_concurrency)
    else:
        detector = UtilizationDetector(a.queue_depth_threshold, a.kv_cache_threshold)
    controller = FlowController(detector, rt.datastore.pod_list, a.request_ttl, inflight=inflight)
    objectives = ObjectiveStore()
    rt.extras["flow_controller"] = controller
    rt.extras["inflight"] = inflight

    def wrap(director: RequestHandler) -> RequestHandler:
        return FlowControlledDirector(director, controller, objectives, inflight)

    def track(servicer: Any, director: RequestHandler) -> Any:
        assert isinstance(director, FlowControlledDirector)
        return TrackingServer(servicer, director)

    rt.director_wrappers.append(wrap)
    rt.servicer_wrappers.append(track)
    threading.Thread(target=controller.run, args=(rt.stop,), daemon=True).start()

    last = [0.0]

    def refresh_objectives() -> None:
        if time.time() - last[0] < 5.0:
            return
        last[0] = time.time()
        try:
            from kubernetes import client

            listed = client.CustomObjectsApi().list_namespaced_custom_object(
                OBJECTIVE_GROUP, OBJECTIVE_VERSION, rt.opts.pool_namespace, OBJECTIVE_PLURAL
            )
            objectives.update(list(listed.get("items", [])))
        except Exception as e:
            print(f"flowcontrol: listing InferenceObjectives failed: {e}", flush=True)

    rt.ticks.append(refresh_objectives)
