"""Day 3 - discovering endpoints: an InferencePool, its Pods, reconcilers.

Your implementation. Every function below raises until you write it.

    uv run lean-llmd test --day 3         # your code
    uv run lean-llmd test --day 3 --ref   # proves the tests are passable
    uv run lean-llmd diff --day 3         # only after you have a passing run
"""

import queue
import sys
import threading
from collections.abc import Callable, Iterator
from typing import Any, Protocol

from lean_llmd.datastore import Datastore, EndpointPool, Pod, PodLister, is_pod_ready

Obj = dict[str, Any]
POOL_GROUP = "inference.networking.k8s.io"
POOL_VERSION = "v1"
POOL_PLURAL = "inferencepools"


def inference_pool(
    name: str,
    namespace: str,
    selector: dict[str, str],
    target_port: int,
    epp_service: str,
    epp_port: int = 9002,
) -> Obj:
    """An ``inference.networking.k8s.io/v1`` InferencePool object.

    ``spec.selector.matchLabels`` is ``selector``; ``spec.targetPorts`` is
    one ``{"number": target_port}``; ``spec.endpointPickerRef`` names the
    EPP's Service and gRPC port, with ``failureMode: FailOpen``.
    """
    raise NotImplementedError("day 3: implement inference_pool()")


def inference_pool_to_endpoint_pool(obj: Obj) -> EndpointPool:
    """Read name, namespace, ``matchLabels`` and target port numbers from an InferencePool."""
    raise NotImplementedError("day 3: implement inference_pool_to_endpoint_pool()")


class Reader(PodLister, Protocol):
    def get_pod(self, namespace: str, name: str) -> Pod | None: ...

    def get_pool(self, namespace: str, name: str) -> Obj | None: ...


class PodReconciler:
    """Make the datastore agree with one Pod. Upstream: ``PodReconciler``."""

    def __init__(self, datastore: Datastore, reader: Reader) -> None:
        raise NotImplementedError("day 3: implement __init__()")

    def reconcile(self, namespace: str, name: str) -> None:
        """Re-read Pod ``name`` and update the datastore.

        Do nothing until the pool has synced. A Pod that no longer exists is
        deleted; otherwise ``update_datastore``.
        """
        raise NotImplementedError("day 3: implement reconcile()")

    def update_datastore(self, pod: Pod) -> None:
        """Delete the Pod if it is not ready or the pool no longer selects it; else upsert it."""
        raise NotImplementedError("day 3: implement update_datastore()")


class InferencePoolReconciler:
    """Make the datastore agree with the InferencePool. Upstream: ``InferencePoolReconciler``."""

    def __init__(self, datastore: Datastore, reader: Reader) -> None:
        raise NotImplementedError("day 3: implement __init__()")

    def reconcile(self, namespace: str, name: str) -> None:
        """Re-read the pool. Missing or being deleted: clear. Otherwise ``pool_set``."""
        raise NotImplementedError("day 3: implement reconcile()")


class KubeClient:
    """The Kubernetes calls discovery needs, returning plain JSON-shaped dicts."""

    def __init__(self, context: str | None = None) -> None:
        raise NotImplementedError("day 3: implement __init__()")

    def get_pod(self, namespace: str, name: str) -> Pod | None:
        raise NotImplementedError("day 3: implement get_pod()")

    def list_pods(self, namespace: str, selector: dict[str, str]) -> list[Pod]:
        raise NotImplementedError("day 3: implement list_pods()")

    def get_pool(self, namespace: str, name: str) -> Obj | None:
        raise NotImplementedError("day 3: implement get_pool()")

    def list_and_watch(self, kind: str, namespace: str, stop: threading.Event) -> Iterator[Obj]:
        """Yield ``{"type", "object"}`` events for ``kind`` ("pods" or "pools") until ``stop``.

        First a synthetic ADDED for every object in a fresh list, then a watch
        from that list's resourceVersion. When the watch ends, resume from the
        last resourceVersion seen; when it expires (410 Gone), list again.
        """
        raise NotImplementedError("day 3: implement list_and_watch()")


class Controller:
    """Watches Pods and one InferencePool, feeds keys through a queue to the reconcilers.

    Events are filtered and de-duplicated into ``(kind, name)`` keys; a single
    worker reconciles them one at a time, so reconcilers never race.
    """

    def __init__(
        self, datastore: Datastore, client: KubeClient, namespace: str, pool_name: str
    ) -> None:
        raise NotImplementedError("day 3: implement __init__()")

    def enqueue(self, kind: str, name: str) -> None:
        """Queue ``(kind, name)`` unless it is already waiting."""
        raise NotImplementedError("day 3: implement enqueue()")

    def handle_pod_event(self, event_type: str, pod: Pod) -> None:
        """Enqueue the Pod if the pool selects it now or selected it before this event.

        The "before" labels are what this controller last saw for the Pod;
        forget them when the Pod is deleted.
        """
        raise NotImplementedError("day 3: implement handle_pod_event()")

    def handle_pool_event(self, event_type: str, pool: Obj) -> None:
        """Enqueue the pool if it is the one this EPP serves."""
        raise NotImplementedError("day 3: implement handle_pool_event()")

    def process_next(self, timeout: float = 0.5) -> bool:
        """Reconcile one queued key. False if the queue stayed empty for ``timeout``.

        A reconcile that raises (the API server hiccuped) is retried a second
        later rather than lost.
        """
        raise NotImplementedError("day 3: implement process_next()")

    def run(self, stop: threading.Event) -> list[threading.Thread]:
        """Start the two watch threads and the worker; return them (daemon threads)."""
        raise NotImplementedError("day 3: implement run()")
