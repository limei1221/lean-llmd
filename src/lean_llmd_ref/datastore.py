"""Day 3 - the datastore: which endpoints the router may pick from.

Shaped like llm-d-router's ``pkg/epp/datastore``. It holds one pool (the
selector and ports from an InferencePool) and one ``Endpoint`` per ready Pod
per target port. Reconcilers write it; the scheduler reads it.
"""

import threading
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, Protocol

from lean_llmd_ref.metrics import Metrics

Pod = dict[str, Any]


@dataclass(frozen=True)
class NamespacedName:
    namespace: str
    name: str

    def __str__(self) -> str:
        return f"{self.namespace}/{self.name}"


@dataclass(frozen=True)
class EndpointPool:
    """The parts of an InferencePool the datastore needs."""

    name: str
    namespace: str
    selector: dict[str, str]
    target_ports: tuple[int, ...]


@dataclass(frozen=True)
class EndpointMetadata:
    id: NamespacedName
    pod_name: str
    address: str
    port: int
    labels: dict[str, str]

    @property
    def host_port(self) -> str:
        return f"{self.address}:{self.port}"


@dataclass(eq=False)
class Endpoint:
    """One model-server port the scheduler can pick. Hashes by identity.

    ``attributes`` carries per-request data that producers write before
    scheduling (day 5's prefix match, for example).
    """

    metadata: EndpointMetadata
    metrics: Metrics = field(default_factory=Metrics)
    attributes: dict[str, Any] = field(default_factory=dict)

    def update_metrics(self, metrics: Metrics) -> None:
        self.metrics = metrics


class PodLister(Protocol):
    def list_pods(self, namespace: str, selector: dict[str, str]) -> list[Pod]: ...


class PoolNotSyncedError(Exception):
    pass


def is_pod_ready(pod: Pod) -> bool:
    """True if the Pod's ``Ready`` condition is ``"True"`` and it is not being deleted.

    ``pod`` is the API's JSON shape: ``metadata.deletionTimestamp``,
    ``status.conditions[].type`` / ``.status``. Missing fields mean not ready.
    """
    if pod.get("metadata", {}).get("deletionTimestamp"):
        return False
    for condition in pod.get("status", {}).get("conditions") or []:
        if condition.get("type") == "Ready":
            return bool(condition.get("status") == "True")
    return False


def labels_match(selector: dict[str, str], labels: dict[str, str]) -> bool:
    """``matchLabels`` semantics: every selector key present with an equal value."""
    return all(labels.get(k) == v for k, v in selector.items())


def endpoint_name(pod: Pod, rank: int) -> NamespacedName:
    """The endpoint for target port number ``rank`` of ``pod``: ``<pod>-rank-<rank>``."""
    meta = pod["metadata"]
    return NamespacedName(meta.get("namespace", ""), f"{meta['name']}-rank-{rank}")


class Datastore:
    """Thread-safe store of the pool and its endpoints. Upstream: ``datastore``."""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._pool: EndpointPool | None = None
        self._endpoints: dict[NamespacedName, Endpoint] = {}
        self._needs_resync = False

    def pool_set(self, pool: EndpointPool | None, lister: PodLister) -> None:
        """Replace the pool. ``None`` clears everything.

        If the selector or target ports changed (or this is the first pool,
        or the last resync failed), resync every endpoint from ``lister``:
        no Pod event will ever tell you that a selector changed. If the
        resync raises, remember to retry it on the next ``pool_set``.
        """
        if pool is None:
            self.clear()
            return
        with self._lock:
            old = self._pool
            self._pool = pool
            selector_changed = old is None or old.selector != pool.selector
            ports_changed = old is not None and old.target_ports != pool.target_ports
            if selector_changed or ports_changed or self._needs_resync:
                try:
                    self._pod_resync_all(lister)
                except Exception:
                    self._needs_resync = True
                    raise
                self._needs_resync = False

    def pool_get(self) -> EndpointPool:
        """The current pool; raise ``PoolNotSyncedError`` if there is none."""
        with self._lock:
            if self._pool is None:
                raise PoolNotSyncedError("InferencePool not synced yet")
            return self._pool

    def pool_has_synced(self) -> bool:
        with self._lock:
            return self._pool is not None

    def pool_labels_match(self, labels: dict[str, str]) -> bool:
        """Does the current pool select a Pod with ``labels``? False with no pool."""
        with self._lock:
            return self._pool is not None and labels_match(self._pool.selector, labels)

    def pod_update_or_add_if_not_exist(self, pod: Pod) -> None:
        """Upsert one endpoint per pool target port for ``pod``.

        An endpoint that already exists keeps its ``Endpoint`` object (and so
        its metrics); only its metadata is refreshed. Skip silently if the
        pool has not synced: the resync on ``pool_set`` will pick the Pod up.
        """
        with self._lock:
            if self._pool is None:
                return
            meta = pod["metadata"]
            address = pod.get("status", {}).get("podIP", "")
            for rank, port in enumerate(self._pool.target_ports):
                key = endpoint_name(pod, rank)
                metadata = EndpointMetadata(
                    id=key,
                    pod_name=meta["name"],
                    address=address,
                    port=port,
                    labels=dict(meta.get("labels") or {}),
                )
                existing = self._endpoints.get(key)
                if existing is None:
                    self._endpoints[key] = Endpoint(metadata)
                else:
                    existing.metadata = metadata

    def pod_delete(self, pod_name: str) -> None:
        """Remove every endpoint that belongs to Pod ``pod_name``."""
        with self._lock:
            for key in [k for k, e in self._endpoints.items() if e.metadata.pod_name == pod_name]:
                del self._endpoints[key]

    def pod_list(self, predicate: Callable[[Endpoint], bool] | None = None) -> list[Endpoint]:
        """A snapshot list of endpoints, optionally filtered, in insertion order."""
        with self._lock:
            return [e for e in self._endpoints.values() if predicate is None or predicate(e)]

    def clear(self) -> None:
        with self._lock:
            self._pool = None
            self._endpoints.clear()

    def _pod_resync_all(self, lister: PodLister) -> None:
        """List the pool's Pods, upsert the ready ones, drop every other endpoint."""
        assert self._pool is not None
        pods = lister.list_pods(self._pool.namespace, self._pool.selector)
        active: set[NamespacedName] = set()
        for pod in pods:
            if not is_pod_ready(pod):
                continue
            for rank in range(len(self._pool.target_ports)):
                active.add(endpoint_name(pod, rank))
            self.pod_update_or_add_if_not_exist(pod)
        for key in [k for k in self._endpoints if k not in active]:
            del self._endpoints[key]
