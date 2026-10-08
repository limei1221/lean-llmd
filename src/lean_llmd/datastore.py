"""Day 3 - the datastore: which endpoints the router may pick from.

Your implementation. Every function below raises until you write it.

    uv run lean-llmd test --day 3         # your code
    uv run lean-llmd test --day 3 --ref   # proves the tests are passable
    uv run lean-llmd diff --day 3         # only after you have a passing run
"""

import threading
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, Protocol

from lean_llmd.metrics import Metrics

Pod = dict[str, Any]


@dataclass(frozen=True)
class NamespacedName:
    namespace: str
    name: str

    def __str__(self) -> str:
        raise NotImplementedError("day 3: implement __str__()")


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
        raise NotImplementedError("day 3: implement host_port()")


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
        raise NotImplementedError("day 3: implement update_metrics()")


class PodLister(Protocol):
    def list_pods(self, namespace: str, selector: dict[str, str]) -> list[Pod]: ...


class PoolNotSyncedError(Exception):
    pass


def is_pod_ready(pod: Pod) -> bool:
    """True if the Pod's ``Ready`` condition is ``"True"`` and it is not being deleted.

    ``pod`` is the API's JSON shape: ``metadata.deletionTimestamp``,
    ``status.conditions[].type`` / ``.status``. Missing fields mean not ready.
    """
    raise NotImplementedError("day 3: implement is_pod_ready()")


def labels_match(selector: dict[str, str], labels: dict[str, str]) -> bool:
    """``matchLabels`` semantics: every selector key present with an equal value."""
    raise NotImplementedError("day 3: implement labels_match()")


def endpoint_name(pod: Pod, rank: int) -> NamespacedName:
    """The endpoint for target port number ``rank`` of ``pod``: ``<pod>-rank-<rank>``."""
    raise NotImplementedError("day 3: implement endpoint_name()")


class Datastore:
    """Thread-safe store of the pool and its endpoints. Upstream: ``datastore``."""

    def __init__(self) -> None:
        raise NotImplementedError("day 3: implement __init__()")

    def pool_set(self, pool: EndpointPool | None, lister: PodLister) -> None:
        """Replace the pool. ``None`` clears everything.

        If the selector or target ports changed (or this is the first pool,
        or the last resync failed), resync every endpoint from ``lister``:
        no Pod event will ever tell you that a selector changed. If the
        resync raises, remember to retry it on the next ``pool_set``.
        """
        raise NotImplementedError("day 3: implement pool_set()")

    def pool_get(self) -> EndpointPool:
        """The current pool; raise ``PoolNotSyncedError`` if there is none."""
        raise NotImplementedError("day 3: implement pool_get()")

    def pool_has_synced(self) -> bool:
        raise NotImplementedError("day 3: implement pool_has_synced()")

    def pool_labels_match(self, labels: dict[str, str]) -> bool:
        """Does the current pool select a Pod with ``labels``? False with no pool."""
        raise NotImplementedError("day 3: implement pool_labels_match()")

    def pod_update_or_add_if_not_exist(self, pod: Pod) -> None:
        """Upsert one endpoint per pool target port for ``pod``.

        An endpoint that already exists keeps its ``Endpoint`` object (and so
        its metrics); only its metadata is refreshed. Skip silently if the
        pool has not synced: the resync on ``pool_set`` will pick the Pod up.
        """
        raise NotImplementedError("day 3: implement pod_update_or_add_if_not_exist()")

    def pod_delete(self, pod_name: str) -> None:
        """Remove every endpoint that belongs to Pod ``pod_name``."""
        raise NotImplementedError("day 3: implement pod_delete()")

    def pod_list(self, predicate: Callable[[Endpoint], bool] | None = None) -> list[Endpoint]:
        """A snapshot list of endpoints, optionally filtered, in insertion order."""
        raise NotImplementedError("day 3: implement pod_list()")

    def clear(self) -> None:
        raise NotImplementedError("day 3: implement clear()")
