"""Day 3 - discovering endpoints: an InferencePool, its Pods, and two reconcilers.

Shaped like llm-d-router's ``pkg/epp/controller``. Upstream uses
controller-runtime: informers cache objects, a work queue de-duplicates keys,
and ``Reconcile(key)`` re-reads the object and makes the datastore agree.
``Controller`` here is the smallest version of that machinery: list, then
watch from the list's resourceVersion, and re-list when the watch expires.
"""

import queue
import sys
import threading
from collections.abc import Callable, Iterator
from typing import Any, Protocol

from lean_llmd_ref.datastore import Datastore, EndpointPool, Pod, PodLister, is_pod_ready

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
    return {
        "apiVersion": f"{POOL_GROUP}/{POOL_VERSION}",
        "kind": "InferencePool",
        "metadata": {"name": name, "namespace": namespace},
        "spec": {
            "selector": {"matchLabels": dict(selector)},
            "targetPorts": [{"number": target_port}],
            "endpointPickerRef": {
                "name": epp_service,
                "port": {"number": epp_port},
                "failureMode": "FailOpen",
            },
        },
    }


def inference_pool_to_endpoint_pool(obj: Obj) -> EndpointPool:
    """Read name, namespace, ``matchLabels`` and target port numbers from an InferencePool."""
    spec = obj["spec"]
    return EndpointPool(
        name=obj["metadata"]["name"],
        namespace=obj["metadata"]["namespace"],
        selector=dict(spec["selector"]["matchLabels"]),
        target_ports=tuple(int(p["number"]) for p in spec["targetPorts"]),
    )


class Reader(PodLister, Protocol):
    def get_pod(self, namespace: str, name: str) -> Pod | None: ...
    def get_pool(self, namespace: str, name: str) -> Obj | None: ...


class PodReconciler:
    """Make the datastore agree with one Pod. Upstream: ``PodReconciler``."""

    def __init__(self, datastore: Datastore, reader: Reader) -> None:
        self.datastore = datastore
        self.reader = reader

    def reconcile(self, namespace: str, name: str) -> None:
        """Re-read Pod ``name`` and update the datastore.

        Do nothing until the pool has synced. A Pod that no longer exists is
        deleted; otherwise ``update_datastore``.
        """
        if not self.datastore.pool_has_synced():
            return
        pod = self.reader.get_pod(namespace, name)
        if pod is None:
            self.datastore.pod_delete(name)
            return
        self.update_datastore(pod)

    def update_datastore(self, pod: Pod) -> None:
        """Delete the Pod if it is not ready or the pool no longer selects it; else upsert it."""
        labels = pod["metadata"].get("labels") or {}
        if not is_pod_ready(pod) or not self.datastore.pool_labels_match(labels):
            self.datastore.pod_delete(pod["metadata"]["name"])
        else:
            self.datastore.pod_update_or_add_if_not_exist(pod)


class InferencePoolReconciler:
    """Make the datastore agree with the InferencePool. Upstream: ``InferencePoolReconciler``."""

    def __init__(self, datastore: Datastore, reader: Reader) -> None:
        self.datastore = datastore
        self.reader = reader

    def reconcile(self, namespace: str, name: str) -> None:
        """Re-read the pool. Missing or being deleted: clear. Otherwise ``pool_set``."""
        obj = self.reader.get_pool(namespace, name)
        if obj is None or obj["metadata"].get("deletionTimestamp"):
            self.datastore.clear()
            return
        self.datastore.pool_set(inference_pool_to_endpoint_pool(obj), self.reader)


class KubeClient:
    """The Kubernetes calls discovery needs, returning plain JSON-shaped dicts."""

    def __init__(self, context: str | None = None) -> None:
        from kubernetes import client, config

        try:
            config.load_incluster_config()
        except config.ConfigException:
            config.load_kube_config(context=context)
        self._api = client.ApiClient()
        self._core = client.CoreV1Api(self._api)
        self._custom = client.CustomObjectsApi(self._api)

    def get_pod(self, namespace: str, name: str) -> Pod | None:
        from kubernetes.client.exceptions import ApiException

        try:
            pod = self._core.read_namespaced_pod(name, namespace)
        except ApiException as e:
            if e.status == 404:
                return None
            raise
        return dict(self._api.sanitize_for_serialization(pod))

    def list_pods(self, namespace: str, selector: dict[str, str]) -> list[Pod]:
        selector_text = ",".join(f"{k}={v}" for k, v in sorted(selector.items()))
        pods = self._core.list_namespaced_pod(namespace, label_selector=selector_text)
        return [dict(self._api.sanitize_for_serialization(p)) for p in pods.items]

    def get_pool(self, namespace: str, name: str) -> Obj | None:
        from kubernetes.client.exceptions import ApiException

        try:
            obj = self._custom.get_namespaced_custom_object(
                POOL_GROUP, POOL_VERSION, namespace, POOL_PLURAL, name
            )
        except ApiException as e:
            if e.status == 404:
                return None
            raise
        return dict(obj)

    def list_and_watch(self, kind: str, namespace: str, stop: threading.Event) -> Iterator[Obj]:
        """Yield ``{"type", "object"}`` events for ``kind`` ("pods" or "pools") until ``stop``.

        First a synthetic ADDED for every object in a fresh list, then a watch
        from that list's resourceVersion. When the watch ends, resume from the
        last resourceVersion seen; when it expires (410 Gone), list again.
        """
        from kubernetes import watch
        from kubernetes.client.exceptions import ApiException

        def list_call(**kwargs: Any) -> Any:
            if kind == "pods":
                return self._core.list_namespaced_pod(namespace, **kwargs)
            return self._custom.list_namespaced_custom_object(
                POOL_GROUP, POOL_VERSION, namespace, POOL_PLURAL, **kwargs
            )

        while not stop.is_set():
            listed = self._api.sanitize_for_serialization(list_call())
            version = listed["metadata"]["resourceVersion"]
            for item in listed["items"]:
                yield {"type": "ADDED", "object": item}
            expired = False
            while not stop.is_set() and not expired:
                stream = watch.Watch().stream(
                    list_call, resource_version=version, timeout_seconds=5
                )
                try:
                    for event in stream:
                        obj = event.get("raw_object") or event["object"]
                        if event["type"] == "ERROR":
                            expired = obj.get("code") == 410
                            break
                        version = obj["metadata"]["resourceVersion"]
                        yield {"type": event["type"], "object": obj}
                        if stop.is_set():
                            return
                except ApiException as e:
                    if e.status != 410:
                        raise
                    expired = True


class Controller:
    """Watches Pods and one InferencePool, feeds keys through a queue to the reconcilers.

    Events are filtered and de-duplicated into ``(kind, name)`` keys; a single
    worker reconciles them one at a time, so reconcilers never race.
    """

    def __init__(
        self, datastore: Datastore, client: KubeClient, namespace: str, pool_name: str
    ) -> None:
        self.datastore = datastore
        self.client = client
        self.namespace = namespace
        self.pool_name = pool_name
        self.pods = PodReconciler(datastore, client)
        self.pools = InferencePoolReconciler(datastore, client)
        self.queue: queue.Queue[tuple[str, str]] = queue.Queue()
        self._queued: set[tuple[str, str]] = set()
        self._queued_lock = threading.Lock()
        self._last_labels: dict[str, dict[str, str]] = {}

    def enqueue(self, kind: str, name: str) -> None:
        """Queue ``(kind, name)`` unless it is already waiting."""
        key = (kind, name)
        with self._queued_lock:
            if key in self._queued:
                return
            self._queued.add(key)
        self.queue.put(key)

    def handle_pod_event(self, event_type: str, pod: Pod) -> None:
        """Enqueue the Pod if the pool selects it now or selected it before this event.

        The "before" labels are what this controller last saw for the Pod;
        forget them when the Pod is deleted.
        """
        name = pod["metadata"]["name"]
        labels = pod["metadata"].get("labels") or {}
        old = self._last_labels.get(name)
        if event_type == "DELETED":
            self._last_labels.pop(name, None)
        else:
            self._last_labels[name] = dict(labels)
        selected_before = old is not None and self.datastore.pool_labels_match(old)
        if selected_before or self.datastore.pool_labels_match(labels):
            self.enqueue("pod", name)

    def handle_pool_event(self, event_type: str, pool: Obj) -> None:
        """Enqueue the pool if it is the one this EPP serves."""
        if pool["metadata"]["name"] == self.pool_name:
            self.enqueue("pool", self.pool_name)

    def process_next(self, timeout: float = 0.5) -> bool:
        """Reconcile one queued key. False if the queue stayed empty for ``timeout``.

        A reconcile that raises (the API server hiccuped) is retried a second
        later rather than lost.
        """
        try:
            key = self.queue.get(timeout=timeout)
        except queue.Empty:
            return False
        with self._queued_lock:
            self._queued.discard(key)
        kind, name = key
        try:
            if kind == "pool":
                self.pools.reconcile(self.namespace, name)
            else:
                self.pods.reconcile(self.namespace, name)
        except Exception as e:
            print(f"reconcile {kind}/{name} failed, retrying: {e}", file=sys.stderr)
            threading.Timer(1.0, self.enqueue, args=(kind, name)).start()
        return True

    def run(self, stop: threading.Event) -> list[threading.Thread]:
        """Start the two watch threads and the worker; return them (daemon threads)."""

        def watch(kind: str, handle: Callable[[str, Obj], None]) -> None:
            while not stop.is_set():
                try:
                    for event in self.client.list_and_watch(kind, self.namespace, stop):
                        handle(event["type"], event["object"])
                except Exception as e:
                    print(f"watch {kind} failed, restarting: {e}", file=sys.stderr)
                    stop.wait(1.0)

        def work() -> None:
            while not stop.is_set():
                self.process_next()

        threads = [
            threading.Thread(target=watch, args=("pools", self.handle_pool_event), daemon=True),
            threading.Thread(target=watch, args=("pods", self.handle_pod_event), daemon=True),
            threading.Thread(target=work, daemon=True),
        ]
        for thread in threads:
            thread.start()
        return threads
