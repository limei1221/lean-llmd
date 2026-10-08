"""Day 3 - discovering endpoints from an InferencePool.

Oracles: hand-built Pod objects for the logic, and on kind, Kubernetes' own
EndpointSlice controller. It computes "ready Pods behind this selector" for
every Service; your datastore must converge to the same set of IPs.
"""

import threading

import pytest

import k8s


def pod(name, ip="10.0.0.1", labels=None, ready=True, deleting=False, namespace="ns"):
    meta = {"name": name, "namespace": namespace, "labels": labels or {"app": "sim"}}
    if deleting:
        meta["deletionTimestamp"] = "2026-01-01T00:00:00Z"
    conditions = [
        {"type": "ContainersReady", "status": "True"},
        {"type": "Ready", "status": "True" if ready else "False"},
    ]
    return {"metadata": meta, "status": {"podIP": ip, "conditions": conditions}}


class FakeReader:
    def __init__(self, pods=(), pools=()):
        self.pods = {p["metadata"]["name"]: p for p in pods}
        self.pools = {p["metadata"]["name"]: p for p in pools}
        self.list_calls = 0
        self.fail_next_list = False

    def list_pods(self, namespace, selector):
        self.list_calls += 1
        if self.fail_next_list:
            self.fail_next_list = False
            raise ConnectionError("API server unavailable")
        return [
            p
            for p in self.pods.values()
            if all((p["metadata"].get("labels") or {}).get(k) == v for k, v in selector.items())
        ]

    def get_pod(self, namespace, name):
        return self.pods.get(name)

    def get_pool(self, namespace, name):
        return self.pools.get(name)


@pytest.fixture
def ds(mod):
    return mod("datastore")


@pytest.fixture
def disc(mod):
    return mod("discovery")


def pool(ds, selector=None, ports=(8000,)):
    return ds.EndpointPool("pool", "ns", selector or {"app": "sim"}, tuple(ports))


def addresses(store):
    return sorted(e.metadata.host_port for e in store.pod_list())


def test_is_pod_ready(ds):
    assert ds.is_pod_ready(pod("a"))
    assert not ds.is_pod_ready(pod("a", ready=False))
    assert not ds.is_pod_ready(pod("a", deleting=True)), "a terminating pod takes no new work"
    no_conditions = pod("a")
    no_conditions["status"]["conditions"] = []
    assert not ds.is_pod_ready(no_conditions)
    only_containers = pod("a")
    only_containers["status"]["conditions"] = [{"type": "ContainersReady", "status": "True"}]
    assert not ds.is_pod_ready(only_containers)
    assert not ds.is_pod_ready({"metadata": {"name": "pending"}})


def test_labels_match(ds):
    assert ds.labels_match({"app": "sim"}, {"app": "sim", "x": "y"})
    assert not ds.labels_match({"app": "sim"}, {"app": "other"})
    assert not ds.labels_match({"app": "sim", "tier": "a"}, {"app": "sim"})


def test_the_first_pool_resyncs_ready_pods_only(ds):
    reader = FakeReader(
        [pod("a", "10.0.0.1"), pod("b", "10.0.0.2", ready=False), pod("c", "10.0.0.3")]
    )
    store = ds.Datastore()
    assert not store.pool_has_synced()
    store.pool_set(pool(ds), reader)
    assert store.pool_has_synced()
    assert addresses(store) == ["10.0.0.1:8000", "10.0.0.3:8000"]
    ids = sorted(str(e.metadata.id) for e in store.pod_list())
    assert ids == ["ns/a-rank-0", "ns/c-rank-0"]


def test_each_target_port_is_an_endpoint(ds):
    store = ds.Datastore()
    store.pool_set(pool(ds, ports=(8000, 8001)), FakeReader([pod("a", "10.0.0.1")]))
    assert addresses(store) == ["10.0.0.1:8000", "10.0.0.1:8001"]
    assert sorted(e.metadata.id.name for e in store.pod_list()) == ["a-rank-0", "a-rank-1"]
    store.pod_delete("a")
    assert store.pod_list() == []


def test_an_unchanged_pool_does_not_relist(ds):
    reader = FakeReader([pod("a")])
    store = ds.Datastore()
    store.pool_set(pool(ds), reader)
    store.pool_set(pool(ds), reader)
    assert reader.list_calls == 1


def test_a_new_selector_resyncs_everything(ds):
    reader = FakeReader([pod("a", "10.0.0.1"), pod("b", "10.0.0.2", labels={"app": "other"})])
    store = ds.Datastore()
    store.pool_set(pool(ds), reader)
    assert addresses(store) == ["10.0.0.1:8000"]
    store.pool_set(pool(ds, selector={"app": "other"}), reader)
    assert addresses(store) == ["10.0.0.2:8000"]


def test_a_failed_resync_is_retried_on_the_next_pool_set(ds):
    reader = FakeReader([pod("a", "10.0.0.1")])
    store = ds.Datastore()
    reader.fail_next_list = True
    with pytest.raises(ConnectionError):
        store.pool_set(pool(ds), reader)
    store.pool_set(pool(ds), reader)
    assert addresses(store) == ["10.0.0.1:8000"]


def test_an_upsert_keeps_the_endpoint_and_its_metrics(ds, mod):
    metrics = mod("metrics")
    store = ds.Datastore()
    store.pool_set(pool(ds), FakeReader([pod("a", "10.0.0.1")]))
    (endpoint,) = store.pod_list()
    endpoint.update_metrics(metrics.Metrics(waiting_queue_size=7))
    store.pod_update_or_add_if_not_exist(pod("a", "10.0.0.1", labels={"app": "sim", "v": "2"}))
    (again,) = store.pod_list()
    assert again is endpoint
    assert again.metrics.waiting_queue_size == 7
    assert again.metadata.labels["v"] == "2"


def test_pods_before_the_pool_are_ignored(ds):
    store = ds.Datastore()
    store.pod_update_or_add_if_not_exist(pod("a"))
    assert store.pod_list() == []
    assert not store.pool_labels_match({"app": "sim"})
    with pytest.raises(ds.PoolNotSyncedError):
        store.pool_get()


def test_pool_manifest_round_trips(disc):
    obj = disc.inference_pool("p", "ns", {"app": "sim"}, 8000, "epp")
    assert obj["apiVersion"] == "inference.networking.k8s.io/v1"
    assert obj["spec"]["endpointPickerRef"] == {
        "name": "epp",
        "port": {"number": 9002},
        "failureMode": "FailOpen",
    }
    converted = disc.inference_pool_to_endpoint_pool(obj)
    assert (converted.name, converted.namespace) == ("p", "ns")
    assert converted.selector == {"app": "sim"}
    assert tuple(converted.target_ports) == (8000,)


def test_the_pod_reconciler(ds, disc):
    reader = FakeReader([pod("a", "10.0.0.1"), pod("b", "10.0.0.2")])
    store = ds.Datastore()
    reconciler = disc.PodReconciler(store, reader)
    reconciler.reconcile("ns", "a")
    assert store.pod_list() == [], "no pool yet: nothing to reconcile against"
    store.pool_set(pool(ds), reader)
    assert len(store.pod_list()) == 2
    reader.pods["a"] = pod("a", "10.0.0.1", ready=False)
    reconciler.reconcile("ns", "a")
    assert addresses(store) == ["10.0.0.2:8000"]
    reader.pods["a"] = pod("a", "10.0.0.1")
    reconciler.reconcile("ns", "a")
    assert len(store.pod_list()) == 2
    reader.pods["a"] = pod("a", "10.0.0.1", labels={"app": "moved"})
    reconciler.reconcile("ns", "a")
    assert addresses(store) == ["10.0.0.2:8000"]
    del reader.pods["b"]
    reconciler.reconcile("ns", "b")
    assert store.pod_list() == []


def test_the_pool_reconciler(ds, disc):
    obj = disc.inference_pool("pool", "ns", {"app": "sim"}, 8000, "epp")
    reader = FakeReader([pod("a")], [obj])
    store = ds.Datastore()
    reconciler = disc.InferencePoolReconciler(store, reader)
    reconciler.reconcile("ns", "pool")
    assert len(store.pod_list()) == 1
    obj["metadata"]["deletionTimestamp"] = "2026-01-01T00:00:00Z"
    reconciler.reconcile("ns", "pool")
    assert not store.pool_has_synced() and store.pod_list() == []
    reader.pools.clear()
    reconciler.reconcile("ns", "pool")
    assert not store.pool_has_synced()


def test_a_pod_leaving_the_pool_is_still_reconciled(ds, disc):
    """The predicate must look at the old labels too, or the pod is never removed."""
    reader = FakeReader([pod("a", "10.0.0.1")])
    store = ds.Datastore()
    store.pool_set(pool(ds), reader)
    controller = disc.Controller(store, reader, "ns", "pool")
    controller.handle_pod_event("ADDED", pod("a", "10.0.0.1"))
    assert controller.process_next(timeout=0.1)
    moved = pod("a", "10.0.0.1", labels={"app": "moved"})
    reader.pods["a"] = moved
    controller.handle_pod_event("MODIFIED", moved)
    assert controller.process_next(timeout=0.1), "relabelled pod was filtered out"
    assert store.pod_list() == []
    controller.handle_pod_event("MODIFIED", pod("z", labels={"app": "unrelated"}))
    assert not controller.process_next(timeout=0.1)


def test_the_work_queue_deduplicates(ds, disc):
    reader = FakeReader([pod("a")])
    store = ds.Datastore()
    store.pool_set(pool(ds), reader)
    controller = disc.Controller(store, reader, "ns", "pool")
    for _ in range(5):
        controller.handle_pod_event("MODIFIED", pod("a"))
    assert controller.process_next(timeout=0.1)
    assert not controller.process_next(timeout=0.1)


def test_only_our_pool_is_watched(ds, disc):
    reader = FakeReader()
    controller = disc.Controller(ds.Datastore(), reader, "ns", "pool")
    controller.handle_pool_event("ADDED", {"metadata": {"name": "someone-elses-pool"}})
    assert not controller.process_next(timeout=0.1)


@pytest.fixture
def cluster(mod):
    """Simulators, a Service over them (the oracle) and an InferencePool, in a fresh namespace."""
    manifests = mod("manifests")
    disc = mod("discovery")
    with k8s.temp_namespace("day03") as ns:
        k8s.apply(
            [
                manifests.sim_deployment(namespace=ns, replicas=3),
                manifests.sim_service(namespace=ns),
                disc.inference_pool("pool", ns, {"app": "sim"}, manifests.SIM_PORT, "epp"),
            ]
        )
        yield ns


@pytest.mark.kind
def test_the_api_server_accepts_your_pool(disc):
    obj = disc.inference_pool("p", "default", {"app": "sim"}, 8000, "epp")
    assert "created (server dry run)" in k8s.apply([obj], dry_run=True)


@pytest.mark.kind
def test_discovery_converges_with_the_endpointslice_controller(mod, cluster):
    ds, disc = mod("datastore"), mod("discovery")
    ns = cluster
    store = ds.Datastore()
    controller = disc.Controller(store, disc.KubeClient(k8s.CONTEXT), ns, "pool")
    stop = threading.Event()
    controller.run(stop)

    def converged(expected_count):
        def check():
            truth = k8s.endpointslice_ready_ips(ns, "sim")
            mine = {e.metadata.address for e in store.pod_list()}
            ports = {e.metadata.port for e in store.pod_list()}
            return len(truth) == expected_count and mine == truth and ports <= {8000}

        return check

    try:
        k8s.wait_until(converged(3), timeout=240, what="3 endpoints")

        k8s.kubectl("scale", "deployment/sim", "--replicas=4", "-n", ns)
        k8s.wait_until(converged(4), timeout=120, what="4 endpoints after scale-up")

        victim = k8s.get_json("pods", "-n", ns, "-l", "app=sim")["items"][0]
        k8s.kubectl(
            "label", "pod", victim["metadata"]["name"], "app=quarantined", "--overwrite", "-n", ns
        )
        k8s.wait_until(
            lambda: victim["status"]["podIP"] not in {e.metadata.address for e in store.pod_list()},
            timeout=60,
            what="the relabelled pod to leave the pool",
        )
        k8s.wait_until(converged(4), timeout=120, what="the replacement pod")

        k8s.kubectl(
            "patch",
            "inferencepool",
            "pool",
            "-n",
            ns,
            "--type=merge",
            "-p",
            '{"spec":{"selector":{"matchLabels":{"app":"quarantined"}}}}',
        )
        k8s.wait_until(
            lambda: {e.metadata.address for e in store.pod_list()} == {victim["status"]["podIP"]},
            timeout=60,
            what="a selector change to resync the pool",
        )

        k8s.kubectl("delete", "inferencepool", "pool", "-n", ns)
        k8s.wait_until(lambda: not store.pool_has_synced(), timeout=60, what="the pool to clear")
    finally:
        stop.set()
