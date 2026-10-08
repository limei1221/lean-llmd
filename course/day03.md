# Day 3 — Discovering endpoints from an InferencePool

~4 hours · laptop (kind)

> **Goal:** keep an always-current list of the Pods your router may send
> to. You will write a tiny Kubernetes controller: list, watch, a work
> queue, and two reconcilers, the same pattern every operator uses.
> llm-d's EPP gets its endpoints exactly this way.

## What you build

| File | What to implement |
|---|---|
| `src/lean_llmd/datastore.py` | `is_pod_ready`, `labels_match`, `endpoint_name`, `Datastore` (`pool_set`, `pool_get`, `pool_has_synced`, `pool_labels_match`, `pod_update_or_add_if_not_exist`, `pod_delete`, `pod_list`, `clear`) and the small dataclass helpers |
| `src/lean_llmd/discovery.py` | `inference_pool`, `inference_pool_to_endpoint_pool`, `PodReconciler`, `InferencePoolReconciler`, `KubeClient`, `Controller` |

## The InferencePool

```yaml
apiVersion: inference.networking.k8s.io/v1
kind: InferencePool
spec:
  selector: {matchLabels: {app: sim}}   # which Pods serve this model
  targetPorts: [{number: 8000}]         # which ports on them
  endpointPickerRef: {name: epp, port: {number: 9002}}  # who decides
```

It looks like a Service, minus the virtual IP. A Service tells kube-proxy
to spread connections; an InferencePool tells a gateway to **ask an endpoint
picker** for every request. It is a CustomResourceDefinition (CRD) from the
Gateway API Inference Extension, which `cluster up` installed. Look at it:

```bash
kubectl get crd inferencepools.inference.networking.k8s.io
kubectl explain inferencepool.spec --recursive
```

## Part A — the datastore

### Step 1. `is_pod_ready`, `labels_match`, `endpoint_name`

- Ready means the `Ready` condition is `"True"` **and** there is no
  `metadata.deletionTimestamp`. A Pod being deleted can still report
  Ready for its grace period; it must not get new work.
- Do not use `status.phase == "Running"`. A Running Pod may not be ready
  (yesterday's slow model was Running for 30 s).
- An endpoint is one (Pod, target port) pair, named `<pod>-rank-<i>`. With
  vLLM data parallelism, one Pod serves several ranks on several ports.

### Step 2. `Datastore`

A lock, the current pool, and a dict of endpoints. The interesting method
is `pool_set`:

> **Trap:** when the pool's **selector** changes, no Pod changes, so no Pod
> event arrives. Pods that now match stay unknown, and Pods that no longer
> match stay in your list. `pool_set` must list the Pods itself and rebuild
> (`_pod_resync_all` in the reference). It must do the same on the very
> first pool, because Pod events that arrived before it were dropped.

Keep the `Endpoint` object when a Pod is updated. The scraper (day 6) has
written metrics onto it, and the prefix index (day 5) is keyed by its id.

## Part B — the controller

### Step 3. `KubeClient`

Three reads (`get_pod`, `list_pods`, `get_pool`), returning plain dicts in
the API's own JSON shape. `sanitize_for_serialization` turns the client's
objects into that shape.

Then `list_and_watch`, the core of every Kubernetes client:

1. **List**: get every object, plus the list's `resourceVersion`.
2. **Watch** from that version: the API server streams `ADDED`,
   `MODIFIED`, `DELETED` events for everything after it.
3. When the watch ends (timeouts are normal), resume from the last
   `resourceVersion` you saw.
4. When the server says **410 Gone**, your version is too old (etcd has
   compacted it). List again.

The list step is why you never miss an object that existed before you
started; the version is why you never miss a change between list and
watch. Upstream gets all this from client-go's *informer*, plus a local
cache of every object.

### Step 4. The reconcilers

A reconciler is given a **key**, not an event. It re-reads the object and
makes the datastore agree with what it finds. That makes it *level-
triggered*: it does not matter how many events you missed or merged, only
what is true now.

- `PodReconciler.reconcile`: no pool yet → nothing. Pod gone → delete.
  Otherwise delete it if it is unready or no longer selected, else upsert.
- `InferencePoolReconciler.reconcile`: pool gone or being deleted → clear.
  Otherwise `pool_set`.

### Step 5. `Controller`

Watch threads turn events into keys on a queue; one worker reconciles keys
one at a time. Two details:

- **De-duplicate** keys already queued. Ten updates to one Pod in a burst
  need one reconcile.
- **Filter Pod events by the old labels too.** If someone relabels a Pod
  out of the pool, its new labels no longer match. Filter on those alone
  and you drop the very event that should remove it.

> **Trap:** a reconcile that raises must not be lost. The reference
> re-queues it a second later; controller-runtime uses exponential backoff.

## Part C — on the cluster

```bash
uv run lean-llmd test --day 3
```

The kind test runs your controller from your laptop against the cluster
and checks it against the **EndpointSlice controller**, which computes
"ready Pods behind this selector" for every Service. It scales,
relabels, changes the pool's selector and deletes the pool, and expects
your datastore to keep up each time.

Watch the raw protocol yourself:

```bash
uv run lean-llmd render pool | kubectl apply -f -
RV=$(kubectl get --raw /api/v1/namespaces/lean-llmd/pods \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)["metadata"]["resourceVersion"])')
kubectl get --raw "/api/v1/namespaces/lean-llmd/pods?watch=1&resourceVersion=$RV" &
kubectl delete pod -n lean-llmd -l app=sim --wait=false   # in another terminal
```

Count the events per Pod: one `MODIFIED` for the deletion timestamp, more
as containers stop, and a final `DELETED`. Then run your controller
interactively and watch it track the cluster:

```bash
uv run python -c "
import threading, time
from lean_llmd.datastore import Datastore
from lean_llmd.discovery import Controller, KubeClient
ds = Datastore(); c = Controller(ds, KubeClient('kind-lean-llmd'), 'lean-llmd', 'pool')
c.run(threading.Event())
while True:
    print(sorted(e.metadata.host_port for e in ds.pod_list())); time.sleep(1)"
```

While it runs: `kubectl scale deploy/sim -n lean-llmd --replicas=5`, then
`kubectl label pod <one> -n lean-llmd app=away --overwrite`.

## Compare with the reference

```bash
uv run lean-llmd diff --day 3
```

## Map it to llm-d

```bash
uv run lean-llmd map --day 3
```

1. **`docs/architecture/core/inferencepool.md`.** The two consumers of an
   InferencePool: the EPP (discovery) and the gateway controller (wiring
   ext-proc). Today was the first; day 6 is a manual version of the second.
2. **`controller/pod_reconciler.go`.** `SetupWithManager` holds the event
   filter. Find where it checks `oldPod` *and* `newPod`.
3. **`datastore/datastore.go`.** `PoolSet` and its comment listing the three
   reasons for a full resync. Which of the three did you meet today?
4. **`util/pod/pod.go`.** `IsPodReady`, ten lines.

## Check yourself

1. An InferencePool with two `targetPorts` selects three ready Pods. How
   many endpoints does the datastore hold, and what is the name of the
   last one for a Pod called `sim-x`?
2. In your raw watch, how many events did one deleted Pod produce?
3. The EPP's reconciler calls `get_pod` straight to the API server; upstream
   reads from the informer's cache. With 1,000 Pods in a pool restarting at
   once, roughly how many API requests does each approach make?
4. Find the line in `datastore.go` that names endpoints `-rank-`. What vLLM
   feature puts several endpoints in one Pod?
