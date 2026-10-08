# Day 13 — Failure modes

~4 hours · laptop (Docker + kind)

> **Goal:** stop losing requests to operations rather than to models. A
> node drain takes too many replicas at once; a rollout kills replicas
> mid-request; the endpoint picker itself goes down. Each has a precise
> Kubernetes or Envoy answer, and you will watch each one work.

## What you build

| File | What to implement |
|---|---|
| `src/lean_llmd/resilience.py` | `pod_disruption_budget`, `zero_downtime`, `fail_open` |

## Part A — voluntary disruptions

### Step 1. `pod_disruption_budget`

A PodDisruptionBudget limits **voluntary** disruptions: `kubectl drain`, a
cluster autoscaler removing a node, the eviction API. It does not stop a
node from dying. With 3 replicas and `minAvailable: 2`, the API server lets
one eviction through and refuses the next with **429** until a replacement
is Ready. The test calls the eviction API directly and expects exactly that.

```bash
kubectl drain lean-llmd-worker --ignore-daemonsets --delete-emptydir-data
```

Try it with and without a PDB while load runs. Then
`kubectl uncordon lean-llmd-worker`.

> For GPU model servers this matters more: a replica that is evicted takes
> minutes to come back, so "how many may be down at once" is a capacity
> decision, not a default.

## Part B — rollouts

### Step 2. `zero_downtime`

What happens when a Pod is deleted, in order:

1. The API server sets `deletionTimestamp`. In parallel: the EndpointSlice
   controller drops the Pod, kube-proxy and **your EPP's watch** notice,
   and the kubelet runs the `preStop` hook.
2. After `preStop`, the container gets SIGTERM; after
   `terminationGracePeriodSeconds`, SIGKILL.

The race: requests routed in the instant before the EPP notices still
arrive. A `preStop` sleep keeps the server serving through that window.
`maxUnavailable: 0, maxSurge: 1` makes the rollout add before it removes.

The kind test runs six request loops for 40 s and restarts the Deployment
in the middle. It must drop **nothing**. Try it without `zero_downtime` too
(edit the test or do it by hand) and count.

## Part C — the picker goes down

### Step 3. `fail_open`

With `failure_mode_allow: false` (day 6), a dead EPP means every request
fails. Failing open alone does not help either: the request continues
without a destination header, and `ORIGINAL_DST` has nowhere to send it.

The fix is two routes: requests *with* the header go to
`original_destination_cluster`; everything else goes to a `fallback`
cluster pointing at the plain Service. This is where day 6's
`clear_route_cache` finally matters: Envoy picks a route before the EPP
answers, so only a cleared cache lets the header select the first route.

The Docker test runs Envoy's own validator on both configs; the kind test
replaces the picker with a process that never listens, and expects 200
from the fail-open Envoy and a 5xx from the fail-closed one.

> **Which to choose?** Fail-open keeps serving, but blind: no prefix
> affinity, no flow control, possibly overloading a replica. Fail-closed
> protects the servers and tells clients the truth. The InferencePool's
> `failureMode` asks the gateway for one or the other; day 11's test used
> `FailClose` precisely so a broken EPP could not hide.

## Run it

```bash
uv run lean-llmd test --day 13
uv run lean-llmd image
```

Then:

1. Run day 7's load generator and drain a node, with and without a PDB.
   Count failures.
2. Roll the simulators under load with and without `zero_downtime`.
3. Deploy the fail-open config, then
   `kubectl exec deploy/epp -c epp -- python -c "import os, signal; os.kill(1, signal.SIGTERM)"`
   under load. What happens to the prefix-cache hit rate during the
   restart?

## Compare, map

```bash
uv run lean-llmd diff --day 13
uv run lean-llmd map --day 13
```

1. **`docs/operations/graceful-shutdown.md`**: vLLM's own drain
   (`--shutdown-timeout`) and how it relates to the grace period.
2. **`inferencepool.md`**: `failureMode`.
3. **The standalone chart's `values.yaml`**: `failure_mode_allow` is a
   chart value, and Envoy's `--drain-strategy`.

## Check yourself

1. 4 replicas, `minAvailable: 3`, one Pod not Ready. How many evictions does
   the API server allow right now?
2. Quote the error message of the refused eviction.
3. In Part B, how many requests failed without `zero_downtime` on your run?
4. With `preStop` 5 s and the default 30 s grace period, how long can a
   request that started 1 s before deletion take and still finish?
