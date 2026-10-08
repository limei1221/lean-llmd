# Day 10 — Flow control: priority, fairness, saturation

~4 hours · laptop (kind)

> **Goal:** when the pool is full, hold requests in the EPP, in policy-aware
> queues, instead of letting them pile up inside model servers where nobody
> can reorder them. Premium traffic first, tenants take turns, and a request
> that waits too long gets a clean 429.

## What you build

| File | What to implement |
|---|---|
| `src/lean_llmd/flowcontrol.py` | `InflightCounter`, `ConcurrencyDetector`, `UtilizationDetector`, `RoundRobinFairness`, `FlowController`, `ObjectiveStore`, `flow_key`, `FlowControlledDirector`, `TrackingServer`, `inference_objective`, `objective_rbac`, `install` |

## Why queue in the EPP

Once a request reaches a model server's queue it is stuck there: the EPP
cannot move it to a replica that frees up, cannot let a premium request
overtake it, and cannot stop one tenant from filling every queue. llm-d's
flow control keeps excess demand at the gateway, where policy can act.

```
request ─▶ FlowKey (fairness id, priority) ─▶ its queue
dispatcher: while saturation < 1:
    band   = highest priority with anything queued      (strict)
    flow   = round-robin among that band's flows        (fairness)
    request = oldest in that flow                       (FCFS)
    release it ─▶ scheduler ─▶ Envoy
```

## Part A — saturation

### Step 1. `InflightCounter`, `ConcurrencyDetector`

Day 6 left a gift: Envoy keeps a request's ext-proc stream open until the
response is done (you can measure it). So "routed, stream not closed" is
exactly "in flight". The concurrency detector is in-flight over capacity:
`load / (endpoints × max_concurrency)`.

### Step 2. `UtilizationDetector`

The alternative from scraped metrics: average of
`max(queue / queue_threshold, kv / kv_threshold)`, with a stale endpoint
scoring 1.0 (better to hold traffic than to send it blind).

> **Trap:** scraped metrics are 50 ms old and do not move when you release a
> request. Gate on them alone and a burst of 50 requests all pass before
> the next scrape notices. That is why the concurrency detector is
> upstream's default.

## Part B — the controller

### Step 3. `RoundRobinFairness`, `FlowController`

`dispatch_once` is the three tiers above. Two details matter:

- **Count before you release.** The dispatcher loops `while
  dispatch_once()`. If the in-flight count only rises when the released
  thread wakes up, the loop sees the same saturation every time and
  releases the whole queue. `test_a_burst_cannot_slip_through` catches it.
- **Expire.** A request older than its TTL is released with `expired=True`.

### Step 4. `FlowControlledDirector`, `TrackingServer`

The director wrapper blocks the request's gRPC thread until release, then
schedules. The servicer wrapper subtracts it from in-flight when the stream
closes. They meet through a thread-local, which works because gRPC runs one
stream's generator on one thread.

An expired request raises `RequestRejected(429)`: **429 means contended,
retry later**; 503 means there is no capacity at all. Clients and
autoscalers treat them differently.

### Step 5. Priorities from a CRD

Priority comes from an `InferenceObjective` (`llm-d.ai/v1alpha2`), named by
the `x-llm-d-inference-objective` header; the tenant from
`x-llm-d-inference-fairness-id`. `cluster up` installed the CRD. The EPP
needs a second Role to read it: `objective_rbac`. RBAC is additive, so day
6's Role stays as it was.

> **Trust boundary:** a client that can set these headers can make itself
> premium. In production a trusted gateway or auth filter strips and sets
> them.

## Run it

```bash
uv run lean-llmd test --day 10
uv run lean-llmd image
```

The kind test saturates two replicas with 16 batch requests, then sends 4
premium ones 0.3 s later; all premium requests must finish before the last
batch ones.

On the cluster:

```bash
uv run python -c "
import yaml
from lean_llmd import deploy, flowcontrol as fc
flags = ('--feature', 'flowcontrol', '--max-concurrency', '2')
objs = [*deploy.epp_objects('epp', 'lean-llmd', 'pool', extra_args=flags),
        *fc.objective_rbac('epp', 'lean-llmd'),
        fc.inference_objective('premium', 'lean-llmd', 'pool', 100),
        fc.inference_objective('batch', 'lean-llmd', 'pool', -10)]
print(yaml.safe_dump_all(objs))" | kubectl apply -f -
kubectl get inferenceobjectives -n lean-llmd
```

1. Send 30 concurrent requests with `x-llm-d-inference-objective: batch`
   and then 5 with `premium`. Plot completion times by class.
2. Two tenants, same priority: one sends 30 requests, the other 3. With
   fairness, when does the quiet tenant's last request finish? Without
   (`--max-concurrency 100`, so nothing ever queues)?
3. `--request-ttl 2` under the same load: count the 429s.
4. Compare `--saturation utilization` with the default under a burst.

## Compare, map

```bash
uv run lean-llmd diff --day 10
uv run lean-llmd map --day 10
```

1. **`flow-control.md`**: the three tiers and the trust boundary.
2. **`concurrency/detector.go`**: `Saturation` has a token mode too. Why
   would tokens be a better unit than requests?
3. **`utilization/README.md`**: the staleness policy.
4. **`admission.go`**: find the 429/503 rule.

## Check yourself

1. Two endpoints, `max_concurrency` 2, 3 requests in flight. Saturation?
   How many more will the dispatcher release?
2. Flows `a`, `b`, `c` in one band, the cursor after `b`, and `c` empty:
   which flow is next?
3. In the kind test, what were the premium and batch completion times on
   your run?
4. Why does `TrackingServer` subtract in a `finally`, and what would leak
   without it?
