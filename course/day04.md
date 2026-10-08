# Day 4 — The scheduler: filter, score, pick

~4 hours · laptop (kind for the lab)

> **Goal:** build llm-d's scheduling framework and its two load scorers.
> Every routing decision llm-d makes, from "least busy" to prefill/decode
> disaggregation, is this one pipeline with different plugins in it. Then
> meet the other scheduler with the same shape: Kubernetes' own.

## What you build

| File | What to implement |
|---|---|
| `src/lean_llmd/scheduling.py` | `SchedulingResult.primary`, `enforce_score_range`, `SchedulerProfile` (`__init__`, `run`), `Scheduler` (`__init__`, `schedule`) |
| `src/lean_llmd/plugins.py` | `QueueScorer`, `KVCacheUtilizationScorer`, `LabelSelectorFilter`, `MaxScorePicker`, `RandomPicker`, `SingleProfileHandler` |

The Protocols in `scheduling.py` (`Filter`, `Scorer`, `Picker`,
`ProfileHandler`) are given. They are llm-d's Go interfaces, one for one.

## Part A — the pipeline

```
Scheduler.schedule(request, candidates)
  loop:
    profiles = handler.pick(request, all_profiles, results_so_far)
    if none: break
    for each picked profile:  filters -> weighted scorers -> picker
  return handler.process_results(request, results)
```

### Step 1. `SchedulerProfile.run`

- **Filters** chain: each sees the previous one's survivors. Stop at the
  first that empties the list and raise `NoEndpointsError` (the EPP
  answers 503).
- **Scorers** return a score in [0, 1] per endpoint. Clamp each with
  `enforce_score_range`, multiply by the scorer's weight, sum. An endpoint
  a scorer forgot gets 0 from that scorer.
- The **picker** sees `ScoredEndpoint`s in filtered order.

llm-d's scheduling doc has the arithmetic: scorer A (weight 2) gives 0.8,
scorer B (weight 1) gives 0.5, total 2.1. The tests use that example.

> **Trap:** skipping the clamp. A scorer that returns 9.0 by mistake
> silently outvotes every other scorer combined.

### Step 2. `Scheduler.schedule`

The loop matters as soon as there is more than one profile. A P/D handler
first asks for "decode", looks at the result, and only then decides whether
"prefill" is needed. The last test builds such a handler. A profile that
raises gets a `None` result; if processing then fails, raise the profile's
error, which is the real cause.

## Part B — the plugins

### Step 3. Scorers

- `QueueScorer`: min-max normalise `waiting_queue_size`. Shortest queue
  scores 1, longest 0, **all equal scores 1** (no division by zero, and no
  endpoint is penalised when there is nothing to choose between).
- `KVCacheUtilizationScorer`: `1 - kv_cache_usage_percent`.

These two are llm-d's defaults, together with the prefix scorer you write
tomorrow. Note what is missing: nothing scores *running* requests or
predicted latency by default. You will see why queue depth dominates on
day 7.

### Step 4. Pickers

`MaxScorePicker` sorts by score, highest first, keeping input order among
equals. Then it rotates each run of equal scores by a call counter.

> **Trap:** without rotation, ties always resolve to the first endpoint in
> the datastore. Under light load every queue is 0 and every score is 1, so
> one replica gets every request while the others idle. Go hides this
> because map iteration order is random; Python dicts keep insertion order,
> so you must make the choice yourself.

### Step 5. `SingleProfileHandler`

Run every profile on the first call, none after. `process_results` wraps
the single result and names it primary.

## Part C — Kubernetes' scheduler does the same thing

kube-scheduler places Pods on nodes with the same shape: **Filter** plugins
drop nodes that cannot run the Pod (not enough CPU, a taint, a node
selector), **Score** plugins rank the rest, scores are normalised and
weighted, and the best node wins. Make it fail on purpose:

```bash
kubectl label node lean-llmd-worker2 accelerator=fake-gpu
kubectl taint node lean-llmd-worker2 dedicated=inference:NoSchedule
```

1. Add `nodeSelector: {accelerator: fake-gpu}` to the sim Pod template
   (`kubectl edit deploy/sim -n lean-llmd`, or patch your render). The new
   Pods stay `Pending`. Read `kubectl describe pod` → Events. Which filter
   rejected each node?
2. Add a matching toleration:
   `tolerations: [{key: dedicated, value: inference, effect: NoSchedule}]`.
   Where do the Pods land now?
3. Ask for more than exists: set the container's CPU request to `"64"`.
   Read the event again.
4. Replace the nodeSelector with a *preferred* node affinity
   (`preferredDuringSchedulingIgnoredDuringExecution`, weight 50). Scale to
   6 replicas. That is a Score plugin with a weight, not a Filter.

Undo with `kubectl taint node lean-llmd-worker2 dedicated-` and
`kubectl label node lean-llmd-worker2 accelerator-`.

On a real GPU cluster the filter that matters is
`resources.limits: {nvidia.com/gpu: 1}`. The NVIDIA device plugin
advertises `nvidia.com/gpu` in each node's `status.allocatable`, and the
scheduler filters on it exactly like CPU.

## Run, compare, map

```bash
uv run lean-llmd test --day 4
uv run lean-llmd diff --day 4
uv run lean-llmd map --day 4
```

Read in this order:

1. **`framework/interface/scheduling/plugins.go`.** The four interfaces.
   Find `ScorerCategory`; you did not implement it. What is it for?
2. **`scheduling/scheduler.go`** and **`scheduler_profile.go`.** Strip the
   tracing and metrics and they are your two methods.
3. **`picker/maxscore/picker.go`** and `picker/common.go`'s
   `RotateScoredEndpoints`.
4. **`scorer/queuedepth/queue.go`.** Find the comment on `Consumes()`: a
   scorer declares which data it reads, and the framework checks it.

## Check yourself

1. Three endpoints have queues 2, 2 and 6, and KV use 0.5, 0.1 and 0.0.
   With both scorers at weight 1, what is each total, and who wins?
2. Twelve requests arrive one at a time; every queue is 0 and every KV use
   is 0 throughout. How many does each of three endpoints get with your
   `MaxScorePicker`? Without the rotation?
3. In Part C step 1, quote the scheduler's event message.
4. What weight would make the KV scorer irrelevant in question 1, and what
   weight would make it decide?
