# Day 7 — Measuring it against llm-d

~4 hours · laptop (kind)

> **Goal:** write a streaming load generator, then run one experiment: the
> same simulators behind a plain Service, behind your EPP, and behind
> llm-d's own EPP. Measure TTFT, time per output token, throughput and
> cache hits, and claim only what the measurement supports.

## What you build

| File | What to implement |
|---|---|
| `src/lean_llmd/loadgen.py` | `shared_prefix_workload`, `send_one`, `run`, `percentile`, `summarize`, `main` |

## Part A — the load generator

### Step 1. `shared_prefix_workload`

`groups` tenants, each with its own long system prompt, each sending
`requests_per_group` requests that append a short question. Shuffle them so
groups interleave, the way real traffic does. Seeded, so every router sees
the same requests.

### Step 2. `send_one`

Ask for `stream: true` and `stream_options: {include_usage: true}`, plus
`ignore_eos: true` (a vLLM extension) so every request generates exactly
`max_tokens` tokens. Otherwise the simulator stops at random lengths, and
two runs differ in more than their routing. Read server-sent events:
`data: {...}` lines ending with `data: [DONE]`.

- **TTFT**: from just before sending to the first chunk whose text is not
  empty. The first chunk can be empty; skip it.
- **Usage**: the last chunk carries `prompt_tokens`, `completion_tokens` and
  `prompt_tokens_details.cached_tokens`. That last number comes from the
  server's own cache, so it is your ground truth for hit rate.
- Never raise. A failed request is a record with `ok=False`, so that
  failures are counted, not lost.

### Step 3. `run`, `percentile`, `summarize`

A semaphore caps requests in flight (a *closed* loop: a new request starts
only when one finishes). TPOT is `(e2e - ttft) / (output_tokens - 1)`.
Percentiles interpolate linearly between ranks, as numpy does.

> **Trap:** a closed loop at concurrency 8 measures something different
> from "8 requests per second". When the system slows down, a closed loop
> slows down with it and the queue never grows. Real traffic does not wait
> for you. Note which kind you ran whenever you quote a number.

## Part B — the experiment

```bash
uv run lean-llmd test --day 7       # includes: your client's numbers match the simulator's
uv run lean-llmd image              # the benchmark runs your EPP and your load generator
uv run lean-llmd bench
```

`bench` deploys into namespace `lean-llmd-bench`: three simulators with a
KV cache too small to hold every tenant's prefix, your EPP, and llm-d's
router from its Helm chart (`setup/llm-d-router.values.yaml`). For each
router it restarts the simulators and the router (cold caches, empty
index), runs your load generator in a Pod, and reads the simulators'
counters. Output looks like:

```
router    TTFT p50  TTFT p99    TPOT  req/s hit (client) hit (sims)  served per replica
service      ...
epp          ...
llm-d        ...
```

and `results/bench.json` holds everything, including the pins and the
simulator config. `bench --ref` runs the reference instead of your code.

### What you may claim

Every latency here comes from the simulator's latency model:
`TTFT = 20 ms + 2 ms × uncached tokens`, 10 ms per output token, more under
load. So you may say "on a simulated model server with this latency model,
prefix-aware routing raised the cache hit rate from X to Y and throughput
from A to B". You may **not** say "llm-d makes vLLM N× faster". The hit
rate and the spread across replicas are real properties of the routing; the
milliseconds are only as real as the model.

### Experiments

Write down your prediction before each run.

1. **The baseline.** Run `bench` as is. Is your EPP within noise of
   llm-d's? Which columns differ, and why?
2. **Turn off the prefix scorer.** Redeploy your EPP with
   `--prefix-weight 0` (pass `extra_args` to `epp_objects`, or
   `kubectl edit deploy/epp -n lean-llmd-bench`) and run
   `bench --routers epp`. Which numbers move?
3. **Replicas.** `bench --replicas 2` and `--replicas 4`. Does the gap
   between Service and EPP grow or shrink with more replicas? Why?
4. **Load.** `--concurrency 2` and `--concurrency 24`. Watch the TTFT p99
   and the spread across replicas. When does queue depth start to matter
   more than cache affinity?
5. **A rollout under load** (your plan's week-4 exercise). Start a long run
   (`--requests-per-group 100`). In another terminal:
   `kubectl rollout restart deploy/sim -n lean-llmd-bench`. Count errors.
   Then give the simulator container
   `lifecycle: {preStop: {sleep: {seconds: 5}}}`, set
   `terminationGracePeriodSeconds` to 15, and repeat. What changed, and
   which component was still sending to the terminating Pods?

## Deliverable

`results/bench.json`, plus one page in your own words:

- the request-path diagram from day 6, with every Kubernetes object named;
- the table from experiment 1 and one sentence per row;
- the claim you can defend, worded as above;
- what you would measure next on real GPUs, and why the simulator cannot
  answer it.

## Map it to llm-d

```bash
uv run lean-llmd map --day 7
```

1. **`guides/optimized-baseline/README.md`.** The same experiment at
   production scale. Compare its scorer set with the default you used.
2. **The simulator's `docs/latency-simulation.md`.** Find
   `time-factor-under-load`: your runs left it at 1. What would it change?
3. **The simulator's `docs/metrics.md`.** Which server-side histogram
   should your client's TTFT p50 agree with, and why can it never be lower?

## Check yourself

1. Your Service row's "served per replica": with 8 keep-alive connections
   and 3 replicas, what spreads are possible? Which did you get?
2. What hit rate did each router reach (client and simulator columns)?
   Do the two columns agree? If not, which one do you believe?
3. In experiment 3, give the Service-to-EPP throughput ratio at 2, 3 and 4
   replicas.
4. In experiment 5, how many requests failed with and without `preStop`?

## What next

The core ends here, with an EPP that routes on load and prefix affinity.
The extension days build on it, each adding one `--feature` to the EPP:

- [Day 8](day08.md): precise prefix routing from the simulators' KV events.
- [Day 9](day09.md): prefill/decode disaggregation, with a sidecar you write.
- [Day 10](day10.md): flow control: priorities from a CRD, fairness, 429s.
- [Day 11](day11.md): gateway mode with agentgateway, TLS, full-duplex ext-proc.
- [Day 12](day12.md): EPP metrics, Prometheus, and KEDA scaling the simulators.
- [Day 13](day13.md): disruption budgets, zero-downtime rollouts, failing open.
- [Day 14](day14.md) (optional, needs a GPU): real vLLM in place of the simulator.
