# Day 2 — Reading the model server's metrics

~4 hours · laptop (Docker + kind)

> **Goal:** turn a model server's `/metrics` page into the three numbers a
> router balances on: requests waiting, requests running, and KV-cache use.
> The format is simple; the traps are in the corners (label values with
> commas, info metrics, several series under one name).

## What you build

| File | What to implement |
|---|---|
| `src/lean_llmd/metrics.py` | `Metrics.clone`, `parse_text`, `parse_spec`, `get_latest_metric`, `extract`, `scrape` |

## Why these three numbers

A Service picks a Pod at random. An LLM router can do better because a
model server tells you how busy it is:

| Metric | Type | What it says |
|---|---|---|
| `vllm:num_requests_waiting` | gauge | requests queued because the batch is full (`max_num_seqs`) or the KV cache is |
| `vllm:num_requests_running` | gauge | requests in the current batch |
| `vllm:kv_cache_usage_perc` | gauge | fraction of KV-cache blocks in use, 0..1 |
| `vllm:cache_config_info` | info | value `1`; the numbers live in its labels (`block_size`, `num_gpu_blocks`) |

`num_requests_waiting` is the strongest signal: a queued request has not
started, so its TTFT keeps growing until a slot frees.

## Part A — the text format

```
# HELP vllm:num_requests_waiting Number of requests waiting to be processed.
# TYPE vllm:num_requests_waiting gauge
vllm:num_requests_waiting{model_name="mini/sim"} 4
vllm:time_to_first_token_seconds_bucket{model_name="mini/sim",le="0.1"} 12
http_requests_total{path="/a,b}c"} 7 1712345678901
```

One sample per line: a name, optional labels, a value, an optional
millisecond timestamp. A histogram `x` appears as several names (`x_bucket`,
`x_sum`, `x_count`); key them separately.

### Step 1. `parse_text`

Walk each line by hand rather than splitting on `,` or `}`: a label value
may contain both, plus the escapes `\\`, `\"` and `\n`. The tests compare
you with `prometheus_client`'s parser on a deliberately nasty input.

### Step 2. `parse_spec`

llm-d lets operators name the series to read, as `name` or
`name{label=value}`. Bare values get quotes added before parsing, so
`vllm:lora_requests_info{max_lora=4}` works. Port llm-d's three regular
expressions or write your own; the tests check behaviour, not method.

### Step 3. `get_latest_metric`

Several series can share a name (one per label combination). A spec's
labels must all be present with equal values; extra labels on the sample
are fine. Among matches, the newest timestamp wins. With no timestamps at
all, the **first** match wins (llm-d compares `ts > recent`, starting from
`recent = -1`).

### Step 4. `extract`

Build a **new** `Metrics` from a clone of the current one, and leave the
original alone. On day 6 the scheduler reads an endpoint's metrics on one
thread while a scraper replaces them on another. Swapping a whole new
object is safe; editing the old one in place lets a reader see half an
update.

Each field is read independently. One missing series should not stop the
others from updating, and it should leave its own field unchanged rather
than resetting it to zero.

> **Trap:** `cache_config_info`'s value is always 1. Its information is in
> the labels, as strings.

### Step 5. `scrape`

One GET, then `parse_text`. Accept an `httpx.Client` so the caller can
reuse connections; on day 6 you scrape every endpoint every 50 ms.

## Part B — on the cluster

```bash
uv run lean-llmd test --day 2
```

The Docker test saturates a simulator with `max_num_seqs=2` and checks
your numbers against the limit you set.

Now read live metrics without exposing anything. The API server can proxy
to any Pod port:

```bash
POD=$(kubectl get pods -n lean-llmd -l app=sim -o jsonpath='{.items[0].metadata.name}')
kubectl get --raw "/api/v1/namespaces/lean-llmd/pods/$POD:8000/proxy/metrics" | grep '^vllm:'
```

1. Put load on the Service from inside the cluster and watch one Pod's
   queue:

   ```bash
   kubectl run load -n lean-llmd --rm -i --restart=Never --image=curlimages/curl -- sh -c \
     'for i in $(seq 40); do curl -s -o /dev/null http://sim/v1/completions \
        -H "content-type: application/json" \
        -d "{\"model\":\"mini/sim\",\"prompt\":\"hi\",\"max_tokens\":200}" & done; wait'
   ```

   While it runs, poll the proxy URL every second
   (`watch -n1 "kubectl get --raw ... | grep -E 'waiting|running|usage'"`).
2. Compare that with `kubectl port-forward pod/$POD 8000:8000` followed by
   `curl localhost:8000/metrics`. Both reach the Pod. Which one goes through
   the API server, and which one would you trust under heavy load?

## Compare with the reference

```bash
uv run lean-llmd diff --day 2
```

## Map it to llm-d

```bash
uv run lean-llmd map --day 2
```

This is the first day you read llm-d's Go. If Go is new to you, read
[`reading-go.md`](reading-go.md) first; it takes ten minutes.

1. **`framework/interface/datalayer/metrics.go`.** The `Metrics` struct.
   You dropped the LoRA fields. Find `KvCacheMaxTokenCapacity` and
   `CachePrefixMatchUnit`, and note which plugin reads them (`grep -rn`).
2. **`extractor/metrics/spec.go`.** Your `parse_spec` and
   `get_latest_metric`. Upstream parses with
   `github.com/prometheus/common/expfmt`, the same parser Prometheus uses.
3. **`extractor/metrics/extractor.go`.** Find `clone := current.Clone()` and
   `ep.UpdateMetrics(clone)`. Then find `getEngineTypeFromEndpoint`: that is
   where yesterday's `llm-d.ai/engine-type` label is read.
4. **`extractor/metrics/factories.go`.** The vLLM names next to SGLang's.

## Check yourself

1. A simulator with `max_num_seqs=4` receives 10 long requests at once.
   What do `num_requests_running` and `num_requests_waiting` read a moment
   later?
2. From your step-1 load test, write down the peak
   `vllm:kv_cache_usage_perc` you saw, and the `num_gpu_blocks` label.
   About how many blocks were in use at the peak?
3. Why does `extract` take `now` as an argument instead of calling
   `time.time()`? (Name the test that would get harder otherwise.)
4. In `extractor.go`, what does the extractor do when a Pod carries no
   engine-type label at all? Give the constant's name.
