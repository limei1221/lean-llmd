# Day 12 — Observability and autoscaling

~4 hours · laptop (kind)

> **Goal:** export the EPP's view of demand as Prometheus metrics, and close
> the loop: Prometheus scrapes your EPP, a KEDA `ScaledObject` turns a
> PromQL query into an HPA, and the simulators scale with load. This is
> llm-d's current autoscaling path; the older variant autoscaler (WVA) is
> deprecated.

## What you build

| File | What to implement |
|---|---|
| `src/lean_llmd/observability.py` | `escape_label_value`, `Registry`, `new_epp_registry`, `refresh_pool_metrics`, `is_routing_response`, `CountingServer`, `MetricsServer`, `scrape_annotations`, `scaled_object`, `install` |

## Before you start

```bash
uv run lean-llmd addons monitoring    # Prometheus (namespace monitoring) + KEDA
```

## Why scale on EPP metrics

GPU utilization sits near 100% during batching whether a server is
comfortable or drowning, so it cannot drive scaling. The EPP sees demand
directly: requests running, requests queued in flow control, pool
saturation. They rise *before* latency does.

## Part A — exporting

### Step 1. `Registry`

Day 2 in reverse. Write the text format you parsed: `# HELP`, `# TYPE`, one
line per label set, label values escaped. A counter's name ends in `_total`,
and the `# TYPE` line uses that full name (that is the classic format,
and what `prometheus_client`'s parser expects).

### Step 2. The series

| Series | Type | Labels |
|---|---|---|
| `llm_d_epp_request_running` | gauge | `model_name` |
| `llm_d_epp_request_total` | counter | `model_name` |
| `llm_d_epp_ready_endpoints` | gauge | `name` (pool) |
| `llm_d_epp_average_queue_size`, `..._average_kv_cache_utilization` | gauge | `name` |
| `llm_d_epp_flow_control_queue_size`, `..._pool_saturation` | gauge | `name` (with day 10) |

Running requests come from `CountingServer`, a servicer wrapper like day
10's: +1 at the routing answer, −1 when the stream closes. It reads the
model name from the request body on the way past.

### Step 3. `MetricsServer`, `scrape_annotations`

`GET /metrics` on port 9090. The Prometheus chart's `kubernetes-pods` job
scrapes any Pod annotated `prometheus.io/scrape: "true"` on the annotated
port; that is service discovery through the Kubernetes API, with the
ClusterRole the chart installed.

## Part B — scaling

### Step 4. `scaled_object`

```
Prometheus ──query──▶ KEDA ──external metric──▶ HPA (KEDA owns it) ──▶ Deployment/sim
```

One `prometheus` trigger, `metricType: AverageValue`: the HPA aims for
`threshold` per replica, so `sum(running) = 8` with threshold 2 asks for 4
replicas. You write one `ScaledObject`, never a second HPA: two HPAs on one
Deployment fight.

The stabilization windows decide how fast it reacts. llm-d uses 300 s
because a real model takes minutes to load: scaling on demand that a
still-loading replica will soon absorb overshoots. Simulators start in a
second, so the test uses 5 s.

## Run it

```bash
uv run lean-llmd test --day 12
uv run lean-llmd image
```

The kind test is the whole loop: your metrics must appear in a real
Prometheus, and under load KEDA must scale the simulators past one replica.

On the cluster:

```bash
uv run python -c "
import yaml
from lean_llmd import deploy, observability as ob
flags = ('--feature', 'flowcontrol', '--feature', 'observability')
epp = ob.scrape_annotations(deploy.epp_deployment('epp', 'lean-llmd', 'pool', extra_args=flags))
so = ob.scaled_object('sim', 'lean-llmd', 'sim', 'http://prometheus-server.monitoring.svc',
                      'sum(llm_d_epp_request_running{namespace=\"lean-llmd\"})', 2)
print(yaml.safe_dump_all([epp, so]))" | kubectl apply -f -
kubectl port-forward -n monitoring svc/prometheus-server 9091:80   # http://localhost:9091
```

1. In Prometheus, graph `llm_d_epp_request_running` and
   `llm_d_epp_ready_endpoints` while day 7's load generator runs.
2. `kubectl get hpa -n lean-llmd -w` while load ramps up and down. Note the
   delays at each hop: scrape interval, KEDA polling, HPA stabilization,
   Pod startup.
3. Scale on `llm_d_epp_flow_control_queue_size` instead (with day 10 on and
   a low `--max-concurrency`). Which signal leads, and which lags?
4. Annotate the simulators too and graph `vllm:num_requests_waiting`.
   Under flow control, why does it stay near zero?

## Compare, map

```bash
uv run lean-llmd diff --day 12
uv run lean-llmd map --day 12
```

1. **`keda-epp.md`**: the pipeline and the dual-metric strategy.
2. **llm-d's `scaledobject.yaml`**: compare thresholds and windows.
3. **`llm_d_router_metrics.go`**: upstream's names and labels.
4. **`autoscaling/README.md`**: why WVA was deprecated.

## Check yourself

1. `sum(running) = 9`, threshold 2, 2 replicas, max 4. How many replicas
   does the HPA ask for?
2. From experiment 2, how many seconds passed between load starting and the
   third replica becoming Ready? Split it across the hops.
3. Why must `llm_d_epp_request_running` go down in a `finally`?
4. What does the `kubernetes-pods` job need from RBAC to discover your Pod?
   Find the ClusterRole the chart created.
