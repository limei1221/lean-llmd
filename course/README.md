# The syllabus

Fourteen days in three parts. The **core** (days 1–7) builds llm-d's request
path, starting at the model server and working out towards the client: the
server itself, what it reports, how it is found, how it is chosen, why the
choice depends on the prompt, how the proxy asks, and whether it all helped.
The **extensions** (days 8–13) each add one capability to that EPP as a
`--feature`. **Day 14** is optional and needs a GPU.

```bash
uv run lean-llmd list -v      # this list, with the files you write
uv run lean-llmd status       # which days currently pass
```

| Day | Title | You write | Checked against |
|---:|---|---|---|
| [1](day01.md) | The model server on Kubernetes | `manifests.py` | the API server; the simulator's probe endpoints; the EndpointSlice controller |
| [2](day02.md) | Reading the model server's metrics | `metrics.py` | `prometheus_client`'s parser; a saturated simulator |
| [3](day03.md) | Discovering endpoints from an InferencePool | `datastore.py`, `discovery.py` | the EndpointSlice controller, through scale, relabel and selector changes |
| [4](day04.md) | The scheduler: filter, score, pick | `scheduling.py`, `plugins.py` | hand-worked scores, including llm-d's documented example |
| [5](day05.md) | Prefix-cache-aware routing | `prefix.py` | llm-d's hash chain re-derived; three simulators' own caches |
| [6](day06.md) | The endpoint picker behind Envoy | `extproc.py`, `epp.py`, `deploy.py` | Envoy's protobufs over real gRPC; the API server's authorizer; real Envoy on kind |
| [7](day07.md) | Measuring it against llm-d | `loadgen.py` | the simulator's server-side histograms and counters; llm-d's own EPP |
| [8](day08.md) | Precise prefix routing from KV events | `kvevents.py`, `precise.py` | the event format as documented; a simulator's cache through stores and evictions |
| [9](day09.md) | Prefill/decode disaggregation | `disagg.py`, `pd_sidecar.py` | two simulators' counters and TTFT: was prefill really remote? |
| [10](day10.md) | Flow control | `flowcontrol.py` | strict priority, round-robin and FCFS by hand; premium overtaking batch on kind |
| [11](day11.md) | Gateway mode | `gateway.py` | gRPC's TLS stack; Envoy's protobufs; agentgateway routing by your decision |
| [12](day12.md) | Observability and autoscaling | `observability.py` | `prometheus_client`'s parser; a real Prometheus; KEDA scaling the simulators |
| [13](day13.md) | Failure modes | `resilience.py` | Envoy's validator; the eviction API; zero errors through a rollout |
| [14](day14.md) | Optional: real vLLM on a GPU | `gpu.py` | the API server's dry run; the rest is a lab on a GPU machine |

Each day's map sends you into llm-d's Go code. You only read it; start
with [`reading-go.md`](reading-go.md).

## Rules the course keeps

- **The oracle is never the reference.** Every test checks your code
  against something independent; the same tests prove the reference
  passes.
- **The reference is shaped like llm-d.** Same plugin names, same pipeline,
  same constants (block size floor, LRU capacity, header names), so the
  map to Go is about scale, not structure. What upstream does and the
  reference does not is noted in each day's map.
- **Claims match measurements.** Latencies on the simulator are the latency
  model's; day 7 says so in its output.
- **Later days never edit earlier code.** Each extension is a module the EPP
  loads by name (`--feature precise`), the way upstream registers plugins.
- **Upstream is pinned and verified.** `lean-llmd map --day N --verify`
  checks every cited path and symbol against local checkouts.

## Add-ons the later days need

```bash
uv run lean-llmd addons gateway      # day 11: agentgateway
uv run lean-llmd addons monitoring   # day 12: Prometheus and KEDA
```
