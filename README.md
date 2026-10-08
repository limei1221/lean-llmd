# lean-llmd

A 14-day course that teaches Kubernetes and [llm-d](https://github.com/llm-d/llm-d)
by making you rebuild llm-d's request path in Python: a 7-day core that ends
with a working endpoint picker benchmarked against llm-d's, six extension
days, and an optional GPU day.

You implement one piece a day in `src/lean_llmd/`. A test suite checks it
against something independent: the Kubernetes API server and its
controllers, Envoy, `prometheus_client`, or a live vLLM simulator. Passing
unlocks comparison with the reference and a map to the same idea in llm-d's
Go code.

Everything except the optional GPU day runs on a laptop. The model servers are
[`llm-d-inference-sim`](https://github.com/llm-d/llm-d-inference-sim)
replicas on a [kind](https://kind.sigs.k8s.io/) cluster. They speak the
OpenAI API, export vLLM's metrics, and keep a simulated KV cache, so
prefix-cache hits shorten TTFT just as they would on a GPU.

## The goal

Know what happens to an inference request between the client and the
model server on Kubernetes, well enough to build the router yourself: how
replicas are discovered, what the router reads from them, how it scores
them, how it talks to the proxy, and how to measure whether any of it
helped.

## What it covers

| Day | You build | Kubernetes you learn |
|---:|---|---|
| 1 | The model server as a Deployment and a Service | Pods, ReplicaSets, labels, the three probes, EndpointSlices, rollouts |
| 2 | A `/metrics` scraper: queue, running, KV-cache use | The API server's Pod proxy, port-forward, load from inside the cluster |
| 3 | Endpoint discovery from an InferencePool | CRDs, list and watch, resourceVersion, informers, reconcilers |
| 4 | The scheduler: filters, weighted scorers, pickers | kube-scheduler's framework, taints, tolerations, node affinity |
| 5 | Prefix-cache-aware routing | How kube-proxy balances a Service, and why it cannot do this |
| 6 | **The endpoint picker behind Envoy** | Images, multi-container Pods, ConfigMaps, ServiceAccounts, RBAC, gRPC probes |
| 7 | **A load generator, and a benchmark against llm-d** | Jobs, rolling restarts under load, graceful termination |
| 8 | Precise prefix routing from ZMQ KV events | Patching Deployments, Pod-to-Pod sockets |
| 9 | Prefill/decode disaggregation and its sidecar | Multi-container Pods, role labels, independent scaling |
| 10 | Flow control: priority bands, fairness, saturation | Writing objects for a CRD, additive RBAC |
| 11 | Gateway mode with agentgateway | Gateway API (GatewayClass, Gateway, HTTPRoute), TLS |
| 12 | EPP metrics, Prometheus, KEDA | Service discovery for scraping, HPA via KEDA |
| 13 | Failure modes | PodDisruptionBudgets, eviction, rollout strategy, preStop |
| 14 | *Optional:* real vLLM on a GPU | Device plugins, extended resources, taints, PVCs, Secrets |

Per-day detail: [`course/README.md`](course/README.md).

Upstream is pinned: llm-d `v0.10.0`, llm-d-router `v0.11.0`,
llm-d-inference-sim `v0.12.0`, Gateway API Inference Extension `v1.5.0`,
Gateway API `v1.5.1`, agentgateway `v1.6.0`, KEDA `2.20.2`.

## What you produce

An endpoint picker you wrote, routing real traffic on Kubernetes, and one
table measured on the same simulated replicas:

| Router | What it is |
|---|---|
| `service` | a plain Kubernetes Service |
| `epp` | your EPP behind Envoy |
| `llm-d` | llm-d's own EPP and Envoy, from its Helm chart |

On the default workload the reference EPP gets 85% prefix-cache hits
against the Service's 67%, and about 1.5× its throughput, level with
llm-d's EPP (84%). The latencies come from the simulator's latency model;
the hit rates and load spread are real properties of the routing.

## How to use it

Each day takes about four hours. **Do them in order**: every day builds on
the ones before. Days 11 and 12 need add-ons:
`uv run lean-llmd addons gateway monitoring`.

Requires Docker (give it 8 GB of memory or more), kind, kubectl, helm and
[uv](https://docs.astral.sh/uv/). See [`setup/README.md`](setup/README.md).

### Set up

```bash
uv sync
uv run lean-llmd cluster up        # kind cluster + InferencePool CRD
uv run lean-llmd list -v
uv run lean-llmd test --day 1 --ref
```

### Daily loop

1. Read the day's write-up, starting with [`course/day01.md`](course/day01.md).
2. Implement the stubs in `src/lean_llmd/`; their docstrings are the spec.
3. Run `uv run lean-llmd test --day N` until it passes. Add pytest filters
   after `--`, for example `-- -k probe`.
4. Work through the day's cluster lab.
5. Run `uv run lean-llmd diff --day N`, then `uv run lean-llmd map --day N`
   to compare with the reference and find the same idea in llm-d.
6. Answer the write-up's closing questions before moving on.

`diff` refuses to show the reference until your tests pass. If you suspect
the tests, run the same day with `--ref`.

From day 6 on, the cluster runs your code from an image:
`uv run lean-llmd image` rebuilds it from your working tree.

## Layout

```text
course/                 the write-up for each day
src/lean_llmd/          your implementation
src/lean_llmd_ref/      the reference implementation
src/lean_llmd_course/   the syllabus and the lean-llmd CLI
tests/                  per-day tests, run against either package via --impl
setup/                  kind config, llm-d's Helm values, setup notes
tools/make_stubs.py     regenerates stubs from the reference, keeping signatures
```

## Checks

```bash
uv run pytest tests --impl ref     # the reference passes every test
uv run ruff check . && uv run ruff format --check . && uv run mypy
```
