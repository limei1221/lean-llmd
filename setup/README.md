# Setup

## Tools

```bash
brew install kind kubectl helm uv     # Docker Desktop (or OrbStack/colima) separately
```

Give Docker **8 GB of memory or more** (Docker Desktop → Settings →
Resources). Three kind nodes, a dozen simulator Pods, two EPPs and two
Envoys fit in that; the default 7.7 GB on some machines is tight once day 7
runs llm-d's router too.

## The cluster

```bash
uv sync
uv run lean-llmd cluster up      # creates kind cluster "lean-llmd" from setup/kind.yaml and
                                 # installs CRDs: InferencePool (GAIE v1.5.0),
                                 # InferenceObjective (llm-d-router v0.11.0), Gateway API v1.5.1
kubectl config current-context   # kind-lean-llmd
```

Nodes pull `ghcr.io/llm-d/llm-d-inference-sim:v0.12.0` themselves the first
time a day's tests run, so the first kind test is slow.

`uv run lean-llmd cluster down` deletes it all.

## Add-ons (days 11 and 12)

```bash
uv run lean-llmd addons gateway      # agentgateway v1.6.0 (GatewayClass agentgateway)
uv run lean-llmd addons monitoring   # Prometheus (namespace monitoring) + KEDA (namespace keda)
```

llm-d v0.10.0's docs pin agentgateway v1.1.0; its CRDs are rejected by
Kubernetes 1.37 (a CEL rule exceeds the cost budget), so the course uses
v1.6.0. Prometheus uses [`prometheus.values.yaml`](prometheus.values.yaml).

## The course image (day 6 on)

```bash
uv run lean-llmd image           # docker build -t lean-llmd:dev . && kind load docker-image
```

The image holds both packages: the cluster runs `python -m lean_llmd.epp`
(yours) or `python -m lean_llmd_ref.epp`. **Rebuild after every change** you
want the cluster to see.

## Upstream checkouts (for `map --verify`)

```bash
mkdir -p ~/workspace/llm-d-src && cd ~/workspace/llm-d-src
git clone --depth 1 --branch v0.10.0 https://github.com/llm-d/llm-d.git
git clone --depth 1 --branch v0.11.0 https://github.com/llm-d/llm-d-router.git
git clone --depth 1 --branch v0.12.0 https://github.com/llm-d/llm-d-inference-sim.git
export LLMD_SOURCE_DIR=~/workspace/llm-d-src
uv run lean-llmd map --day 4 --verify
```

## Test tiers

| Marker | Needs | Skipped when |
|---|---|---|
| (none) | Python | never |
| `docker` | a Docker daemon | `docker info` fails |
| `kind` | the `kind-lean-llmd` context with the CRD | the CRD is not found |

`lean-llmd status` skips the kind tier to stay quick; `lean-llmd test --day N`
runs everything.

## llm-d's router (day 7)

`lean-llmd bench` installs `oci://ghcr.io/llm-d/charts/llm-d-router-standalone`
at the pinned version with [`llm-d-router.values.yaml`](llm-d-router.values.yaml):
the course's simulators as model servers, laptop-sized resources.
