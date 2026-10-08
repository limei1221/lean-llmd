# Day 1 — The model server on Kubernetes

~4 hours · laptop (Docker + kind)

> **Goal:** run three simulated vLLM replicas on Kubernetes behind a
> Service, and understand the four objects involved: Namespace, Deployment,
> ReplicaSet and Service. You write them as Python dicts; the API server
> decides whether they are valid, and the controllers decide what happens.

## Before you start

```bash
uv sync
uv run lean-llmd cluster up        # a 3-node kind cluster + the InferencePool CRD
kubectl config current-context     # kind-lean-llmd
kubectl get nodes -o wide
```

kind runs each Kubernetes "node" as a Docker container (`docker ps` shows
them). Everything you do this week runs inside those three containers.

The model server is [`llm-d-inference-sim`](https://github.com/llm-d/llm-d-inference-sim):
a Go program that answers the OpenAI API and exports vLLM's `/metrics`. It
does no math. It sleeps according to a latency model and keeps a fake KV
cache, so prefix-cache hits make TTFT shorter, just like on a GPU:

```
TTFT = prefill_overhead + (prompt_tokens - cached_tokens) * prefill_per_token
```

## What you build

| File | What to implement |
|---|---|
| `src/lean_llmd/manifests.py` | `namespace`, `sim_labels`, `sim_args`, `sim_deployment`, `sim_service` |

The docstrings are the spec. This page explains why each piece exists.

## Part A — the objects

Every Kubernetes object has the same four top-level keys: `apiVersion`,
`kind`, `metadata` (name, namespace, labels) and `spec` (what you want).
The cluster adds `status` (what it has done). You never write `status`.

### Step 1. `namespace()` and `sim_labels()`

A Namespace scopes names and permissions. Labels are free-form key/value
pairs, and they are how objects find each other in Kubernetes: a Service
does not list its Pods, it *selects* them by label.

`sim_labels` returns two labels:

- `app: <name>`, which everything selects on.
- `llm-d.ai/engine-type: vllm`. llm-d's metrics extractor reads this label
  to know which metric names a Pod exports (vLLM, SGLang and TRT-LLM name
  them differently). You will read the code that uses it on day 2.

### Step 2. `sim_args()`

Translate `SimConfig` into the simulator's flags. Note `--enable-kvcache`:
without it there is no prefix cache, and day 5 has nothing to measure.

### Step 3. `sim_deployment()`

A Deployment says "keep N copies of this Pod template running". It does
not create Pods itself. It creates a **ReplicaSet** per template version,
and the ReplicaSet creates the Pods. That indirection is what makes rolling
updates possible: a new template gets a new ReplicaSet, the old one scales
down.

Three details to get right:

1. **`spec.selector.matchLabels` must select the template's labels.** The
   API server rejects a Deployment whose selector would not match its own
   Pods. The selector is also **immutable** after creation. Pick it once.
2. **`POD_IP` comes from the downward API**:
   `valueFrom.fieldRef.fieldPath: status.podIP`. The simulator puts its own
   IP in the topic of its KV-cache events, so a listener knows who sent
   them. A Pod cannot know its IP before it is scheduled; the kubelet fills
   the variable in at container start.
3. **Three probes, three questions.**

   | Probe | Question | Path here | On failure |
   |---|---|---|---|
   | startup | has the model finished loading? | `/health/ready` | keep waiting (up to `failureThreshold × periodSeconds`), then restart |
   | readiness | may I receive traffic now? | `/health/ready` | removed from Service endpoints; not restarted |
   | liveness | is the process wedged? | `/health` | container restarted |

   Liveness and readiness do not run until the startup probe has succeeded.

> **Trap:** pointing liveness at the readiness path. A server that is alive
> but busy, or still loading, gets killed and restarted, and has to load
> its weights all over again. On a real 100 GB model that is a restart loop.

### Step 4. `sim_service()`

A ClusterIP Service gets a stable virtual IP and DNS name (`sim`,
`sim.<namespace>.svc.cluster.local`). The EndpointSlice controller keeps a
list of the **ready** Pods it selects, and kube-proxy on every node programs
rules that send connections for the virtual IP to one of them.

Target the container port by **name** (`targetPort: http`), not by number.

## Part B — on the cluster

```bash
uv run lean-llmd test --day 1          # pure tests, then Docker, then kind
uv run lean-llmd render sim            # your objects as YAML
uv run lean-llmd render sim | kubectl apply -f -
```

Then work through these, writing down what you expected before each one.

1. **Where did the Pods go?** `kubectl get pods -n lean-llmd -o wide`.
   Which nodes? Run `kubectl describe node lean-llmd-control-plane | grep -A3 Taints`
   to see why none landed on the control plane.
2. **The chain of ownership.** `kubectl get deploy,rs,pods -n lean-llmd`.
   Follow `metadata.ownerReferences` from a Pod up to the Deployment
   (`kubectl get pod <pod> -n lean-llmd -o yaml`).
3. **Endpoints.** `kubectl get endpointslices -n lean-llmd -o yaml`. Find
   each Pod's IP and its `conditions`.
4. **Self-healing.** In one terminal run `kubectl get pods -n lean-llmd -w`.
   In another, delete a Pod. What appears, and which controller created it?
5. **A slow model.** Re-apply with a 30-second "model load":

   ```bash
   uv run python -c "
   import yaml; from lean_llmd import manifests as m
   print(yaml.safe_dump(m.sim_deployment(config=m.SimConfig(startup_seconds=30))))" \
     | kubectl apply -f -
   ```

   Watch `kubectl get pods -n lean-llmd -w` and
   `kubectl get endpointslices -n lean-llmd -w`. New Pods are `Running` but
   not `Ready`, and the old ones keep serving until they are. Read the probe
   failures in `kubectl describe pod`.
6. **A request through the Service, from inside the cluster.**

   ```bash
   kubectl run curl -n lean-llmd --rm -i --restart=Never --image=curlimages/curl -- \
     curl -s http://sim/v1/completions -H 'content-type: application/json' \
     -d '{"model":"mini/sim","prompt":"hello","max_tokens":5}'
   ```

7. **A rollout.** Change one flag (say `inter_token_latency_ms`) and
   re-apply. Watch `kubectl get rs -n lean-llmd -w`.

## Compare with the reference

```bash
uv run lean-llmd diff --day 1
```

It refuses until your tests pass.

## Map it to llm-d

```bash
uv run lean-llmd map --day 1
```

1. **The simulator's own `manifests/deployment.yaml`.** It runs a vLLM
   "render" container as a native sidecar (an init container with
   `restartPolicy: Always`) to tokenize with a real tokenizer. You skip it:
   with a made-up model name, the simulator tokenizes by itself.
2. **llm-d's base model-server Deployment**
   (`guides/recipes/modelserver/base/single-host/default/decode-deployment.yaml`).
   Compare its three probes with yours. It uses `/v1/models` for readiness,
   not `/health`. Why would a real vLLM answer `/health` before it can serve?

## Check yourself

1. With the default `SimConfig`, what TTFT does the simulator promise for
   a 600-token prompt with no cache hits? And for the same prompt when 592
   of its tokens are cached?
2. After step 7 of Part B, how many ReplicaSets does `kubectl get rs` show,
   and what is `spec.replicas` on each?
3. With your startup probe (`periodSeconds: 1`, `failureThreshold: 300`),
   how long may the model take to load before the kubelet restarts the
   container? What would you change for a model that takes 20 minutes?
4. During step 5, what are the loading Pod's `ready`, `serving` and
   `terminating` conditions in its EndpointSlice entry?
5. Which field of which object would you change to stop Pods from ever
   landing on `lean-llmd-worker2`? (You will use it on day 4.)
