# Day 14 (optional) — Real vLLM on a GPU

~4 hours · a rented GPU machine (one GPU is enough; two for tensor parallelism)

> **Goal:** swap the simulator for vLLM and see that everything you built
> routes it unchanged. On the way, meet what the simulator hid: device
> plugins, extended resources, taints, shared memory, model downloads and a
> startup probe measured in minutes.

## What you build

| File | What to implement |
|---|---|
| `src/lean_llmd/gpu.py` | `hf_token_secret`, `model_cache_pvc`, `vllm_args`, `vllm_deployment` |

The tests run on your laptop: the API server validates GPU objects with a
server-side dry run even though kind has no GPU.

```bash
uv run lean-llmd test --day 14
```

## Part A — the objects

### Step 1. `vllm_args`

`vllm serve <model>` with `--port 8000`, `--enable-prefix-caching`, and
quiet access logs for probe paths. With `kv_events_port`, the same
`--kv-events-config` llm-d's precise guide uses. Its topic is
`kv@$(POD_IP):8000@<model>`: vLLM uses the topic verbatim, and Kubernetes
expands `$(POD_IP)` from the container's env before vLLM sees it. That is
why the Deployment sets `POD_IP` from the downward API, as day 1 did.

### Step 2. `vllm_deployment`

| Field | Why |
|---|---|
| `resources: {nvidia.com/gpu: N}` in requests **and** limits | an *extended resource*, advertised by the NVIDIA device plugin; the scheduler filters on it like CPU (day 4) |
| toleration `nvidia.com/gpu: NoSchedule` | GPU node pools are usually tainted so nothing else lands there |
| `/dev/shm` as `emptyDir: {medium: Memory}` | tensor parallelism shares tensors through shared memory; the container default is 64 MB |
| `HF_TOKEN` from a Secret | gated models; never put the token in args |
| a PVC at the Hugging Face cache | a restart should not re-download 60 GB |
| startup probe: 10 s × 180 | half an hour to download and load, then readiness and liveness take over |
| readiness on `/v1/models` | llm-d's choice: `/health` answers before the model can serve |

Same labels as the simulator, so day 3's InferencePool selects it.

## Part B — the lab (on the GPU machine)

Rent a VM with an NVIDIA GPU and drivers installed (L4, A10, A100, H100...).

```bash
# 1. Kubernetes and the device plugin
curl -sfL https://get.k3s.io | sh -                      # single-node k3s
sudo k3s kubectl get nodes
# NVIDIA's GPU operator installs the container toolkit and the device plugin.
# On k3s it needs k3s's containerd paths; follow NVIDIA's "Getting Started"
# for your distribution: https://docs.nvidia.com/datacenter/cloud-native/gpu-operator/
helm repo add nvidia https://helm.ngc.nvidia.com/nvidia && helm repo update
helm install gpu-operator nvidia/gpu-operator -n gpu-operator --create-namespace  # + k3s values
kubectl get node -o jsonpath='{.items[0].status.allocatable}'   # look for nvidia.com/gpu

# 2. The course, pointed at this cluster
#    (copy the repo; build and import the image with k3s: docker save | k3s ctr images import -)
kubectl create namespace lean-llmd
```

Then, from Python on that machine (or rendered on your laptop):

```python
import yaml
from lean_llmd import gpu, discovery as d, deploy

objs = [
    gpu.hf_token_secret("lean-llmd", "<token>"),
    gpu.model_cache_pvc("cache", "lean-llmd"),
    gpu.vllm_deployment("sim", "lean-llmd", "Qwen/Qwen3-0.6B", cache_claim="cache"),
    d.inference_pool("pool", "lean-llmd", {"app": "sim"}, 8000, "epp"),
    *deploy.epp_objects("epp", "lean-llmd", "pool"),
]
print(yaml.safe_dump_all(objs))
```

The Deployment is named `sim` on purpose: the label `app: sim` is what
everything else selects.

1. Watch the Pod start: `kubectl get pods -w`, `kubectl describe pod`. How
   long in each phase (image pull, download, load, CUDA graph capture)?
2. Request without a GPU available (scale to more replicas than GPUs). Quote
   the scheduler's event.
3. Run day 7's load generator against `http://epp`. Your TTFT and TPOT are
   now real; compare with the simulator's latency model. Tune
   `SimConfig` until the simulator matches your GPU; that calibration is how
   llm-d's own simulator profiles are made.
4. With two GPUs: two replicas versus one with `--tensor-parallel-size 2`.
   Which gives better throughput, and which better TTFT?
5. Day 8 on real vLLM: `vllm_args(..., kv_events_port=5557)` and
   `--feature precise`. The render endpoint is vLLM's own `/render`; does
   your tokenizer call still work?

## Map it

```bash
uv run lean-llmd map --day 14
```

1. **llm-d's release image component**: the vLLM version llm-d ships with.
2. **The optimized-baseline GPU patch**: compare resources, volumes and the
   startup budget with yours.
3. **The precise-routing GPU patch**: the KV-events config, including a
   replay endpoint you did not use.

## Check yourself

1. What does `kubectl get node -o jsonpath='{.items[0].status.allocatable}'`
   show for `nvidia.com/gpu` on your machine?
2. How long did the first start take, and the second (with the PVC warm)?
3. Your measured TTFT for a 600-token prompt, cold and with a cached prefix.
4. The `SimConfig` that best matched your GPU.
