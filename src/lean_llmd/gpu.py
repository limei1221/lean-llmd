"""Day 14 (optional) - real vLLM on a GPU.

Your implementation. Every function below raises until you write it.

    uv run lean-llmd test --day 14         # your code
    uv run lean-llmd test --day 14 --ref   # proves the tests are passable
    uv run lean-llmd diff --day 14         # only after you have a passing run
"""

from typing import Any

Obj = dict[str, Any]
VLLM_IMAGE = "docker.io/vllm/vllm-openai:v0.30.0"
GPU_RESOURCE = "nvidia.com/gpu"
HF_SECRET = "llm-d-hf-token"


def hf_token_secret(namespace: str, token: str, name: str = HF_SECRET) -> Obj:
    """An Opaque Secret holding ``HF_TOKEN`` (``stringData``, so no base64 by hand)."""
    raise NotImplementedError("day 14: implement hf_token_secret()")


def model_cache_pvc(name: str, namespace: str, size: str = "100Gi") -> Obj:
    """A ReadWriteOnce PersistentVolumeClaim of ``size`` for downloaded weights."""
    raise NotImplementedError("day 14: implement model_cache_pvc()")


def vllm_args(
    model: str,
    max_model_len: int = 8192,
    gpu_memory_utilization: float = 0.9,
    tensor_parallel_size: int = 1,
    kv_events_port: int | None = None,
) -> list[str]:
    """``vllm serve`` arguments: the model first, then flags.

    Always ``--port 8000``, ``--enable-prefix-caching`` and
    ``--disable-access-log-for-endpoints=/health,/metrics,/v1/models``
    (probes and scrapes would drown the log). With ``kv_events_port``, add
    ``--kv-events-config`` as JSON publishing on ``tcp://*:<port>`` (day 8's
    subscribers dial it) under topic ``kv@$(POD_IP):8000@<model>``. vLLM uses
    the topic verbatim; Kubernetes expands ``$(POD_IP)`` from the
    container's env before vLLM starts.
    """
    raise NotImplementedError("day 14: implement vllm_args()")


def vllm_deployment(
    name: str,
    namespace: str,
    model: str,
    gpus: int = 1,
    replicas: int = 1,
    image: str = VLLM_IMAGE,
    cache_claim: str | None = None,
    args: list[str] | None = None,
) -> Obj:
    """A Deployment of vLLM that drops into the course in place of the simulator.

    - Labels ``app: <name>`` and ``llm-d.ai/engine-type: vllm``, as day 1.
    - ``command: ["vllm", "serve"]`` with ``args`` (default ``vllm_args(model,
      tensor_parallel_size=gpus)``); port ``http`` 8000.
    - ``nvidia.com/gpu: gpus`` in both requests and limits (extended
      resources must be equal), plus a toleration for the
      ``nvidia.com/gpu`` NoSchedule taint GPU node pools usually carry.
    - ``POD_IP`` from the downward API (the KV-events topic needs it);
      ``HF_TOKEN`` from the Secret's ``HF_TOKEN`` key; ``/dev/shm`` as a
      memory-backed ``emptyDir`` (tensor parallelism shares memory there);
      with ``cache_claim``, the PVC mounted at ``/root/.cache/huggingface``.
    - Probes: startup on ``/health`` every 10 s for up to 30 minutes (weights
      download and load), readiness on ``/v1/models``, liveness on ``/health``.
    """
    raise NotImplementedError("day 14: implement vllm_deployment()")
