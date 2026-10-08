"""Day 14 (optional, needs a GPU) - real vLLM in place of the simulator.

Shaped like llm-d's base model-server recipe
(``guides/recipes/modelserver/base``) with its release vLLM image. The Pods
carry the same labels as the simulator's, so the InferencePool, your EPP and
``lean-llmd bench`` work unchanged; only the model server is real.
"""

from typing import Any

Obj = dict[str, Any]

VLLM_IMAGE = "docker.io/vllm/vllm-openai:v0.30.0"
GPU_RESOURCE = "nvidia.com/gpu"
HF_SECRET = "llm-d-hf-token"


def hf_token_secret(namespace: str, token: str, name: str = HF_SECRET) -> Obj:
    """An Opaque Secret holding ``HF_TOKEN`` (``stringData``, so no base64 by hand)."""
    return {
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {"name": name, "namespace": namespace},
        "type": "Opaque",
        "stringData": {"HF_TOKEN": token},
    }


def model_cache_pvc(name: str, namespace: str, size: str = "100Gi") -> Obj:
    """A ReadWriteOnce PersistentVolumeClaim of ``size`` for downloaded weights."""
    return {
        "apiVersion": "v1",
        "kind": "PersistentVolumeClaim",
        "metadata": {"name": name, "namespace": namespace},
        "spec": {"accessModes": ["ReadWriteOnce"], "resources": {"requests": {"storage": size}}},
    }


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
    args = [
        model,
        "--port",
        "8000",
        "--max-model-len",
        str(max_model_len),
        "--gpu-memory-utilization",
        str(gpu_memory_utilization),
        "--tensor-parallel-size",
        str(tensor_parallel_size),
        "--enable-prefix-caching",
        "--disable-access-log-for-endpoints=/health,/metrics,/v1/models",
    ]
    if kv_events_port is not None:
        args += [
            "--kv-events-config",
            '{"enable_kv_cache_events": true, "publisher": "zmq", '
            f'"endpoint": "tcp://*:{kv_events_port}", "topic": "kv@$(POD_IP):8000@{model}"}}',
        ]
    return args


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
    labels = {"app": name, "llm-d.ai/engine-type": "vllm"}
    volumes: list[Obj] = [{"name": "shm", "emptyDir": {"medium": "Memory", "sizeLimit": "8Gi"}}]
    mounts: list[Obj] = [{"name": "shm", "mountPath": "/dev/shm"}]
    if cache_claim is not None:
        volumes.append({"name": "model-cache", "persistentVolumeClaim": {"claimName": cache_claim}})
        mounts.append({"name": "model-cache", "mountPath": "/root/.cache/huggingface"})
    container: Obj = {
        "name": "vllm",
        "image": image,
        "command": ["vllm", "serve"],
        "args": args if args is not None else vllm_args(model, tensor_parallel_size=gpus),
        "env": [
            {"name": "POD_IP", "valueFrom": {"fieldRef": {"fieldPath": "status.podIP"}}},
            {
                "name": "HF_TOKEN",
                "valueFrom": {"secretKeyRef": {"name": HF_SECRET, "key": "HF_TOKEN"}},
            },
        ],
        "ports": [{"name": "http", "containerPort": 8000, "protocol": "TCP"}],
        "resources": {"requests": {GPU_RESOURCE: gpus}, "limits": {GPU_RESOURCE: gpus}},
        "startupProbe": {
            "httpGet": {"path": "/health", "port": "http"},
            "periodSeconds": 10,
            "failureThreshold": 180,
        },
        "readinessProbe": {"httpGet": {"path": "/v1/models", "port": "http"}, "periodSeconds": 5},
        "livenessProbe": {"httpGet": {"path": "/health", "port": "http"}, "periodSeconds": 10},
        "volumeMounts": mounts,
    }
    return {
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {"name": name, "namespace": namespace, "labels": labels},
        "spec": {
            "replicas": replicas,
            "selector": {"matchLabels": {"app": name}},
            "template": {
                "metadata": {"labels": labels},
                "spec": {
                    "containers": [container],
                    "volumes": volumes,
                    "tolerations": [
                        {"key": GPU_RESOURCE, "operator": "Exists", "effect": "NoSchedule"}
                    ],
                },
            },
        },
    }
