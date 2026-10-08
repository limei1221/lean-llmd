"""Day 1 - the model server as Kubernetes objects.

A Kubernetes object is a dict: apiVersion, kind, metadata, spec. These
functions build them; ``lean-llmd render sim`` prints them as YAML for
``kubectl apply -f -``. The API server, not this file, decides whether they
are valid.
"""

from dataclasses import dataclass
from typing import Any

Obj = dict[str, Any]

SIM_IMAGE = "ghcr.io/llm-d/llm-d-inference-sim:v0.12.0"
MODEL = "mini/sim"
SIM_PORT = 8000
NAMESPACE = "lean-llmd"
# The label llm-d's metrics extractor reads to pick vLLM's metric names.
ENGINE_LABEL = "llm-d.ai/engine-type"


@dataclass(frozen=True)
class SimConfig:
    """How the simulated vLLM behaves. Times are milliseconds.

    TTFT = prefill_overhead + uncached_prompt_tokens * prefill_per_token, so a
    prefix-cache hit is visible as a shorter TTFT.
    """

    max_num_seqs: int = 4
    kv_cache_blocks: int = 512
    block_size: int = 16
    prefill_overhead_ms: int = 20
    prefill_per_token_ms: int = 2
    inter_token_latency_ms: int = 10
    startup_seconds: int = 0


def namespace(name: str = NAMESPACE) -> Obj:
    """A Namespace object named ``name``."""
    return {"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": name}}


def sim_labels(name: str) -> dict[str, str]:
    """The labels every simulator Pod carries.

    ``app: <name>`` is what selectors match on. The engine-type label tells
    llm-d which metric names this server exports.
    """
    return {"app": name, ENGINE_LABEL: "vllm"}


def sim_args(model: str, config: SimConfig) -> list[str]:
    """The simulator's command-line flags for ``model`` and ``config``.

    Always enables the KV-cache simulation (prefix-cache hits shorten TTFT)
    and listens on ``SIM_PORT``. ``--startup-duration`` is passed only when
    ``config.startup_seconds`` is non-zero.
    """
    args = [
        "--model",
        model,
        "--port",
        str(SIM_PORT),
        "--max-num-seqs",
        str(config.max_num_seqs),
        "--enable-kvcache",
        "--kv-cache-size",
        str(config.kv_cache_blocks),
        "--block-size",
        str(config.block_size),
        "--prefill-overhead",
        f"{config.prefill_overhead_ms}ms",
        "--prefill-time-per-token",
        f"{config.prefill_per_token_ms}ms",
        "--inter-token-latency",
        f"{config.inter_token_latency_ms}ms",
    ]
    if config.startup_seconds:
        args += ["--startup-duration", f"{config.startup_seconds}s"]
    return args


def sim_deployment(
    name: str = "sim",
    namespace: str = NAMESPACE,
    replicas: int = 3,
    model: str = MODEL,
    image: str = SIM_IMAGE,
    config: SimConfig | None = None,
) -> Obj:
    """A Deployment of ``replicas`` simulator Pods.

    - ``spec.selector.matchLabels`` is ``{"app": name}``; the Pod template
      carries ``sim_labels(name)``.
    - One container named ``sim`` with ``sim_args``, exposing ``SIM_PORT`` as
      a port named ``http``.
    - ``POD_IP`` and ``POD_NAME`` come from the downward API (the simulator
      needs ``POD_IP`` for its KV-cache events).
    - Probes on the ``http`` port: startup and readiness on ``/health/ready``
      (it returns 503 while the "model" loads), liveness on ``/health``.
    - Requests 50m CPU and 64Mi memory; memory limit 256Mi.
    """
    config = config or SimConfig()
    labels = sim_labels(name)
    container: Obj = {
        "name": "sim",
        "image": image,
        "imagePullPolicy": "IfNotPresent",
        "args": sim_args(model, config),
        "env": [
            {"name": "POD_IP", "valueFrom": {"fieldRef": {"fieldPath": "status.podIP"}}},
            {"name": "POD_NAME", "valueFrom": {"fieldRef": {"fieldPath": "metadata.name"}}},
        ],
        "ports": [{"name": "http", "containerPort": SIM_PORT, "protocol": "TCP"}],
        "startupProbe": {
            "httpGet": {"path": "/health/ready", "port": "http"},
            "periodSeconds": 1,
            "failureThreshold": 300,
        },
        "readinessProbe": {
            "httpGet": {"path": "/health/ready", "port": "http"},
            "periodSeconds": 2,
        },
        "livenessProbe": {
            "httpGet": {"path": "/health", "port": "http"},
            "periodSeconds": 10,
        },
        "resources": {
            "requests": {"cpu": "50m", "memory": "64Mi"},
            "limits": {"memory": "256Mi"},
        },
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
                "spec": {"containers": [container], "terminationGracePeriodSeconds": 5},
            },
        },
    }


def sim_service(name: str = "sim", namespace: str = NAMESPACE, port: int = 80) -> Obj:
    """A ClusterIP Service on ``port`` selecting ``{"app": name}``.

    It targets the container port by name (``http``), not by number, so the
    Pod template can change the number without breaking the Service.
    """
    return {
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {"name": name, "namespace": namespace, "labels": {"app": name}},
        "spec": {
            "type": "ClusterIP",
            "selector": {"app": name},
            "ports": [{"name": "http", "port": port, "targetPort": "http", "protocol": "TCP"}],
        },
    }
