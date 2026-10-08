"""Day 1 - the model server as Kubernetes objects.

Your implementation. Every function below raises until you write it.

    uv run lean-llmd test --day 1         # your code
    uv run lean-llmd test --day 1 --ref   # proves the tests are passable
    uv run lean-llmd diff --day 1         # only after you have a passing run
"""

from dataclasses import dataclass
from typing import Any

Obj = dict[str, Any]
SIM_IMAGE = "ghcr.io/llm-d/llm-d-inference-sim:v0.12.0"
MODEL = "mini/sim"
SIM_PORT = 8000
NAMESPACE = "lean-llmd"
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
    raise NotImplementedError("day 1: implement namespace()")


def sim_labels(name: str) -> dict[str, str]:
    """The labels every simulator Pod carries.

    ``app: <name>`` is what selectors match on. The engine-type label tells
    llm-d which metric names this server exports.
    """
    raise NotImplementedError("day 1: implement sim_labels()")


def sim_args(model: str, config: SimConfig) -> list[str]:
    """The simulator's command-line flags for ``model`` and ``config``.

    Always enables the KV-cache simulation (prefix-cache hits shorten TTFT)
    and listens on ``SIM_PORT``. ``--startup-duration`` is passed only when
    ``config.startup_seconds`` is non-zero.
    """
    raise NotImplementedError("day 1: implement sim_args()")


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
    raise NotImplementedError("day 1: implement sim_deployment()")


def sim_service(name: str = "sim", namespace: str = NAMESPACE, port: int = 80) -> Obj:
    """A ClusterIP Service on ``port`` selecting ``{"app": name}``.

    It targets the container port by name (``http``), not by number, so the
    Pod template can change the number without breaking the Service.
    """
    raise NotImplementedError("day 1: implement sim_service()")
