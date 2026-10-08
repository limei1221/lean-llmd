"""kubectl, kind and Docker plumbing shared by the CLI and the tests. Not course material."""

import contextlib
import json
import re
import shutil
import subprocess
import textwrap
import time
import uuid
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import yaml

ROOT = Path(__file__).resolve().parent.parent.parent
CLUSTER = "lean-llmd"
CONTEXT = f"kind-{CLUSTER}"
COURSE_IMAGE = "lean-llmd:dev"
GAIE_CRDS = (
    "https://github.com/kubernetes-sigs/gateway-api-inference-extension/"
    "releases/download/{version}/v1-manifests.yaml"
)
OBJECTIVE_CRD = (
    "https://raw.githubusercontent.com/llm-d/llm-d-router/{version}/"
    "config/crd/bases/llm-d.ai_inferenceobjectives.yaml"
)
GATEWAY_API_CRDS = "https://github.com/kubernetes-sigs/gateway-api/releases/download/{version}/standard-install.yaml"


def run(cmd: list[str], input: str | None = None, check: bool = True, timeout: int = 600) -> str:
    result = subprocess.run(cmd, input=input, capture_output=True, text=True, timeout=timeout)
    if check and result.returncode != 0:
        raise RuntimeError(f"{' '.join(cmd[:4])} ... failed:\n{result.stderr or result.stdout}")
    return result.stdout


def kubectl(*args: str, input: str | None = None, check: bool = True, timeout: int = 600) -> str:
    return run(["kubectl", "--context", CONTEXT, *args], input=input, check=check, timeout=timeout)


def get_json(*args: str) -> Any:
    return json.loads(kubectl("get", *args, "-o", "json"))


def apply(objects: list[dict[str, Any]], dry_run: bool = False) -> str:
    """``kubectl apply`` the objects; ``dry_run`` makes the API server validate, not store."""
    text = yaml.safe_dump_all(objects, sort_keys=False)
    extra = ["--dry-run=server"] if dry_run else []
    return kubectl("apply", *extra, "-f", "-", input=text)


def wait_until(
    predicate: Callable[[], Any], timeout: float = 120, interval: float = 1.0, what: str = ""
) -> Any:
    deadline = time.monotonic() + timeout
    last: Any = None
    while time.monotonic() < deadline:
        last = predicate()
        if last:
            return last
        time.sleep(interval)
    raise TimeoutError(f"timed out after {timeout}s waiting for {what or predicate}; last={last!r}")


def docker_unavailable() -> str | None:
    if shutil.which("docker") is None:
        return "docker is not installed"
    if subprocess.run(["docker", "info"], capture_output=True).returncode != 0:
        return "the Docker daemon is not running"
    return None


def kind_unavailable() -> str | None:
    if shutil.which("kubectl") is None:
        return "kubectl is not installed"
    probe = subprocess.run(
        [
            "kubectl",
            "--context",
            CONTEXT,
            "get",
            "crd",
            "inferencepools.inference.networking.k8s.io",
        ],
        capture_output=True,
        timeout=20,
    )
    if probe.returncode != 0:
        return f"no {CONTEXT} cluster with the InferencePool CRD (run: uv run lean-llmd cluster up)"
    return None


def image_present_in_kind(image: str = COURSE_IMAGE) -> bool:
    out = subprocess.run(
        ["docker", "exec", f"{CLUSTER}-worker", "crictl", "images", "-o", "json"],
        capture_output=True,
        text=True,
    )
    if out.returncode != 0:
        return False
    tags = [tag for img in json.loads(out.stdout)["images"] for tag in img["repoTags"]]
    return any(tag.endswith(image) for tag in tags)


@contextlib.contextmanager
def temp_namespace(prefix: str) -> Iterator[str]:
    name = f"{prefix}-{uuid.uuid4().hex[:6]}"
    kubectl("create", "namespace", name)
    try:
        yield name
    finally:
        kubectl("delete", "namespace", name, "--wait=false", check=False)


def ready_pod_ips(namespace: str, selector: str) -> set[str]:
    """IPs of Pods the API server reports Ready (the kubelet's verdict)."""
    ips = set()
    for pod in get_json("pods", "-n", namespace, "-l", selector)["items"]:
        if pod["metadata"].get("deletionTimestamp"):
            continue
        conditions = pod.get("status", {}).get("conditions") or []
        if any(c["type"] == "Ready" and c["status"] == "True" for c in conditions):
            ips.add(pod["status"]["podIP"])
    return ips


def endpointslice_ready_ips(namespace: str, service: str) -> set[str]:
    """Addresses the EndpointSlice controller marks ready for ``service``."""
    slices = get_json(
        "endpointslices", "-n", namespace, "-l", f"kubernetes.io/service-name={service}"
    )["items"]
    ips = set()
    for s in slices:
        for endpoint in s.get("endpoints") or []:
            if (endpoint.get("conditions") or {}).get("ready"):
                ips.update(endpoint["addresses"])
    return ips


def pod_metrics(namespace: str, pod: str, port: int = 8000) -> str:
    """A Pod's /metrics through the API server's proxy: no port-forward needed."""
    return kubectl(
        "get", "--raw", f"/api/v1/namespaces/{namespace}/pods/{pod}:{port}/proxy/metrics"
    )


def sim_counters(namespace: str, selector: str = "app=sim") -> dict[str, dict[str, float]]:
    """Per simulator Pod: requests served, prefix-cache hit and query tokens, from its /metrics."""

    def total(text: str, name: str) -> float:
        return sum(float(v) for v in re.findall(rf"^{name}(?:{{.*}})? (\S+)$", text, re.M))

    counters = {}
    for pod in get_json("pods", "-n", namespace, "-l", selector)["items"]:
        if pod["metadata"].get("deletionTimestamp"):
            continue
        text = pod_metrics(namespace, pod["metadata"]["name"])
        counters[pod["metadata"]["name"]] = {
            "served": total(text, "vllm:request_success_total"),
            "hit_tokens": total(text, "vllm:prefix_cache_hits_total"),
            "query_tokens": total(text, "vllm:prefix_cache_queries_total"),
        }
    return counters


def run_in_cluster(
    namespace: str, args: list[str], name: str = "client", timeout: int = 900
) -> str:
    """Run a command in a throwaway Pod built from the course image; return its stdout."""
    return kubectl(
        "run",
        f"{name}-{uuid.uuid4().hex[:5]}",
        "-n",
        namespace,
        f"--image={COURSE_IMAGE}",
        "--image-pull-policy=IfNotPresent",
        "--rm",
        "-i",
        "--restart=Never",
        "--quiet",
        "--command",
        "--",
        *args,
        timeout=timeout,
    )


def run_python_in_cluster(namespace: str, script: str) -> str:
    return run_in_cluster(namespace, ["python", "-c", textwrap.dedent(script)])
