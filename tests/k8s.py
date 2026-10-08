"""Test plumbing for Docker and kind. Not an oracle: it only runs things and reads results."""

import contextlib
import subprocess
import uuid
from collections.abc import Iterator

import httpx

from lean_llmd_course.kube import (  # noqa: F401 - re-exported for the day tests
    CONTEXT,
    apply,
    docker_unavailable,
    endpointslice_ready_ips,
    get_json,
    image_present_in_kind,
    kind_unavailable,
    kubectl,
    pod_metrics,
    ready_pod_ips,
    run_python_in_cluster,
    sim_counters,
    temp_namespace,
    wait_until,
)

SIM_IMAGE = "ghcr.io/llm-d/llm-d-inference-sim:v0.12.0"
_CONTAINERS: dict[str, str] = {}


def _docker_port(name: str, container_port: int) -> int:
    out = subprocess.run(
        ["docker", "port", name, str(container_port)], check=True, capture_output=True, text=True
    ).stdout
    return int(out.splitlines()[0].split(":")[-1].strip())


@contextlib.contextmanager
def sim_container(args: list[str], extra_ports: tuple[int, ...] = ()) -> Iterator[str]:
    """Run the simulator in Docker with ``args``; yield its base URL on localhost.

    ``extra_ports`` are published too; find them with ``published_port``.
    """
    name = f"lean-llmd-sim-{uuid.uuid4().hex[:6]}"
    publish = [arg for port in (8000, *extra_ports) for arg in ("-p", f"127.0.0.1::{port}")]
    subprocess.run(
        [
            "docker",
            "run",
            "-d",
            "--rm",
            "--name",
            name,
            *publish,
            "-e",
            "POD_IP=127.0.0.1",
            SIM_IMAGE,
            *args,
        ],
        check=True,
        capture_output=True,
    )
    url = ""
    try:
        url = f"http://127.0.0.1:{_docker_port(name, 8000)}"
        _CONTAINERS[url] = name

        def healthy() -> bool:
            try:
                return httpx.get(f"{url}/health", timeout=1).status_code == 200
            except httpx.HTTPError:
                return False

        wait_until(healthy, timeout=60, interval=0.3, what="the simulator container")
        yield url
    finally:
        _CONTAINERS.pop(url, None)
        subprocess.run(["docker", "rm", "-f", name], capture_output=True)


def published_port(url: str, container_port: int) -> int:
    """The localhost port Docker published for ``container_port`` of the simulator at ``url``."""
    return _docker_port(_CONTAINERS[url], container_port)
