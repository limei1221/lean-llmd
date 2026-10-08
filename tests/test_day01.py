"""Day 1 - the model server as Kubernetes objects.

Oracles: the simulator itself (do your probe paths mean what you think?), and
the Kubernetes API server and controllers (is the object valid, does the
Deployment converge, do probes gate the Service's endpoints?).
"""

import time

import httpx
import pytest

import k8s


@pytest.fixture
def manifests(mod):
    return mod("manifests")


def flags(args: list[str]) -> dict[str, str | None]:
    """``["--a", "1", "--b"]`` -> ``{"--a": "1", "--b": None}``."""
    out: dict[str, str | None] = {}
    i = 0
    while i < len(args):
        if i + 1 < len(args) and not args[i + 1].startswith("--"):
            out[args[i]] = args[i + 1]
            i += 2
        else:
            out[args[i]] = None
            i += 1
    return out


def container(deployment):
    (c,) = deployment["spec"]["template"]["spec"]["containers"]
    return c


def test_namespace_object(manifests):
    assert manifests.namespace("abc") == {
        "apiVersion": "v1",
        "kind": "Namespace",
        "metadata": {"name": "abc"},
    }


def test_sim_args_carry_the_config(manifests):
    config = manifests.SimConfig(
        max_num_seqs=7,
        kv_cache_blocks=99,
        block_size=32,
        prefill_overhead_ms=5,
        prefill_per_token_ms=3,
        inter_token_latency_ms=4,
    )
    f = flags(manifests.sim_args("m/x", config))
    assert f["--model"] == "m/x"
    assert f["--port"] == str(manifests.SIM_PORT)
    assert f["--max-num-seqs"] == "7"
    assert "--enable-kvcache" in f
    assert f["--kv-cache-size"] == "99"
    assert f["--block-size"] == "32"
    assert f["--prefill-overhead"] == "5ms"
    assert f["--prefill-time-per-token"] == "3ms"
    assert f["--inter-token-latency"] == "4ms"
    assert "--startup-duration" not in f
    slow = manifests.sim_args("m/x", manifests.SimConfig(startup_seconds=12))
    assert flags(slow)["--startup-duration"] == "12s"


def test_the_deployment_selects_its_own_pods(manifests):
    d = manifests.sim_deployment(name="s1", namespace="ns", replicas=2)
    assert d["apiVersion"] == "apps/v1" and d["kind"] == "Deployment"
    assert d["metadata"]["namespace"] == "ns"
    assert d["spec"]["replicas"] == 2
    selector = d["spec"]["selector"]["matchLabels"]
    template_labels = d["spec"]["template"]["metadata"]["labels"]
    assert selector == {"app": "s1"}
    # The API server rejects a Deployment whose selector misses its template.
    assert all(template_labels.get(k) == v for k, v in selector.items())
    assert template_labels["llm-d.ai/engine-type"] == "vllm"


def test_the_service_reaches_the_pods_by_port_name(manifests):
    d = manifests.sim_deployment(name="s1")
    s = manifests.sim_service(name="s1", port=8080)
    template_labels = d["spec"]["template"]["metadata"]["labels"]
    assert all(template_labels.get(k) == v for k, v in s["spec"]["selector"].items())
    (port,) = s["spec"]["ports"]
    assert port["port"] == 8080
    port_names = {p["name"]: p["containerPort"] for p in container(d)["ports"]}
    assert port["targetPort"] in port_names, "target the container port by its name"
    assert port_names[port["targetPort"]] == manifests.SIM_PORT


def test_pod_ip_comes_from_the_downward_api(manifests):
    env = {e["name"]: e for e in container(manifests.sim_deployment())["env"]}
    assert env["POD_IP"]["valueFrom"]["fieldRef"]["fieldPath"] == "status.podIP"


def test_resources_are_requested(manifests):
    resources = container(manifests.sim_deployment())["resources"]
    assert resources["requests"]["cpu"] and resources["requests"]["memory"]
    assert resources["limits"]["memory"]


@pytest.mark.docker
def test_probe_paths_mean_what_the_probes_need(manifests):
    """Readiness must fail while the model loads; liveness must not."""
    config = manifests.SimConfig(startup_seconds=4)
    c = container(manifests.sim_deployment(config=config))
    ready_path = c["readinessProbe"]["httpGet"]["path"]
    startup_path = c["startupProbe"]["httpGet"]["path"]
    live_path = c["livenessProbe"]["httpGet"]["path"]
    with k8s.sim_container(manifests.sim_args(manifests.MODEL, config)) as url:
        assert httpx.get(url + live_path).status_code == 200, "liveness fails while loading"
        assert httpx.get(url + ready_path).status_code == 503, "readiness passes while loading"
        assert httpx.get(url + startup_path).status_code == 503
        time.sleep(4.5)
        assert httpx.get(url + ready_path).status_code == 200
        assert httpx.get(url + live_path).status_code == 200


@pytest.mark.docker
def test_the_simulator_reports_the_cache_you_configured(manifests):
    config = manifests.SimConfig(kv_cache_blocks=77, block_size=32)
    with k8s.sim_container(manifests.sim_args(manifests.MODEL, config)) as url:
        text = httpx.get(url + "/metrics").text
    info = next(line for line in text.splitlines() if line.startswith("vllm:cache_config_info"))
    assert 'block_size="32"' in info and 'num_gpu_blocks="77"' in info


@pytest.fixture
def namespace():
    with k8s.temp_namespace("day01") as ns:
        yield ns


@pytest.mark.kind
def test_the_api_server_accepts_the_objects(manifests, namespace):
    objects = [
        manifests.sim_deployment(namespace=namespace),
        manifests.sim_service(namespace=namespace),
    ]
    out = k8s.apply(objects, dry_run=True)
    assert "deployment.apps/sim created (server dry run)" in out
    assert "service/sim created (server dry run)" in out


@pytest.mark.kind
def test_three_replicas_become_three_ready_endpoints(manifests, namespace):
    k8s.apply(
        [
            manifests.sim_deployment(namespace=namespace, replicas=3),
            manifests.sim_service(namespace=namespace),
        ]
    )
    ips = k8s.wait_until(
        lambda: (lambda s: s if len(s) == 3 else None)(k8s.ready_pod_ips(namespace, "app=sim")),
        timeout=240,
        what="3 ready simulator pods",
    )
    k8s.wait_until(
        lambda: k8s.endpointslice_ready_ips(namespace, "sim") == ips,
        what="the Service's EndpointSlice to list exactly the ready pods",
    )


@pytest.mark.kind
def test_the_deployment_replaces_a_deleted_pod(manifests, namespace):
    k8s.apply([manifests.sim_deployment(namespace=namespace, replicas=2)])
    k8s.wait_until(lambda: len(k8s.ready_pod_ips(namespace, "app=sim")) == 2, timeout=240)
    pods = k8s.get_json("pods", "-n", namespace, "-l", "app=sim")["items"]
    victim = pods[0]["metadata"]["name"]
    k8s.kubectl("delete", "pod", victim, "-n", namespace, "--wait=false")

    def replaced() -> bool:
        names = {
            p["metadata"]["name"]
            for p in k8s.get_json("pods", "-n", namespace, "-l", "app=sim")["items"]
            if not p["metadata"].get("deletionTimestamp")
        }
        return victim not in names and len(k8s.ready_pod_ips(namespace, "app=sim")) == 2

    k8s.wait_until(replaced, timeout=120, what="a replacement pod")


@pytest.mark.kind
def test_readiness_keeps_a_loading_pod_out_of_the_service(manifests, namespace):
    config = manifests.SimConfig(startup_seconds=15)
    k8s.apply(
        [
            manifests.sim_deployment(namespace=namespace, replicas=1, config=config),
            manifests.sim_service(namespace=namespace),
        ]
    )

    def running_ip() -> str | None:
        for p in k8s.get_json("pods", "-n", namespace, "-l", "app=sim")["items"]:
            if p.get("status", {}).get("phase") == "Running" and p["status"].get("podIP"):
                return str(p["status"]["podIP"])
        return None

    ip = k8s.wait_until(running_ip, timeout=240, interval=0.5, what="a running pod")
    assert ip not in k8s.endpointslice_ready_ips(namespace, "sim"), "a loading pod got traffic"
    k8s.wait_until(
        lambda: ip in k8s.endpointslice_ready_ips(namespace, "sim"),
        timeout=60,
        what="the pod to become a ready endpoint once loaded",
    )
