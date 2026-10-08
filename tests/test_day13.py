"""Day 13 - failure modes.

Oracles: Envoy's own config validator, the API server's eviction API (it
enforces disruption budgets), real Envoy with a dead endpoint picker, and
the client's error count during a rolling restart under load.
"""

import json
import subprocess
import tempfile
import threading
import time
from pathlib import Path

import pytest
import yaml

import k8s

ENVOY_IMAGE = "docker.io/envoyproxy/envoy:distroless-v1.33.2"


@pytest.fixture
def rs(mod):
    return mod("resilience")


def test_pdb(rs):
    pdb = rs.pod_disruption_budget("sim", "ns", {"app": "sim"}, 2)
    assert pdb["apiVersion"] == "policy/v1" and pdb["kind"] == "PodDisruptionBudget"
    assert pdb["spec"] == {"minAvailable": 2, "selector": {"matchLabels": {"app": "sim"}}}


def test_zero_downtime(rs, mod):
    original = mod("manifests").sim_deployment()
    d = rs.zero_downtime(original, pre_stop_seconds=7)
    assert d["spec"]["strategy"] == {
        "type": "RollingUpdate",
        "rollingUpdate": {"maxSurge": 1, "maxUnavailable": 0},
    }
    pod = d["spec"]["template"]["spec"]
    assert pod["containers"][0]["lifecycle"] == {"preStop": {"sleep": {"seconds": 7}}}
    assert pod["terminationGracePeriodSeconds"] == 17
    assert "strategy" not in original["spec"]


def proxy_routes(config):
    listener = next(x for x in config["static_resources"]["listeners"] if x["name"] == "proxy")
    manager = listener["filter_chains"][0]["filters"][0]["typed_config"]
    return manager, manager["route_config"]["virtual_hosts"][0]["routes"]


def test_fail_open_routes_by_header_presence(rs, mod):
    original = mod("deploy").envoy_config()
    config = rs.fail_open(original, "sim.ns.svc.cluster.local", 80)
    manager, routes = proxy_routes(config)
    ext_proc = next(f for f in manager["http_filters"] if f["name"].endswith("ext_proc"))
    assert ext_proc["typed_config"]["failure_mode_allow"] is True
    assert routes[0]["match"]["headers"] == [
        {"name": "x-gateway-destination-endpoint", "present_match": True}
    ]
    assert routes[0]["route"]["cluster"] == "original_destination_cluster"
    assert routes[1] == {
        "match": {"prefix": "/"},
        "route": {"cluster": "fallback", "timeout": "300s"},
    }
    fallback = next(c for c in config["static_resources"]["clusters"] if c["name"] == "fallback")
    assert fallback["type"] == "STRICT_DNS"
    _, original_routes = proxy_routes(original)
    assert len(original_routes) == 1, "input left alone"


def envoy_validates(config):
    with tempfile.TemporaryDirectory() as tmp:
        Path(tmp, "envoy.yaml").write_text(yaml.safe_dump(config))
        result = subprocess.run(
            [
                "docker",
                "run",
                "--rm",
                "-v",
                f"{tmp}:/cfg",
                ENVOY_IMAGE,
                "--mode",
                "validate",
                "-c",
                "/cfg/envoy.yaml",
            ],
            capture_output=True,
            text=True,
            timeout=120,
        )
    return result.returncode == 0, result.stdout + result.stderr


@pytest.mark.docker
def test_envoy_accepts_both_configs(rs, mod):
    base = mod("deploy").envoy_config()
    for config in (base, rs.fail_open(base, "sim.ns.svc.cluster.local", 80)):
        ok, output = envoy_validates(config)
        assert ok, output[-2000:]


@pytest.mark.kind
def test_the_api_server_enforces_the_budget(rs, mod):
    manifests = mod("manifests")
    with k8s.temp_namespace("day13") as ns:
        k8s.apply(
            [
                manifests.sim_deployment(namespace=ns, replicas=3),
                rs.pod_disruption_budget("sim", ns, {"app": "sim"}, 2),
            ]
        )
        k8s.wait_until(lambda: len(k8s.ready_pod_ips(ns, "app=sim")) == 3, timeout=240)
        k8s.wait_until(
            lambda: (
                k8s.get_json("pdb", "sim", "-n", ns).get("status", {}).get("disruptionsAllowed")
                == 1
            ),
            timeout=60,
            what="the PDB to allow one disruption",
        )
        pods = [
            p["metadata"]["name"] for p in k8s.get_json("pods", "-n", ns, "-l", "app=sim")["items"]
        ]

        def evict(pod):
            body = json.dumps(
                {
                    "apiVersion": "policy/v1",
                    "kind": "Eviction",
                    "metadata": {"name": pod, "namespace": ns},
                }
            )
            return subprocess.run(
                [
                    "kubectl",
                    "--context",
                    k8s.CONTEXT,
                    "create",
                    "--raw",
                    f"/api/v1/namespaces/{ns}/pods/{pod}/eviction",
                    "-f",
                    "-",
                ],
                input=body,
                capture_output=True,
                text=True,
            )

        first = evict(pods[0])
        assert first.returncode == 0, first.stderr
        second = evict(pods[1])
        assert second.returncode != 0, "a second eviction would break the budget"
        assert "disruption budget" in second.stderr.lower() or "429" in second.stderr


def deploy_dead_picker(mod, rs, ns, package, fail_open):
    """Simulators behind a Service, an EPP Pod whose picker never starts, Envoy in front."""
    manifests, disc, deploy = mod("manifests"), mod("discovery"), mod("deploy")
    config = deploy.envoy_config()
    if fail_open:
        config = rs.fail_open(config, f"sim.{ns}.svc.cluster.local", 80)
    configmap = deploy.envoy_config_map("epp", ns)
    configmap["data"]["envoy.yaml"] = yaml.safe_dump(config, sort_keys=False)
    epp = deploy.epp_deployment("epp", ns, "pool", package=package)
    picker = epp["spec"]["template"]["spec"]["containers"][0]
    picker["command"] = ["python", "-c", "import time; time.sleep(10**9)"]
    picker["args"] = []
    picker.pop("readinessProbe")
    k8s.apply(
        [
            manifests.sim_deployment(namespace=ns, replicas=2),
            manifests.sim_service(namespace=ns),
            disc.inference_pool("pool", ns, {"app": "sim"}, manifests.SIM_PORT, "epp"),
            *deploy.epp_rbac("epp", ns),
            configmap,
            epp,
            deploy.epp_service("epp", ns),
        ]
    )
    k8s.kubectl("rollout", "restart", "deployment/epp", "-n", ns)
    for name in ("sim", "epp"):
        k8s.kubectl("rollout", "status", f"deployment/{name}", "-n", ns, "--timeout=240s")


def status_through_envoy(ns):
    """One request through Envoy; retries only while a restarted Pod is still going away."""
    out = k8s.run_python_in_cluster(
        ns,
        """
        import httpx, time
        for attempt in range(10):
            try:
                r = httpx.post("http://epp/v1/completions", timeout=30,
                               json={"model": "mini/sim", "prompt": "hi", "max_tokens": 2})
                print(r.status_code)
                break
            except httpx.ConnectError:
                time.sleep(1)
        """,
    )
    return int(out.strip().splitlines()[-1])


@pytest.mark.kind
def test_fail_open_serves_without_the_picker(mod, rs, package):
    if not k8s.image_present_in_kind():
        pytest.skip("the lean-llmd:dev image is not in kind; run: uv run lean-llmd image")
    with k8s.temp_namespace("day13") as ns:
        deploy_dead_picker(mod, rs, ns, package, fail_open=True)
        assert status_through_envoy(ns) == 200, "fail-open falls back to the Service"
        deploy_dead_picker(mod, rs, ns, package, fail_open=False)
        assert status_through_envoy(ns) >= 500, "fail-closed refuses"


@pytest.mark.kind
def test_a_rollout_under_load_drops_nothing(mod, rs, package):
    if not k8s.image_present_in_kind():
        pytest.skip("the lean-llmd:dev image is not in kind; run: uv run lean-llmd image")
    manifests, disc, deploy = mod("manifests"), mod("discovery"), mod("deploy")
    with k8s.temp_namespace("day13") as ns:
        k8s.apply(
            [
                rs.zero_downtime(manifests.sim_deployment(namespace=ns, replicas=3)),
                disc.inference_pool("pool", ns, {"app": "sim"}, manifests.SIM_PORT, "epp"),
                *deploy.epp_objects("epp", ns, "pool", package=package),
            ]
        )
        for name in ("sim", "epp"):
            k8s.kubectl("rollout", "status", f"deployment/{name}", "-n", ns, "--timeout=240s")
        result = {}

        def load():
            result["out"] = k8s.run_python_in_cluster(
                ns,
                """
                import httpx, json, threading, time
                stats = {"ok": 0, "errors": []}
                def loop():
                    end = time.time() + 40
                    with httpx.Client(timeout=30) as c:
                        while time.time() < end:
                            try:
                                r = c.post("http://epp/v1/completions", json={
                                    "model": "mini/sim", "prompt": "hi", "max_tokens": 20,
                                    "ignore_eos": True})
                                if r.status_code == 200: stats["ok"] += 1
                                else: stats["errors"].append(r.status_code)
                            except Exception as e:
                                stats["errors"].append(type(e).__name__)
                ts = [threading.Thread(target=loop) for _ in range(6)]
                for t in ts: t.start()
                for t in ts: t.join()
                print(json.dumps(stats))
            """,
            )

        loader = threading.Thread(target=load)
        loader.start()
        time.sleep(8)
        k8s.kubectl("rollout", "restart", "deployment/sim", "-n", ns)
        k8s.kubectl("rollout", "status", "deployment/sim", "-n", ns, "--timeout=240s")
        loader.join()
    stats = json.loads(result["out"].strip().splitlines()[-1])
    assert stats["ok"] > 100
    assert stats["errors"] == [], f"dropped during the rollout: {stats['errors'][:10]}"
