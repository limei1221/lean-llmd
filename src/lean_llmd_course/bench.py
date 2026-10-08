"""``lean-llmd bench``: three routers in front of the same simulators, one workload.

- ``service``: a plain Kubernetes Service (kube-proxy picks a Pod at random).
- ``epp``: your EPP (or the reference with ``--ref``) behind Envoy.
- ``llm-d``: llm-d's own EPP and Envoy, from the pinned Helm chart.

Before each run the simulators and routers restart, so every router starts
with cold KV caches and an empty prefix index. The load generator (yours, or
the reference's) runs inside the cluster as a throwaway Pod.
"""

import importlib
import json
import shutil
import time
from pathlib import Path
from typing import Any

from lean_llmd_course import kube
from lean_llmd_course.days import PINS

NAMESPACE = "lean-llmd-bench"
LLMD_RELEASE = "llmd"
LLMD_CHART = "oci://ghcr.io/llm-d/charts/llm-d-router-standalone"
URLS = {
    "service": "http://sim/v1/completions",
    "epp": "http://epp/v1/completions",
    "llm-d": f"http://{LLMD_RELEASE}-epp/v1/completions",
}
DEPLOYMENTS = {"service": [], "epp": ["epp"], "llm-d": [f"{LLMD_RELEASE}-epp"]}


def sim_config(package: str) -> Any:
    """The simulators' setting for every run: a cache too small to hold every prefix."""
    manifests = importlib.import_module(f"{package}.manifests")
    return manifests.SimConfig(
        max_num_seqs=4,
        kv_cache_blocks=256,
        block_size=16,
        prefill_overhead_ms=20,
        prefill_per_token_ms=2,
        inter_token_latency_ms=10,
    )


def deploy(
    package: str,
    routers: list[str],
    replicas: int,
    epp_args: tuple[str, ...] = (),
    sim_kv_events: bool = False,
) -> None:
    manifests = importlib.import_module(f"{package}.manifests")
    discovery = importlib.import_module(f"{package}.discovery")
    deploy_mod = importlib.import_module(f"{package}.deploy")
    kube.kubectl("create", "namespace", NAMESPACE, check=False)
    sims = manifests.sim_deployment(
        namespace=NAMESPACE, replicas=replicas, config=sim_config(package)
    )
    if sim_kv_events:
        sims = importlib.import_module(f"{package}.kvevents").with_kv_events(sims)
    objects = [sims, manifests.sim_service(namespace=NAMESPACE)]
    if "epp" in routers:
        objects.append(
            discovery.inference_pool("pool", NAMESPACE, {"app": "sim"}, manifests.SIM_PORT, "epp")
        )
        objects += deploy_mod.epp_objects(
            "epp", NAMESPACE, "pool", package=package, extra_args=epp_args
        )
    kube.apply(objects)
    if "llm-d" in routers:
        if shutil.which("helm") is None:
            raise SystemExit("the llm-d router needs helm: brew install helm")
        kube.run(
            [
                "helm",
                "upgrade",
                "--install",
                LLMD_RELEASE,
                LLMD_CHART,
                "--version",
                PINS["llm-d-router"],
                "-n",
                NAMESPACE,
                "--kube-context",
                kube.CONTEXT,
                "-f",
                str(kube.ROOT / "setup" / "llm-d-router.values.yaml"),
            ]
        )


def restart(deployments: list[str]) -> None:
    for name in deployments:
        kube.kubectl("rollout", "restart", f"deployment/{name}", "-n", NAMESPACE)
    for name in deployments:
        kube.kubectl("rollout", "status", f"deployment/{name}", "-n", NAMESPACE, "--timeout=300s")


def run_one(router: str, package: str, loadgen_args: list[str]) -> dict[str, Any]:
    restart(["sim", *DEPLOYMENTS[router]])
    time.sleep(3)  # let the EPPs discover the new Pods and scrape them once
    before = kube.sim_counters(NAMESPACE)
    out = kube.run_in_cluster(
        NAMESPACE,
        ["python", "-m", f"{package}.loadgen", "--url", URLS[router], *loadgen_args],
        name=f"loadgen-{router}",
    )
    report: dict[str, Any] = json.loads(out.strip().splitlines()[-1])
    after = kube.sim_counters(NAMESPACE)
    served = {p: after[p]["served"] - before.get(p, {}).get("served", 0) for p in after}
    hits = sum(c["hit_tokens"] for c in after.values()) - sum(
        c["hit_tokens"] for c in before.values()
    )
    queries = sum(c["query_tokens"] for c in after.values()) - sum(
        c["query_tokens"] for c in before.values()
    )
    report["server_hit_rate"] = hits / queries if queries else 0.0
    report["served_per_pod"] = sorted(served.values(), reverse=True)
    return report


def run(
    package: str,
    routers: list[str],
    replicas: int,
    loadgen_args: list[str],
    epp_args: tuple[str, ...] = (),
    sim_kv_events: bool = False,
) -> dict[str, Any]:
    deploy(package, routers, replicas, epp_args, sim_kv_events)
    kube.kubectl("rollout", "status", "deployment/sim", "-n", NAMESPACE, "--timeout=300s")
    results = {}
    for router in routers:
        print(f"running {router} ...", flush=True)
        results[router] = run_one(router, package, loadgen_args)
    return {
        "package": package,
        "replicas": replicas,
        "loadgen_args": loadgen_args,
        "epp_args": list(epp_args),
        "sim_kv_events": sim_kv_events,
        "sim_config": vars(sim_config(package)),
        "pins": PINS,
        "results": results,
    }


def render(report: dict[str, Any]) -> str:
    lines = [
        f"{'router':<8} {'TTFT p50':>9} {'TTFT p99':>9} {'TPOT':>7} {'req/s':>6} "
        f"{'hit (client)':>12} {'hit (sims)':>10}  served per replica",
    ]
    for router, r in report["results"].items():
        s = r["summary"]
        lines.append(
            f"{router:<8} {s['ttft_p50'] * 1000:>7.0f}ms {s['ttft_p99'] * 1000:>7.0f}ms "
            f"{s['tpot_mean'] * 1000:>5.1f}ms {s['throughput_rps']:>6.1f} "
            f"{s['cache_hit_rate']:>12.0%} {r['server_hit_rate']:>10.0%}  "
            f"{[int(x) for x in r['served_per_pod']]}"
            + (f"  errors={s['errors']}" if s["errors"] else "")
        )
    return "\n".join(lines)


def write(report: dict[str, Any], path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, indent=2) + "\n")
    return path
