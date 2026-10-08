"""``lean-llmd``: run a day's tests, compare with the reference, map to llm-d, run the cluster."""

import argparse
import difflib
import importlib
import os
import shlex
import subprocess
import sys
from pathlib import Path
from typing import Any

import yaml

from lean_llmd_course import bench, kube
from lean_llmd_course import days as syllabus
from lean_llmd_course.days import (
    AGENTGATEWAY_VERSION,
    GAIE_VERSION,
    GATEWAY_API_VERSION,
    KEDA_CHART_VERSION,
    PINS,
    PROMETHEUS_CHART_VERSION,
    Day,
)

ROOT = kube.ROOT


def _pytest(day: Day, impl: str, extra: list[str]) -> int:
    test_path = ROOT / day.test_path
    if not test_path.exists():
        print(f"day {day.number} has no tests yet ({day.test_path})")
        return 1
    cmd = [sys.executable, "-m", "pytest", str(test_path), "--impl", impl, *extra]
    return subprocess.call(cmd, cwd=ROOT)


def cmd_test(args: argparse.Namespace) -> int:
    return _pytest(syllabus.get(args.day), "ref" if args.ref else "mine", args.pytest_args)


def cmd_status(args: argparse.Namespace) -> int:
    print("pins: " + ", ".join(f"{repo} {tag}" for repo, tag in PINS.items()) + "\n")
    width = max(len(d.title) for d in syllabus.DAYS)
    for day in syllabus.DAYS:
        if not day.ready:
            state = "not written yet"
        else:
            code = subprocess.call(
                [
                    sys.executable,
                    "-m",
                    "pytest",
                    str(ROOT / day.test_path),
                    "--impl",
                    "mine",
                    "-q",
                    "-x",
                    "--no-header",
                    "-p",
                    "no:cacheprovider",
                    "-m",
                    "not kind",
                ],
                cwd=ROOT,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            state = "pass" if code == 0 else "todo"
        print(f"  day {day.number}  {day.title:<{width}}  {state}")
    print("\n(status skips the kind tests; run `lean-llmd test --day N` for the full set)")
    return 0


SECTION_TITLES = {
    "core": "Core: a working mini llm-d",
    "extension": "Extensions",
    "optional": "Optional (needs a GPU)",
}


def cmd_list(args: argparse.Namespace) -> int:
    current = ""
    for day in syllabus.DAYS:
        if syllabus.section(day) != current:
            current = syllabus.section(day)
            print(f"\n{SECTION_TITLES[current]}")
        mark = " " if day.ready else "."
        print(f" {mark} {day.number}. {day.title}")
        if args.verbose:
            print(f"     {day.concept}")
            print(f"     you write: {', '.join(day.module_paths)}\n")
    print(f"\nthe core ships: {syllabus.DELIVERABLE}")
    for number in syllabus.NUMBERS:
        print(f"  number: {number}")
    return 0


def cmd_diff(args: argparse.Namespace) -> int:
    day = syllabus.get(args.day)
    if not args.anyway:
        code = _pytest(day, "mine", ["-q", "--no-header", "-p", "no:cacheprovider"])
        if code != 0:
            print(
                f"\nDay {day.number} does not pass yet. The comparison is worth much more after "
                "it does: you only get one first look at the reference.\n"
                f"If you are sure: lean-llmd diff --day {day.number} --anyway"
            )
            return code
    for module_path, reference_path in zip(day.module_paths, day.reference_paths, strict=True):
        if args.full:
            sys.stdout.write((ROOT / reference_path).read_text())
            continue
        mine = (ROOT / module_path).read_text().splitlines(keepends=True)
        ref = (ROOT / reference_path).read_text().splitlines(keepends=True)
        sys.stdout.writelines(
            difflib.unified_diff(
                mine, ref, fromfile=module_path, tofile=reference_path, n=args.context
            )
        )
    return 0


def _verify(source_dir: str | None, repo: str, path: str, symbol: str) -> str:
    if not source_dir:
        return ""
    target = Path(source_dir) / repo / path
    if not target.exists():
        return "  MISSING"
    return "" if symbol in target.read_text() else "  MISSING"


def cmd_map(args: argparse.Namespace) -> int:
    day = syllabus.get(args.day)
    source_dir = os.environ.get("LLMD_SOURCE_DIR")
    print(f"Day {day.number} - {day.title}\n")
    failures = 0
    for ref in day.upstream:
        print(f"  {ref.repo} {PINS[ref.repo]}  {ref.path}")
        print(f"    {ref.note}")
        for symbol in ref.symbols:
            status = _verify(source_dir, ref.repo, ref.path, symbol) if args.verify else ""
            failures += status == "  MISSING"
            print(f"      {symbol}{status}")
        print(f"    {ref.url()}\n")
    if args.verify and not source_dir:
        print("set LLMD_SOURCE_DIR to a directory holding llm-d, llm-d-router and")
        print("llm-d-inference-sim checkouts at the pins (see setup/README.md)")
        return 1
    return 1 if failures else 0


def _objects(what: str, package: str) -> list[dict[str, Any]]:
    manifests = importlib.import_module(f"{package}.manifests")
    if what == "sim":
        return [manifests.namespace(), manifests.sim_deployment(), manifests.sim_service()]
    discovery = importlib.import_module(f"{package}.discovery")
    if what == "pool":
        return [
            discovery.inference_pool(
                "pool", manifests.NAMESPACE, {"app": "sim"}, manifests.SIM_PORT, "epp"
            )
        ]
    deploy = importlib.import_module(f"{package}.deploy")
    objects: list[dict[str, Any]] = deploy.epp_objects(
        "epp", manifests.NAMESPACE, "pool", package=package
    )
    return objects


def cmd_render(args: argparse.Namespace) -> int:
    package = "lean_llmd_ref" if args.ref else "lean_llmd"
    try:
        objects = _objects(args.what, package)
    except NotImplementedError as unwritten:
        print(f"not written yet: {unwritten}", file=sys.stderr)
        return 1
    sys.stdout.write(yaml.safe_dump_all(objects, sort_keys=False))
    return 0


def cmd_cluster(args: argparse.Namespace) -> int:
    if args.action == "down":
        subprocess.call(["kind", "delete", "cluster", "--name", kube.CLUSTER])
        return 0
    clusters = kube.run(["kind", "get", "clusters"]).split()
    if kube.CLUSTER not in clusters:
        subprocess.check_call(
            ["kind", "create", "cluster", "--config", str(ROOT / "setup" / "kind.yaml")]
        )
    kube.kubectl("apply", "-f", kube.GAIE_CRDS.format(version=GAIE_VERSION))
    kube.kubectl("apply", "-f", kube.OBJECTIVE_CRD.format(version=PINS["llm-d-router"]))
    kube.kubectl(
        "apply", "--server-side", "-f", kube.GATEWAY_API_CRDS.format(version=GATEWAY_API_VERSION)
    )
    print(
        f"\ncluster {kube.CLUSTER} is up (context {kube.CONTEXT}); CRDs installed: "
        "InferencePool, InferenceObjective, Gateway API"
    )
    return 0


def _helm(*args: str) -> None:
    subprocess.check_call(["helm", *args, "--kube-context", kube.CONTEXT])


def cmd_addons(args: argparse.Namespace) -> int:
    if "gateway" in args.names:
        chart = "oci://cr.agentgateway.dev/charts/agentgateway"
        for release, ref, extra in (
            ("agentgateway-crds", f"{chart}-crds", []),
            ("agentgateway", chart, ["--set", "inferenceExtension.enabled=true"]),
        ):
            _helm(
                "upgrade",
                "--install",
                release,
                ref,
                "--version",
                AGENTGATEWAY_VERSION,
                "-n",
                "agentgateway-system",
                "--create-namespace",
                *extra,
            )
        print(f"agentgateway {AGENTGATEWAY_VERSION} installed (GatewayClass agentgateway)")
    if "monitoring" in args.names:
        for name, url in (
            ("prometheus-community", "https://prometheus-community.github.io/helm-charts"),
            ("kedacore", "https://kedacore.github.io/charts"),
        ):
            subprocess.call(["helm", "repo", "add", name, url], stdout=subprocess.DEVNULL)
        subprocess.check_call(["helm", "repo", "update"], stdout=subprocess.DEVNULL)
        _helm(
            "upgrade",
            "--install",
            "prometheus",
            "prometheus-community/prometheus",
            "--version",
            PROMETHEUS_CHART_VERSION,
            "-n",
            "monitoring",
            "--create-namespace",
            "-f",
            str(ROOT / "setup" / "prometheus.values.yaml"),
        )
        _helm(
            "upgrade",
            "--install",
            "keda",
            "kedacore/keda",
            "--version",
            KEDA_CHART_VERSION,
            "-n",
            "keda",
            "--create-namespace",
        )
        print("Prometheus (namespace monitoring) and KEDA (namespace keda) installed")
    return 0


def cmd_image(args: argparse.Namespace) -> int:
    subprocess.check_call(["docker", "build", "-t", kube.COURSE_IMAGE, str(ROOT)])
    subprocess.check_call(
        ["kind", "load", "docker-image", kube.COURSE_IMAGE, "--name", kube.CLUSTER]
    )
    print(f"\n{kube.COURSE_IMAGE} built from your working tree and loaded into kind")
    return 0


def cmd_bench(args: argparse.Namespace) -> int:
    package = "lean_llmd_ref" if args.ref else "lean_llmd"
    routers = args.routers.split(",")
    unknown = set(routers) - set(bench.URLS)
    if unknown:
        print(f"unknown routers {sorted(unknown)}; choose from {sorted(bench.URLS)}")
        return 1
    if not kube.image_present_in_kind():
        print("the course image is not in kind; run: uv run lean-llmd image")
        return 1
    loadgen_args = [
        "--groups",
        str(args.groups),
        "--requests-per-group",
        str(args.requests_per_group),
        "--prefix-words",
        str(args.prefix_words),
        "--concurrency",
        str(args.concurrency),
    ]
    report = bench.run(
        package,
        routers,
        args.replicas,
        loadgen_args,
        tuple(shlex.split(args.epp_args)),
        args.sim_kv_events,
    )
    print()
    print(bench.render(report))
    path = bench.write(report, ROOT / args.output)
    print(f"\nwrote {path.relative_to(ROOT)}")
    print("All latencies come from the simulator's latency model, not a GPU.")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="lean-llmd", description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("list", help="the syllabus")
    p.add_argument("-v", "--verbose", action="store_true")
    p.set_defaults(func=cmd_list)

    p = sub.add_parser("status", help="which days pass (without the kind tests)")
    p.set_defaults(func=cmd_status)

    p = sub.add_parser("test", help="run one day's tests")
    p.add_argument("--day", type=int, required=True)
    p.add_argument("--ref", action="store_true", help="test the reference instead of yours")
    p.add_argument("pytest_args", nargs="*", help="extra arguments for pytest (after --)")
    p.set_defaults(func=cmd_test)

    p = sub.add_parser("diff", help="compare your day with the reference")
    p.add_argument("--day", type=int, required=True)
    p.add_argument("--full", action="store_true", help="print the reference instead of a diff")
    p.add_argument("--context", type=int, default=3)
    p.add_argument("--anyway", action="store_true", help="skip the passing-tests check")
    p.set_defaults(func=cmd_diff)

    p = sub.add_parser("map", help="where today's concept lives in llm-d")
    p.add_argument("--day", type=int, required=True)
    p.add_argument("--verify", action="store_true", help="check paths against $LLMD_SOURCE_DIR")
    p.set_defaults(func=cmd_map)

    p = sub.add_parser("render", help="print Kubernetes objects as YAML for kubectl apply -f -")
    p.add_argument("what", choices=["sim", "pool", "epp"])
    p.add_argument("--ref", action="store_true", help="render the reference's objects")
    p.set_defaults(func=cmd_render)

    p = sub.add_parser("cluster", help="create or delete the kind cluster")
    p.add_argument("action", choices=["up", "down"])
    p.set_defaults(func=cmd_cluster)

    p = sub.add_parser("addons", help="install agentgateway (day 11), Prometheus + KEDA (day 12)")
    p.add_argument("names", nargs="+", choices=["gateway", "monitoring"])
    p.set_defaults(func=cmd_addons)

    p = sub.add_parser("image", help="build the course image and load it into kind")
    p.set_defaults(func=cmd_image)

    p = sub.add_parser("bench", help="day 7: Service vs your EPP vs llm-d's EPP")
    p.add_argument("--ref", action="store_true", help="run the reference EPP and load generator")
    p.add_argument("--routers", default="service,epp,llm-d")
    p.add_argument("--replicas", type=int, default=3)
    p.add_argument("--groups", type=int, default=8)
    p.add_argument("--requests-per-group", type=int, default=25)
    p.add_argument("--prefix-words", type=int, default=600)
    p.add_argument("--concurrency", type=int, default=8)
    p.add_argument("--epp-args", default="", help='extra EPP flags, e.g. "--feature precise"')
    p.add_argument("--sim-kv-events", action="store_true", help="simulators publish KV events")
    p.add_argument("--output", default="results/bench.json")
    p.set_defaults(func=cmd_bench)

    args = parser.parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
