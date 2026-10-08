"""Regenerate src/lean_llmd/*.py stubs from the reference implementation.

Keeps every public signature and docstring (the docstring is the spec) and
replaces each body with a NotImplementedError. Private helpers (``_name``) are
dropped: how you structure the inside is yours. Names listed in a module's
``__given__`` are copied verbatim; Protocol classes are kept as they are.

    uv run python tools/make_stubs.py [--check]
"""

from __future__ import annotations

import ast
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
REF = ROOT / "src" / "lean_llmd_ref"
MINE = ROOT / "src" / "lean_llmd"

HEADER = '''"""{title}

Your implementation. Every function below raises until you write it.

    uv run lean-llmd test --day {day}         # your code
    uv run lean-llmd test --day {day} --ref   # proves the tests are passable
    uv run lean-llmd diff --day {day}         # only after you have a passing run
"""
'''

# module -> (day, one-line title)
MODULES: dict[str, tuple[int, str]] = {
    "manifests.py": (1, "Day 1 - the model server as Kubernetes objects."),
    "metrics.py": (2, "Day 2 - reading a model server's state from /metrics."),
    "datastore.py": (3, "Day 3 - the datastore: which endpoints the router may pick from."),
    "discovery.py": (3, "Day 3 - discovering endpoints: an InferencePool, its Pods, reconcilers."),
    "scheduling.py": (4, "Day 4 - the scheduling framework: filter, score, pick."),
    "plugins.py": (4, "Day 4 - load scorers, a label filter, pickers, a profile handler."),
    "prefix.py": (5, "Day 5 - approximate prefix-cache-aware routing."),
    "extproc.py": (6, "Day 6 - the ext-proc server: Envoy asks, the EPP answers."),
    "epp.py": (6, "Day 6 - the endpoint picker process."),
    "deploy.py": (6, "Day 6 - the EPP as Kubernetes objects."),
    "loadgen.py": (7, "Day 7 - a streaming load generator."),
    "kvevents.py": (8, "Day 8 - KV-cache events: what a model server cached and evicted."),
    "precise.py": (8, "Day 8 - precise prefix-cache routing from KV events."),
    "disagg.py": (9, "Day 9 - prefill/decode disaggregation: the EPP side."),
    "pd_sidecar.py": (9, "Day 9 - the P/D sidecar in each decode Pod."),
    "flowcontrol.py": (10, "Day 10 - flow control: priority, fairness, saturation."),
    "gateway.py": (11, "Day 11 - gateway mode: TLS and the full-duplex protocol."),
    "observability.py": (12, "Day 12 - EPP metrics, Prometheus and KEDA."),
    "resilience.py": (13, "Day 13 - failure modes: budgets, graceful rollouts, failing open."),
    "gpu.py": (14, "Day 14 (optional) - real vLLM on a GPU."),
}


def _private(name: str) -> bool:
    return name.startswith("_") and not (name.startswith("__") and name.endswith("__"))


def _given(tree: ast.Module) -> set[str]:
    for node in tree.body:
        if (
            isinstance(node, ast.Assign)
            and any(isinstance(t, ast.Name) and t.id == "__given__" for t in node.targets)
            and isinstance(node.value, ast.Tuple | ast.List)
        ):
            return {str(e.value) for e in node.value.elts if isinstance(e, ast.Constant)}
    return set()


def _placeholder(name: str) -> ast.stmt:
    return ast.Expr(ast.Name(id=f"__given_{name}__", ctx=ast.Load()))


class Stubber(ast.NodeTransformer):
    """Stubs public functions, drops private ones, swaps given ones for placeholders."""

    def __init__(self, day: int, given: set[str]) -> None:
        self.day = day
        self.given = given

    def visit_ClassDef(self, node: ast.ClassDef) -> ast.AST | None:
        if node.name in self.given:
            return _placeholder(node.name)
        if any(isinstance(b, ast.Name) and b.id == "Protocol" for b in node.bases):
            return node
        self.generic_visit(node)
        return node

    def _stub(self, node: ast.FunctionDef | ast.AsyncFunctionDef) -> ast.AST | None:
        if node.name in self.given:
            return _placeholder(node.name)
        if _private(node.name):
            return None
        body: list[ast.stmt] = []
        doc = ast.get_docstring(node, clean=False)
        if doc is not None:
            body.append(node.body[0])
        message = f"day {self.day}: implement {node.name}()"
        body.append(
            ast.Raise(
                exc=ast.Call(
                    func=ast.Name(id="NotImplementedError", ctx=ast.Load()),
                    args=[ast.Constant(value=message)],
                    keywords=[],
                ),
                cause=None,
            )
        )
        node.body = body
        return node

    visit_FunctionDef = _stub
    visit_AsyncFunctionDef = _stub


def _drop_private_assignments(tree: ast.Module, given: set[str]) -> None:
    kept: list[ast.stmt] = []
    for node in tree.body:
        if isinstance(node, ast.Assign | ast.AnnAssign):
            targets = node.targets if isinstance(node, ast.Assign) else [node.target]
            names = [t.id for t in targets if isinstance(t, ast.Name)]
            if names and all(_private(n) and n not in given for n in names):
                continue
        kept.append(node)
    tree.body = kept


def render(path: Path, day: int, title: str) -> str:
    text = path.read_text()
    tree = ast.parse(text)
    # Drop the reference's module docstring; the stub gets its own header.
    if (
        tree.body
        and isinstance(tree.body[0], ast.Expr)
        and isinstance(tree.body[0].value, ast.Constant)
    ):
        tree.body = tree.body[1:]
    given = _given(tree)
    # Given code keeps its original text, comments and layout included.
    originals = {
        node.name: ast.get_source_segment(text, node, padded=True) or ""
        for node in ast.walk(tree)
        if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef | ast.ClassDef)
        and node.name in given
    }
    _drop_private_assignments(tree, given)
    tree = Stubber(day, given).visit(tree)
    ast.fix_missing_locations(tree)
    source = ast.unparse(tree)
    for name, original in originals.items():
        source = source.replace(f"__given_{name}__", "\n" + original.strip("\n") + "\n")
    source = source.replace("lean_llmd_ref", "lean_llmd")
    return HEADER.format(title=title, day=day) + "\n" + source + "\n"


def main() -> int:
    check = "--check" in sys.argv
    stale: list[str] = []
    MINE.mkdir(parents=True, exist_ok=True)
    for name, (day, title) in MODULES.items():
        ref_path = REF / name
        if not ref_path.exists():
            continue
        out = MINE / name
        if check:
            if not out.exists():
                stale.append(f"{name}: missing")
            continue
        if out.exists():
            print(f"skip {name} (already written; delete it to regenerate)")
            continue
        out.write_text(render(ref_path, day, title))
        # ast.unparse produces long lines; the learner reads these files.
        subprocess.run(["ruff", "format", "--quiet", str(out)], check=False)
        subprocess.run(["ruff", "check", "--quiet", "--fix", str(out)], check=False)
        subprocess.run(["ruff", "format", "--quiet", str(out)], check=False)
        print(f"wrote {out.relative_to(ROOT)}")
    if stale:
        print("\n".join(stale))
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
