"""Structural checks on the course itself. Not an exercise."""

import ast
import importlib.util
from pathlib import Path

import pytest

from lean_llmd_course import days as syllabus

ROOT = Path(__file__).resolve().parent.parent


def _make_stubs():
    spec = importlib.util.spec_from_file_location("make_stubs", ROOT / "tools" / "make_stubs.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize("day", syllabus.DAYS, ids=lambda d: f"day{d.number}")
def test_every_day_maps_to_llm_d(day):
    assert day.upstream, f"day {day.number} names no place in llm-d"
    for ref in day.upstream:
        assert ref.repo in syllabus.PINS, f"{ref.repo} is not pinned"
        assert not ref.path.startswith("/"), f"{ref.path} is not a repo path"
        assert ref.symbols, f"{ref.path} names no symbol"
        assert ref.note.endswith("."), "the note is a sentence"


@pytest.mark.parametrize("day", syllabus.ready_days(), ids=lambda d: f"day{d.number}")
def test_a_written_day_has_all_of_its_parts(day):
    for path in (day.doc_path, day.test_path, *day.module_paths, *day.reference_paths):
        assert (ROOT / path).exists(), f"day {day.number} is missing {path}"


def _public_signatures(path: Path) -> dict[str, str]:
    found: dict[str, str] = {}

    def visit(body: list[ast.stmt], prefix: str) -> None:
        for node in body:
            if isinstance(node, ast.ClassDef):
                visit(node.body, f"{prefix}{node.name}.")
            elif isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef):
                private = node.name.startswith("_") and not node.name.endswith("__")
                if not private:
                    found[prefix + node.name] = ast.unparse(node.args)

    visit(ast.parse(path.read_text()).body, "")
    return found


@pytest.mark.parametrize("day", syllabus.ready_days(), ids=lambda d: f"day{d.number}")
def test_your_package_keeps_the_references_signatures(day):
    for module_path, reference_path in zip(day.module_paths, day.reference_paths, strict=True):
        mine = _public_signatures(ROOT / module_path)
        for name, args in _public_signatures(ROOT / reference_path).items():
            assert name in mine, f"{module_path} lost {name}; the tests call it"
            assert mine[name] == args, (
                f"{module_path}: {name}({mine[name]}) differs from the reference ({args}). "
                "Regenerate with `uv run python tools/make_stubs.py` after deleting the stub."
            )


def test_the_stub_generator_knows_every_module():
    modules = {f"{m}.py" for d in syllabus.DAYS for m in d.modules}
    known = _make_stubs().MODULES
    assert modules == set(known)
    for day in syllabus.DAYS:
        for m in day.modules:
            assert known[f"{m}.py"][0] == day.number


def test_days_are_numbered_consecutively():
    assert [d.number for d in syllabus.DAYS] == list(range(1, len(syllabus.DAYS) + 1))
    for day in syllabus.DAYS:
        for other in day.depends_on:
            assert other < day.number, f"day {day.number} depends on a later day"


def test_written_days_come_first():
    """A gap would mean an exercise that imports something not written yet."""
    ready = [d.ready for d in syllabus.DAYS]
    assert ready == sorted(ready, reverse=True)


def test_a_reference_module_imports_only_earlier_days():
    owner = {m: d.number for d in syllabus.DAYS for m in d.modules}
    for day in syllabus.DAYS:
        for m in day.modules:
            tree = ast.parse((ROOT / f"src/lean_llmd_ref/{m}.py").read_text())
            for node in ast.walk(tree):
                if isinstance(node, ast.ImportFrom) and (node.module or "").startswith(
                    "lean_llmd_ref."
                ):
                    used = node.module.split(".")[1]
                    assert owner[used] <= day.number, f"{m} (day {day.number}) imports {used}"


def test_no_test_uses_the_reference_as_its_oracle():
    """The whole point: the oracle is never lean_llmd_ref."""
    for path in sorted((ROOT / "tests").glob("test_day*.py")):
        text = path.read_text()
        assert "lean_llmd_ref" not in text, f"{path.name} names the reference"
        assert "import lean_llmd" not in text and "from lean_llmd" not in text, (
            f"{path.name} imports an implementation directly; use the mod fixture"
        )
