"""Swaps your package for the reference one, so both run the same tests.

uv run pytest tests/test_day01.py             # your code
uv run pytest tests/test_day01.py --impl ref  # the reference

Tests marked ``docker`` need a Docker daemon; ``kind`` ones need the
``kind-lean-llmd`` cluster (``uv run lean-llmd cluster up``). Both skip
cleanly when what they need is missing.
"""

import importlib
import os
from collections.abc import Callable
from types import ModuleType

import pytest

import k8s


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--impl",
        action="store",
        default=None,
        choices=["mine", "ref"],
        help="which package to test: your lean_llmd, or lean_llmd_ref",
    )


@pytest.fixture(scope="session")
def impl_name(request: pytest.FixtureRequest) -> str:
    chosen = request.config.getoption("--impl")
    if chosen is None:
        chosen = os.environ.get("LEAN_LLMD_IMPL", "mine")
    return str(chosen)


@pytest.fixture(scope="session")
def package(impl_name: str) -> str:
    return "lean_llmd" if impl_name == "mine" else "lean_llmd_ref"


@pytest.fixture(scope="session")
def mod(package: str) -> Callable[[str], ModuleType]:
    """Import a course module from whichever implementation is under test."""

    def load(module: str) -> ModuleType:
        return importlib.import_module(f"{package}.{module}")

    return load


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    needs_docker = any("docker" in item.keywords for item in items)
    needs_kind = any("kind" in item.keywords for item in items)
    docker_reason = k8s.docker_unavailable() if needs_docker else None
    kind_reason = k8s.kind_unavailable() if needs_kind else None
    for item in items:
        if "docker" in item.keywords and docker_reason:
            item.add_marker(pytest.mark.skip(reason=docker_reason))
        if "kind" in item.keywords and kind_reason:
            item.add_marker(pytest.mark.skip(reason=kind_reason))
