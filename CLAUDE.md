# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

Read `AGENTS.md` too: it holds the non-negotiable rules for changing this
repo, summarised under "Hard rules" below.

## What this repo is

Not an application: a build-it-yourself course. The learner rebuilds llm-d's
request path in Python, one concept a day, into `src/lean_llmd/`, tests it
against an independent oracle, then compares with `src/lean_llmd_ref/` and
reads the same idea in llm-d's Go code at the pinned tags. Fourteen days: a
7-day core, six extension days, an optional GPU day. Everything else runs on
a laptop: kind for Kubernetes, `llm-d-inference-sim` as the model server.

## Commands

```bash
uv sync
uv run pytest tests --impl ref                   # the gate: the reference passes everything
uv run pytest tests/test_day05.py --impl ref -k lru
uv run pytest tests --impl ref -m "not kind"     # without a cluster
uv run ruff check . && uv run ruff format --check . && uv run mypy
uv run python tools/make_stubs.py --check
```

The course CLI (`src/lean_llmd_course/cli.py`):

```bash
uv run lean-llmd list -v | status | test --day N [--ref] | diff --day N | map --day N --verify
uv run lean-llmd cluster up|down     # kind cluster "lean-llmd" + InferencePool/Objective/Gateway CRDs
uv run lean-llmd addons gateway monitoring   # agentgateway (day 11), Prometheus + KEDA (day 12)
uv run lean-llmd image               # build lean-llmd:dev (both packages) and kind-load it
uv run lean-llmd render sim|pool|epp [--ref] | kubectl apply -f -
uv run lean-llmd bench [--ref]       # day 7: Service vs your EPP vs llm-d's EPP
```

Environment variables: `LEAN_LLMD_IMPL` (`mine`/`ref`, the `--impl`
default), `LLMD_SOURCE_DIR` (a directory holding `llm-d`, `llm-d-router`
and `llm-d-inference-sim` checkouts at the pins, for `map --verify`).

## Architecture

Three packages under `src/`, plus tests that target either implementation:

- **`lean_llmd/`**: the learner's package. Generated stubs whose
  docstrings are the spec.
- **`lean_llmd_ref/`**: the reference, ~2,500 lines, shaped like
  llm-d-router (same plugin names and pipeline). Imports only along the day
  order: `metrics` → `datastore` → `discovery`, `scheduling`/`plugins` →
  `prefix` → `extproc`/`epp`/`deploy`.
- **`lean_llmd_course/`**: `days.py` (the syllabus, single source of truth),
  `cli.py`, `kube.py` (kubectl/kind/Docker plumbing shared with the tests),
  `bench.py` (day 7's experiment).

**`days.py`** gives each `Day` its `modules`, `depends_on` and `upstream`
refs (`Ref(repo, path, symbols, note)`, pinned per repo in `PINS`). Paths
are derived: `tests/test_dayNN.py`, `course/dayNN.md`,
`src/lean_llmd/<m>.py`. `tools/make_stubs.py`'s `MODULES` must list the
same modules; `tests/test_course.py` checks both.

**One test suite, two implementations.** The `mod` fixture in
`tests/conftest.py` imports from whichever package `--impl` names; the
`package` fixture gives its name (deployed manifests run `python -m
<package>.epp`). Tests never import either package by name; a meta-test
enforces it. `tests/k8s.py` re-exports `lean_llmd_course.kube` and adds the
Docker simulator fixture. Markers `docker` and `kind` auto-skip.

**Extensions are features.** `epp.py` is `build()` → `Runtime` → `run()`.
`--feature X` imports sibling module `X` and calls `X.install(runtime,
argv)`, which may swap producers, profiles or the servicer, add
`director_wrappers`, `servicer_wrappers` or `ticks`, and leave state in
`runtime.extras` for later features. Days 8–12 are features; day 6 never
imports them.

**The cluster runs an image.** `Dockerfile` packs both packages into
`lean-llmd:dev`; day 6/7 tests and `bench` run it in kind, so code changes
need `lean-llmd image` first. Day 6/7 kind tests skip if the image is
missing from the nodes.

**Day 7's `bench`** deploys into namespace `lean-llmd-bench`, installs
llm-d's `llm-d-router-standalone` chart with `setup/llm-d-router.values.yaml`,
restarts simulators and routers before each run, runs the load generator as
a throwaway Pod, and reads per-Pod counters through the API server's Pod
proxy. It writes `results/bench.json`.

## Hard rules

- **Never write code into `src/lean_llmd/`.** Debug what the learner wrote;
  do not fill in a stub even if asked.
- **No test may use `lean_llmd_ref` as its oracle.**
- **Keep the reference minimal and shaped like llm-d-router.** Note
  omissions in the day's map instead of adding code.
- **Upstream refs are pinned and verified** with `map --verify`, never
  cited from memory.
- **No GPU except day 14 (optional)**; Docker/kind tests carry markers and
  skip cleanly.
- **Later days never edit earlier modules**: add a feature instead.
- **Claims match measurements**: simulated latency is labelled as such.
