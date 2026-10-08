# Agent instructions

This is a build-it-yourself course. It rebuilds llm-d's request path
(discovery, metrics, scheduling, prefix routing, the ext-proc endpoint
picker) in Python on a kind cluster, with `llm-d-inference-sim` as the model
server, pinned against llm-d `v0.10.0` / llm-d-router `v0.11.0` /
llm-d-inference-sim `v0.12.0`.

The learner implements each day into `src/lean_llmd/`, checks it, then
compares with `src/lean_llmd_ref/` and with real llm-d. These rules are not
negotiable.

- **Never write code into `src/lean_llmd/`.** That is the learner's package.
  Answer questions about it, debug what they wrote, point at the failing
  assertion, but do not fill in a stub, even if asked to "just show me". If
  they want the answer, `lean-llmd diff --day N --anyway` is theirs to run.
- **The tests must not use `lean_llmd_ref` as the oracle.** Every test
  compares against something independent: the Kubernetes API server and its
  controllers (validation, EndpointSlices, the authorizer), Envoy's own
  protobufs and a real Envoy, `prometheus_client`, Python's `statistics`,
  hand-derived values, or a live simulator's counters. Both packages run
  the same tests via `--impl`.
- **The reference stays minimal and shaped like llm-d-router.** Same plugin
  names, pipeline, constants and header names, so the diff against the Go
  code is about scale, not structure. Leave out what only exists for LoRA,
  multimodal, multi-cluster, tracing or flow control, and say so in the
  day's map. No dependency on anything a later day introduces.
- **Upstream references are pinned and verified.** Every path and symbol in
  `src/lean_llmd_course/days.py` must resolve at the pins; check with
  `lean-llmd map --day N --verify` against `$LLMD_SOURCE_DIR`. Do not cite a
  path from memory: llm-d moves code between repositories (the inference
  scheduler, routing sidecar and KV-cache indexer all live in llm-d-router
  now).
- **Claims match measurements.** Latency on the simulator comes from its
  latency model. Day 7 may claim hit rates, load spread and simulated
  latency, never real GPU performance.
- **Everything runs on a laptop.** Only day 14 (optional) involves a GPU,
  and even its tests run on kind. Tests needing Docker or kind carry the
  `docker` or `kind` marker and must skip cleanly without them (and without
  the add-ons days 11 and 12 need).
- **Later days never edit earlier modules.** An extension is a module with
  `install(runtime, argv)` that the EPP loads with `--feature <module>`.
- **Every day ends in questions the learner answers themselves**, with a
  number, a quote or a file as the answer.

Also:

- **Signatures come from the reference.** `tools/make_stubs.py` regenerates
  stubs from `src/lean_llmd_ref/*.py`: public signatures and docstrings
  kept, bodies replaced by `NotImplementedError`, private helpers dropped,
  names in a module's `__given__` copied verbatim. It refuses to overwrite a
  stub that exists; after changing a reference signature, delete the stub,
  regenerate, and say so.
- **The cluster runs an image.** Changes to code that runs in the cluster
  need `lean-llmd image` before kind tests see them.

After changes:

```bash
uv run pytest tests --impl ref
uv run ruff check .
uv run ruff format --check .
uv run mypy
uv run python tools/make_stubs.py --check
```

After changing the reference, the EPP or the benchmark, also check the
end-to-end path still works:

```bash
uv run lean-llmd image && uv run lean-llmd bench --ref
```
