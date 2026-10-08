"""Day 9 - prefill/decode disaggregation: the EPP side.

Your implementation. Every function below raises until you write it.

    uv run lean-llmd test --day 9         # your code
    uv run lean-llmd test --day 9 --ref   # proves the tests are passable
    uv run lean-llmd diff --day 9         # only after you have a passing run
"""

import argparse
import copy
from typing import Any

from lean_llmd.datastore import Endpoint
from lean_llmd.epp import Runtime
from lean_llmd.prefix import PREFIX_MATCH_KEY, PrefixCacheMatchInfo, estimate_tokens, prompt_bytes
from lean_llmd.scheduling import (
    InferenceRequest,
    ProfileRunResult,
    SchedulerProfile,
    SchedulingResult,
)

Obj = dict[str, Any]
ROLE_LABEL = "llm-d.ai/role"
PREFILL_ENDPOINT_HEADER = "x-prefiller-host-port"
PREFILL_ROLES = frozenset({"prefill", "encode-prefill", "prefill-decode", "encode-prefill-decode"})
DECODE_ROLES = frozenset({"decode", "prefill-decode", "encode-prefill-decode"})
DECODE, PREFILL = ("decode", "prefill")


class PrefillFilter:
    """Keep endpoints whose role can prefill. Upstream: ``prefill-filter``."""

    name = "prefill-filter"

    def filter(self, request: InferenceRequest, endpoints: list[Endpoint]) -> list[Endpoint]:
        raise NotImplementedError("day 9: implement filter()")


class DecodeFilter:
    """Keep endpoints that can decode, **including those with no role label**.

    Upstream: ``decode-filter``. An unlabelled Pod is an ordinary model
    server that does both, so a P/D pool can grow out of a plain one.
    """

    name = "decode-filter"

    def filter(self, request: InferenceRequest, endpoints: list[Endpoint]) -> list[Endpoint]:
        raise NotImplementedError("day 9: implement filter()")


class PrefixBasedPDDecider:
    """Disaggregate only when the decode endpoint would have a lot to prefill itself.

    Upstream: ``prefix-based-pd-decider``. Remote prefill costs a KV transfer
    and an extra hop; it only pays when the uncached part of the prompt is long.
    """

    name = "prefix-based-pd-decider"

    def __init__(self, non_cached_tokens: int) -> None:
        raise NotImplementedError("day 9: implement __init__()")

    def disaggregate(self, request: InferenceRequest, decode_endpoint: Endpoint) -> bool:
        """``True`` if the prompt's uncached tokens reach ``non_cached_tokens``.

        - A threshold of 0 turns disaggregation off.
        - Prompt length is in day 5's pseudo-tokens (``estimate_tokens``).
        - Cached tokens are the decode endpoint's ``match_blocks *
          block_size_tokens`` from its ``PrefixCacheMatchInfo``; no info
          means nothing is cached.
        """
        raise NotImplementedError("day 9: implement disaggregate()")


class DisaggProfileHandler:
    """Decode first; prefill only if the decider asks. Upstream: ``disagg-profile-handler``."""

    name = "disagg-profile-handler"

    def __init__(self, decider: PrefixBasedPDDecider | None = None) -> None:
        raise NotImplementedError("day 9: implement __init__()")

    def pick(
        self,
        request: InferenceRequest,
        profiles: dict[str, SchedulerProfile],
        results: dict[str, ProfileRunResult | None],
    ) -> dict[str, SchedulerProfile]:
        """One profile per call.

        1. No results yet: run ``decode``.
        2. Decode ran and found an endpoint, prefill has not run, and the
           decider approves that endpoint: run ``prefill``.
        3. Otherwise: nothing (the loop ends).
        """
        raise NotImplementedError("day 9: implement pick()")

    def process_results(
        self, request: InferenceRequest, results: dict[str, ProfileRunResult | None]
    ) -> SchedulingResult:
        """Decode is primary. If prefill chose an endpoint, put its ``ip:port`` in
        ``request.set_headers[PREFILL_ENDPOINT_HEADER]``; otherwise make sure the
        header is absent. ``ValueError`` if decode found nothing.
        """
        raise NotImplementedError("day 9: implement process_results()")


def pd_deployments(
    name: str,
    namespace: str,
    sim_deployment: Obj,
    sidecar_image: str,
    sidecar_command: list[str],
    prefill_replicas: int = 1,
    decode_replicas: int = 2,
    transfer_ms_per_token: int = 1,
) -> list[Obj]:
    """Turn one simulator Deployment into a prefill and a decode Deployment.

    Both keep the template's labels (so one InferencePool selects both) plus
    ``llm-d.ai/role`` (``prefill`` / ``decode``), and get names
    ``<name>-prefill`` / ``<name>-decode``; each selector adds the role.

    - Prefill: the simulator unchanged.
    - Decode: the simulator moves to port 8200 (rename its port ``vllm``,
      drop its probes, add ``--kv-cache-transfer-time-per-token``) and a
      ``sidecar`` container takes over port 8000 (named ``http``, same probes
      the simulator had), running ``sidecar_command``. The pool's target port
      stays 8000, so traffic and metric scrapes reach the sidecar first.
    """
    raise NotImplementedError("day 9: implement pd_deployments()")


def install(rt: Runtime, argv: list[str]) -> None:
    """The ``disagg`` feature: decode and prefill profiles behind a ``DisaggProfileHandler``.

    Both profiles reuse the default profile's scorers and picker, with
    ``DecodeFilter`` / ``PrefillFilter`` in front. Flag
    ``--pd-non-cached-tokens`` (default 256) sets the decider's threshold.
    """
    raise NotImplementedError("day 9: implement install()")
