"""Day 9 - prefill/decode disaggregation: the EPP side.

Shaped like llm-d-router's ``disagg-profile-handler``, ``prefix-based-pd-decider``
and the ``prefill-filter`` / ``decode-filter`` role filters. Every request gets
a decode endpoint; long, uncached prompts also get a prefill endpoint, passed
to the decode Pod's sidecar in the ``x-prefiller-host-port`` header. Run it
with ``--feature disagg``.
"""

import argparse
import copy
from typing import Any

from lean_llmd_ref.datastore import Endpoint
from lean_llmd_ref.epp import Runtime
from lean_llmd_ref.prefix import (
    PREFIX_MATCH_KEY,
    PrefixCacheMatchInfo,
    estimate_tokens,
    prompt_bytes,
)
from lean_llmd_ref.scheduling import (
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
DECODE, PREFILL = "decode", "prefill"


class PrefillFilter:
    """Keep endpoints whose role can prefill. Upstream: ``prefill-filter``."""

    name = "prefill-filter"

    def filter(self, request: InferenceRequest, endpoints: list[Endpoint]) -> list[Endpoint]:
        return [e for e in endpoints if e.metadata.labels.get(ROLE_LABEL) in PREFILL_ROLES]


class DecodeFilter:
    """Keep endpoints that can decode, **including those with no role label**.

    Upstream: ``decode-filter``. An unlabelled Pod is an ordinary model
    server that does both, so a P/D pool can grow out of a plain one.
    """

    name = "decode-filter"

    def filter(self, request: InferenceRequest, endpoints: list[Endpoint]) -> list[Endpoint]:
        kept = []
        for e in endpoints:
            role = e.metadata.labels.get(ROLE_LABEL)
            if role is None or role in DECODE_ROLES:
                kept.append(e)
        return kept


class PrefixBasedPDDecider:
    """Disaggregate only when the decode endpoint would have a lot to prefill itself.

    Upstream: ``prefix-based-pd-decider``. Remote prefill costs a KV transfer
    and an extra hop; it only pays when the uncached part of the prompt is long.
    """

    name = "prefix-based-pd-decider"

    def __init__(self, non_cached_tokens: int) -> None:
        self.non_cached_tokens = non_cached_tokens

    def disaggregate(self, request: InferenceRequest, decode_endpoint: Endpoint) -> bool:
        """``True`` if the prompt's uncached tokens reach ``non_cached_tokens``.

        - A threshold of 0 turns disaggregation off.
        - Prompt length is in day 5's pseudo-tokens (``estimate_tokens``).
        - Cached tokens are the decode endpoint's ``match_blocks *
          block_size_tokens`` from its ``PrefixCacheMatchInfo``; no info
          means nothing is cached.
        """
        if self.non_cached_tokens <= 0:
            return False
        total = len(estimate_tokens(prompt_bytes(request.body)))
        if total < self.non_cached_tokens:
            return False
        info = decode_endpoint.attributes.get(PREFIX_MATCH_KEY)
        cached = 0
        if isinstance(info, PrefixCacheMatchInfo):
            cached = info.match_blocks * info.block_size_tokens
        return total - cached >= self.non_cached_tokens


class DisaggProfileHandler:
    """Decode first; prefill only if the decider asks. Upstream: ``disagg-profile-handler``."""

    name = "disagg-profile-handler"

    def __init__(self, decider: PrefixBasedPDDecider | None = None) -> None:
        self.decider = decider

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
        if DECODE not in results:
            return {DECODE: profiles[DECODE]}
        decode = results[DECODE]
        if (
            PREFILL not in results
            and PREFILL in profiles
            and decode is not None
            and decode.target_endpoints
            and self.decider is not None
            and self.decider.disaggregate(request, decode.target_endpoints[0])
        ):
            return {PREFILL: profiles[PREFILL]}
        return {}

    def process_results(
        self, request: InferenceRequest, results: dict[str, ProfileRunResult | None]
    ) -> SchedulingResult:
        """Decode is primary. If prefill chose an endpoint, put its ``ip:port`` in
        ``request.set_headers[PREFILL_ENDPOINT_HEADER]``; otherwise make sure the
        header is absent. ``ValueError`` if decode found nothing.
        """
        decode = results.get(DECODE)
        if decode is None or not decode.target_endpoints:
            raise ValueError("no decode endpoint for the request")
        request.set_headers.pop(PREFILL_ENDPOINT_HEADER, None)
        prefill = results.get(PREFILL)
        if prefill is not None and prefill.target_endpoints:
            request.set_headers[PREFILL_ENDPOINT_HEADER] = prefill.target_endpoints[
                0
            ].metadata.host_port
        return SchedulingResult(dict(results), DECODE)


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
    out = []
    for role, replicas in ((PREFILL, prefill_replicas), (DECODE, decode_replicas)):
        d = copy.deepcopy(sim_deployment)
        d["metadata"]["name"] = f"{name}-{role}"
        d["metadata"]["namespace"] = namespace
        d["spec"]["replicas"] = replicas
        d["spec"]["selector"]["matchLabels"][ROLE_LABEL] = role
        template = d["spec"]["template"]
        template["metadata"]["labels"][ROLE_LABEL] = role
        if role == DECODE:
            sim = template["spec"]["containers"][0]
            probes = {
                k: sim.pop(k)
                for k in ("startupProbe", "readinessProbe", "livenessProbe")
                if k in sim
            }
            args = list(sim["args"])
            args[args.index("--port") + 1] = "8200"
            sim["args"] = [
                *args,
                "--kv-cache-transfer-time-per-token",
                f"{transfer_ms_per_token}ms",
            ]
            sim["ports"] = [{"name": "vllm", "containerPort": 8200, "protocol": "TCP"}]
            sidecar = {
                "name": "sidecar",
                "image": sidecar_image,
                "imagePullPolicy": "IfNotPresent",
                "command": sidecar_command,
                "ports": [{"name": "http", "containerPort": 8000, "protocol": "TCP"}],
                **probes,
                "resources": {"requests": {"cpu": "50m", "memory": "64Mi"}},
            }
            template["spec"]["containers"] = [sidecar, sim]
        out.append(d)
    return out


def install(rt: Runtime, argv: list[str]) -> None:
    """The ``disagg`` feature: decode and prefill profiles behind a ``DisaggProfileHandler``.

    Both profiles reuse the default profile's scorers and picker, with
    ``DecodeFilter`` / ``PrefillFilter`` in front. Flag
    ``--pd-non-cached-tokens`` (default 256) sets the decider's threshold.
    """
    p = argparse.ArgumentParser(prog="disagg")
    p.add_argument("--pd-non-cached-tokens", type=int, default=256)
    a, _ = p.parse_known_args(argv)
    base = next(iter(rt.profiles.values()))
    rt.profiles = {
        DECODE: SchedulerProfile([DecodeFilter()], list(base.scorers), base.picker),
        PREFILL: SchedulerProfile([PrefillFilter()], list(base.scorers), base.picker),
    }
    rt.profile_handler = DisaggProfileHandler(PrefixBasedPDDecider(a.pd_non_cached_tokens))
