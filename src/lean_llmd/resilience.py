"""Day 13 - failure modes: budgets, graceful rollouts, failing open.

Your implementation. Every function below raises until you write it.

    uv run lean-llmd test --day 13         # your code
    uv run lean-llmd test --day 13 --ref   # proves the tests are passable
    uv run lean-llmd diff --day 13         # only after you have a passing run
"""

import copy
from typing import Any

Obj = dict[str, Any]
DESTINATION_HEADER = "x-gateway-destination-endpoint"


def pod_disruption_budget(
    name: str, namespace: str, selector: dict[str, str], min_available: int
) -> Obj:
    """A ``policy/v1`` PodDisruptionBudget keeping ``min_available`` selected Pods up.

    It limits *voluntary* disruptions (drains, evictions); it cannot stop a
    node from dying.
    """
    raise NotImplementedError("day 13: implement pod_disruption_budget()")


def zero_downtime(deployment: Obj, pre_stop_seconds: int = 5) -> Obj:
    """A copy of a Deployment that rolls without dropping requests.

    - ``strategy``: ``RollingUpdate`` with ``maxSurge: 1`` and
      ``maxUnavailable: 0`` (a new Pod is Ready before an old one goes).
    - The first container gets ``lifecycle.preStop.sleep.seconds =
      pre_stop_seconds``: after deletion starts, the Pod keeps serving while
      the EPP (and every Service) notices it is terminating.
    - ``terminationGracePeriodSeconds`` is ``pre_stop_seconds + 10``, so
      in-flight requests can finish after the sleep.

    Do not modify the input.
    """
    raise NotImplementedError("day 13: implement zero_downtime()")


def fail_open(envoy_config: Obj, fallback_host: str, fallback_port: int) -> Obj:
    """A copy of day 6's Envoy config that serves requests even when the EPP is down.

    - The ext_proc filter gets ``failure_mode_allow: true``: if the EPP
      cannot be reached, the request continues without a destination header.
    - The proxy route list becomes two routes: first, requests that *have*
      the ``x-gateway-destination-endpoint`` header (``present_match``) go to
      ``original_destination_cluster``, keeping the first route's timeout;
      then everything else goes to cluster ``fallback``.
    - Cluster ``fallback``: ``STRICT_DNS``, ``ROUND_ROBIN``, one endpoint at
      ``fallback_host:fallback_port`` (the model servers' plain Service).

    This is where ``clear_route_cache`` earns its keep: Envoy picks a route
    before the EPP answers, so only a cleared cache lets the header select
    the first route. Do not modify the input.
    """
    raise NotImplementedError("day 13: implement fail_open()")
