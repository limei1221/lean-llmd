"""Day 13 - failure modes: disruption budgets, graceful rollouts, failing open.

Three ways an inference service loses requests that have nothing to do with
the model: a node drain takes too many replicas at once, a rollout kills
replicas mid-request, and the endpoint picker itself goes down. Each has a
Kubernetes or Envoy answer, and each is a dict transform here.
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
    return {
        "apiVersion": "policy/v1",
        "kind": "PodDisruptionBudget",
        "metadata": {"name": name, "namespace": namespace},
        "spec": {"minAvailable": min_available, "selector": {"matchLabels": dict(selector)}},
    }


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
    out = copy.deepcopy(deployment)
    out["spec"]["strategy"] = {
        "type": "RollingUpdate",
        "rollingUpdate": {"maxSurge": 1, "maxUnavailable": 0},
    }
    pod = out["spec"]["template"]["spec"]
    pod["containers"][0]["lifecycle"] = {"preStop": {"sleep": {"seconds": pre_stop_seconds}}}
    pod["terminationGracePeriodSeconds"] = pre_stop_seconds + 10
    return out


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
    out = copy.deepcopy(envoy_config)
    listener = next(x for x in out["static_resources"]["listeners"] if x["name"] == "proxy")
    manager = listener["filter_chains"][0]["filters"][0]["typed_config"]
    for f in manager["http_filters"]:
        if f["name"] == "envoy.filters.http.ext_proc":
            f["typed_config"]["failure_mode_allow"] = True
    host = manager["route_config"]["virtual_hosts"][0]
    timeout = host["routes"][0]["route"].get("timeout", "300s")
    host["routes"] = [
        {
            "match": {
                "prefix": "/",
                "headers": [{"name": DESTINATION_HEADER, "present_match": True}],
            },
            "route": {"cluster": "original_destination_cluster", "timeout": timeout},
        },
        {"match": {"prefix": "/"}, "route": {"cluster": "fallback", "timeout": timeout}},
    ]
    out["static_resources"]["clusters"].append(
        {
            "name": "fallback",
            "type": "STRICT_DNS",
            "lb_policy": "ROUND_ROBIN",
            "connect_timeout": "5s",
            "load_assignment": {
                "cluster_name": "fallback",
                "endpoints": [
                    {
                        "lb_endpoints": [
                            {
                                "endpoint": {
                                    "address": {
                                        "socket_address": {
                                            "address": fallback_host,
                                            "port_value": fallback_port,
                                        }
                                    }
                                }
                            }
                        ]
                    }
                ],
            },
        }
    )
    return out
