"""Day 6 - the endpoint picker process.

Your implementation. Every function below raises until you write it.

    uv run lean-llmd test --day 6         # your code
    uv run lean-llmd test --day 6 --ref   # proves the tests are passable
    uv run lean-llmd diff --day 6         # only after you have a passing run
"""

import argparse
import importlib
import signal
import sys
import threading
import time
from collections.abc import Callable
from concurrent import futures
from dataclasses import dataclass, field
from typing import Any

import grpc
import httpx
from envoy.service.ext_proc.v3 import external_processor_pb2_grpc as pb_grpc
from grpc_health.v1 import health, health_pb2, health_pb2_grpc

from lean_llmd.datastore import Datastore
from lean_llmd.discovery import Controller, KubeClient
from lean_llmd.extproc import Director, Producer, RequestHandler, StreamingServer
from lean_llmd.metrics import VLLM_MAPPING, extract, scrape
from lean_llmd.plugins import (
    KVCacheUtilizationScorer,
    MaxScorePicker,
    QueueScorer,
    SingleProfileHandler,
)
from lean_llmd.prefix import ApproxPrefixCacheProducer, PrefixCacheScorer
from lean_llmd.scheduling import ProfileHandler, Scheduler, SchedulerProfile, WeightedScorer

EXT_PROC_SERVICE = "envoy.service.ext_proc.v3.ExternalProcessor"


@dataclass(frozen=True)
class Options:
    pool_name: str
    pool_namespace: str
    grpc_port: int = 9002
    health_port: int = 9003
    refresh_interval: float = 0.05
    queue_weight: float = 1.0
    kv_cache_weight: float = 1.0
    prefix_weight: float = 1.0
    kube_context: str | None = None
    features: tuple[str, ...] = ()
    feature_args: tuple[str, ...] = ()


def parse_options(argv: list[str] | None = None) -> Options:
    """Command-line flags into ``Options``. Pool name and namespace are required.

    ``--feature NAME`` may repeat. Flags this parser does not know are kept in
    ``feature_args`` for the features to parse.
    """
    raise NotImplementedError("day 6: implement parse_options()")


def build_scheduler(
    opts: Options,
) -> tuple[Scheduler, list[Producer], ApproxPrefixCacheProducer | None]:
    """llm-d's default profile: queue, KV-cache and prefix scorers, max-score picker.

    A scorer whose weight is 0 is left out, and the prefix producer with it.
    Return the scheduler, the producers, and the prefix producer (or None).
    """
    raise NotImplementedError("day 6: implement build_scheduler()")


def refresh_metrics_once(datastore: Datastore, client: httpx.Client) -> int:
    """Scrape every endpoint's ``/metrics`` once and swap in fresh ``Metrics``.

    A scrape that fails leaves the endpoint's last metrics in place. Return
    how many endpoints were refreshed.
    """
    raise NotImplementedError("day 6: implement refresh_metrics_once()")


def forget_removed_endpoints(datastore: Datastore, prefix: ApproxPrefixCacheProducer) -> None:
    """Drop index entries for servers that are no longer in the datastore."""
    raise NotImplementedError("day 6: implement forget_removed_endpoints()")


@dataclass
class Runtime:
    """Everything a running EPP is made of. Features change it before ``run``.

    The director is built in ``run`` from ``profile_handler``, ``profiles``
    and ``producers``, then passed through every ``director_wrappers`` entry
    in order. The gRPC servicer is ``servicer_factory(director)`` passed
    through every ``servicer_wrappers`` entry. ``ticks`` run once per refresh
    interval. ``extras`` is where
    features leave things for later features (day 12 reads day 10's queue).
    """

    opts: Options
    datastore: Datastore
    profile_handler: ProfileHandler
    profiles: dict[str, SchedulerProfile]
    producers: list[Producer]
    prefix: ApproxPrefixCacheProducer | None
    stop: threading.Event = field(default_factory=threading.Event)
    director_wrappers: list[Callable[[RequestHandler], RequestHandler]] = field(
        default_factory=list
    )
    servicer_factory: Callable[[RequestHandler], Any] = StreamingServer
    servicer_wrappers: list[Callable[[Any, RequestHandler], Any]] = field(default_factory=list)
    server_credentials: grpc.ServerCredentials | None = None
    ticks: list[Callable[[], None]] = field(default_factory=list)
    extras: dict[str, Any] = field(default_factory=dict)


def build(opts: Options) -> Runtime:
    """The default EPP, then every feature in ``opts.features`` installed in order.

    A feature named ``x`` is the module ``x`` next to this one; call its
    ``install(runtime, list(opts.feature_args))``.
    """
    raise NotImplementedError("day 6: implement build()")


def serve(rt: Runtime, servicer: Any) -> tuple[grpc.Server, health.HealthServicer]:
    """Start the gRPC server: ``servicer`` plus the standard health service (NOT_SERVING at first).

    Serve TLS when ``rt.server_credentials`` is set, plaintext otherwise.
    With TLS, also listen in plaintext on ``health_port``: the kubelet's gRPC
    probe cannot do TLS.
    """
    raise NotImplementedError("day 6: implement serve()")


def run(rt: Runtime) -> int:
    """Start discovery and the gRPC server, then tick until ``rt.stop`` is set.

    Each tick refreshes metrics, forgets removed endpoints in the prefix
    index, runs ``rt.ticks``, and reports SERVING only once the pool has
    synced, so Kubernetes sends traffic only to an EPP that knows its
    endpoints.
    """
    raise NotImplementedError("day 6: implement run()")


def main(argv: list[str] | None = None) -> int:
    """Parse, build, run; SIGTERM or SIGINT stops it."""
    raise NotImplementedError("day 6: implement main()")


if __name__ == "__main__":
    from lean_llmd.epp import main as module_main

    sys.exit(module_main())
