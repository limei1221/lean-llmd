"""Day 6 - the endpoint picker process: discovery, metrics, scheduler, gRPC.

Shaped like llm-d-router's ``cmd/epp/runner``. Run it as
``python -m lean_llmd_ref.epp --pool-name pool --pool-namespace lean-llmd``.

Later days add *features* without editing this file: ``--feature precise``
imports the sibling module ``precise`` and calls its ``install(runtime,
argv)`` before the EPP starts, the way upstream registers plugins by name.
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

from lean_llmd_ref.datastore import Datastore
from lean_llmd_ref.discovery import Controller, KubeClient
from lean_llmd_ref.extproc import Director, Producer, RequestHandler, StreamingServer
from lean_llmd_ref.metrics import VLLM_MAPPING, extract, scrape
from lean_llmd_ref.plugins import (
    KVCacheUtilizationScorer,
    MaxScorePicker,
    QueueScorer,
    SingleProfileHandler,
)
from lean_llmd_ref.prefix import ApproxPrefixCacheProducer, PrefixCacheScorer
from lean_llmd_ref.scheduling import (
    ProfileHandler,
    Scheduler,
    SchedulerProfile,
    WeightedScorer,
)

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
    p = argparse.ArgumentParser(prog="epp", description=__doc__)
    p.add_argument("--pool-name", required=True)
    p.add_argument("--pool-namespace", required=True)
    p.add_argument("--grpc-port", type=int, default=9002)
    p.add_argument("--refresh-interval", type=float, default=0.05)
    p.add_argument("--queue-weight", type=float, default=1.0)
    p.add_argument("--kv-cache-weight", type=float, default=1.0)
    p.add_argument("--prefix-weight", type=float, default=1.0)
    p.add_argument("--kube-context", default=None)
    p.add_argument("--feature", action="append", default=[])
    a, rest = p.parse_known_args(argv)
    return Options(
        pool_name=a.pool_name,
        pool_namespace=a.pool_namespace,
        grpc_port=a.grpc_port,
        refresh_interval=a.refresh_interval,
        queue_weight=a.queue_weight,
        kv_cache_weight=a.kv_cache_weight,
        prefix_weight=a.prefix_weight,
        kube_context=a.kube_context,
        features=tuple(a.feature),
        feature_args=tuple(rest),
    )


def build_scheduler(
    opts: Options,
) -> tuple[Scheduler, list[Producer], ApproxPrefixCacheProducer | None]:
    """llm-d's default profile: queue, KV-cache and prefix scorers, max-score picker.

    A scorer whose weight is 0 is left out, and the prefix producer with it.
    Return the scheduler, the producers, and the prefix producer (or None).
    """
    scorers: list[WeightedScorer] = []
    producers: list[Producer] = []
    prefix: ApproxPrefixCacheProducer | None = None
    if opts.queue_weight:
        scorers.append(WeightedScorer(QueueScorer(), opts.queue_weight))
    if opts.kv_cache_weight:
        scorers.append(WeightedScorer(KVCacheUtilizationScorer(), opts.kv_cache_weight))
    if opts.prefix_weight:
        prefix = ApproxPrefixCacheProducer()
        producers.append(prefix)
        scorers.append(WeightedScorer(PrefixCacheScorer(), opts.prefix_weight))
    profile = SchedulerProfile(scorers=scorers, picker=MaxScorePicker())
    return Scheduler(SingleProfileHandler(), {"default": profile}), producers, prefix


def refresh_metrics_once(datastore: Datastore, client: httpx.Client) -> int:
    """Scrape every endpoint's ``/metrics`` once and swap in fresh ``Metrics``.

    A scrape that fails leaves the endpoint's last metrics in place. Return
    how many endpoints were refreshed.
    """
    refreshed = 0
    for endpoint in datastore.pod_list():
        url = f"http://{endpoint.metadata.host_port}/metrics"
        try:
            families = scrape(url, client, timeout=0.5)
        except (httpx.HTTPError, ValueError):
            continue
        metrics, _ = extract(families, VLLM_MAPPING, endpoint.metrics, time.time())
        endpoint.update_metrics(metrics)
        refreshed += 1
    return refreshed


def forget_removed_endpoints(datastore: Datastore, prefix: ApproxPrefixCacheProducer) -> None:
    """Drop index entries for servers that are no longer in the datastore."""
    live = {str(e.metadata.id) for e in datastore.pod_list()}
    for server in prefix.indexer.servers():
        if server not in live:
            prefix.indexer.remove_server(server)


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
    scheduler, producers, prefix = build_scheduler(opts)
    rt = Runtime(
        opts,
        Datastore(),
        scheduler.profile_handler,
        scheduler.profiles,
        producers,
        prefix,
    )
    package = __name__.rsplit(".", 1)[0]
    for name in opts.features:
        importlib.import_module(f"{package}.{name}").install(rt, list(opts.feature_args))
    return rt


def serve(rt: Runtime, servicer: Any) -> tuple[grpc.Server, health.HealthServicer]:
    """Start the gRPC server: ``servicer`` plus the standard health service (NOT_SERVING at first).

    Serve TLS when ``rt.server_credentials`` is set, plaintext otherwise.
    With TLS, also listen in plaintext on ``health_port``: the kubelet's gRPC
    probe cannot do TLS.
    """
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=64))
    pb_grpc.add_ExternalProcessorServicer_to_server(servicer, server)
    health_servicer = health.HealthServicer()
    health_pb2_grpc.add_HealthServicer_to_server(health_servicer, server)
    for service in ("", EXT_PROC_SERVICE):
        health_servicer.set(service, health_pb2.HealthCheckResponse.NOT_SERVING)
    address = f"[::]:{rt.opts.grpc_port}"
    if rt.server_credentials is None:
        server.add_insecure_port(address)
    else:
        server.add_secure_port(address, rt.server_credentials)
        server.add_insecure_port(f"[::]:{rt.opts.health_port}")
    server.start()
    return server, health_servicer


def run(rt: Runtime) -> int:
    """Start discovery and the gRPC server, then tick until ``rt.stop`` is set.

    Each tick refreshes metrics, forgets removed endpoints in the prefix
    index, runs ``rt.ticks``, and reports SERVING only once the pool has
    synced, so Kubernetes sends traffic only to an EPP that knows its
    endpoints.
    """
    opts = rt.opts
    director: RequestHandler = Director(
        rt.datastore, Scheduler(rt.profile_handler, rt.profiles), rt.producers
    )
    for wrap in rt.director_wrappers:
        director = wrap(director)
    controller = Controller(
        rt.datastore, KubeClient(opts.kube_context), opts.pool_namespace, opts.pool_name
    )
    controller.run(rt.stop)
    servicer = rt.servicer_factory(director)
    for wrap_servicer in rt.servicer_wrappers:
        servicer = wrap_servicer(servicer, director)
    server, health_servicer = serve(rt, servicer)
    print(f"epp: serving ext-proc on :{opts.grpc_port} for pool {opts.pool_name}", flush=True)

    status = health_pb2.HealthCheckResponse.NOT_SERVING
    with httpx.Client() as client:
        while not rt.stop.is_set():
            refresh_metrics_once(rt.datastore, client)
            if rt.prefix is not None:
                forget_removed_endpoints(rt.datastore, rt.prefix)
            for tick in rt.ticks:
                tick()
            wanted = (
                health_pb2.HealthCheckResponse.SERVING
                if rt.datastore.pool_has_synced()
                else health_pb2.HealthCheckResponse.NOT_SERVING
            )
            if wanted != status:
                for service in ("", EXT_PROC_SERVICE):
                    health_servicer.set(service, wanted)
                status = wanted
            rt.stop.wait(opts.refresh_interval)
    server.stop(grace=5).wait()
    return 0


def main(argv: list[str] | None = None) -> int:
    """Parse, build, run; SIGTERM or SIGINT stops it."""
    rt = build(parse_options(argv))

    def shutdown(signum: int, frame: object) -> None:
        rt.stop.set()

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)
    return run(rt)


if __name__ == "__main__":
    # Run the importable module, not this __main__ copy: features import it by name.
    from lean_llmd_ref.epp import main as module_main

    sys.exit(module_main())
