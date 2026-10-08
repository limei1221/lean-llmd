"""The syllabus, and where each day lands in real llm-d.

Upstream paths are pinned per repository in ``PINS``. ``lean-llmd map --day N
--verify`` checks them against local checkouts, so a rename upstream fails
loudly instead of sending you to a file that no longer exists.
"""

from dataclasses import dataclass

# llm-d v0.10.0 ships router v0.11.0 (guides/env.sh); the simulator is its latest.
PINS: dict[str, str] = {
    "llm-d": "v0.10.0",
    "llm-d-router": "v0.11.0",
    "llm-d-inference-sim": "v0.12.0",
}
SOURCE_URL = "https://github.com/llm-d/{repo}/blob/{pin}/{path}"

SIM_IMAGE = "ghcr.io/llm-d/llm-d-inference-sim:v0.12.0"
GAIE_VERSION = "v1.5.0"
GATEWAY_API_VERSION = "v1.5.1"
# llm-d's docs pin agentgateway v1.1.0, whose CRDs exceed Kubernetes 1.37's CEL cost budget.
AGENTGATEWAY_VERSION = "v1.6.0"
PROMETHEUS_CHART_VERSION = "29.36.0"
KEDA_CHART_VERSION = "2.20.2"


@dataclass(frozen=True)
class Ref:
    """A place in llm-d where today's concept actually lives."""

    repo: str
    path: str
    symbols: tuple[str, ...]
    note: str

    def url(self) -> str:
        return SOURCE_URL.format(repo=self.repo, pin=PINS[self.repo], path=self.path)


@dataclass(frozen=True)
class Day:
    number: int
    title: str
    concept: str
    modules: tuple[str, ...] = ()
    upstream: tuple[Ref, ...] = ()
    depends_on: tuple[int, ...] = ()

    @property
    def ready(self) -> bool:
        return bool(self.modules)

    @property
    def test_path(self) -> str:
        return f"tests/test_day{self.number:02d}.py"

    @property
    def doc_path(self) -> str:
        return f"course/day{self.number:02d}.md"

    @property
    def module_paths(self) -> tuple[str, ...]:
        return tuple(f"src/lean_llmd/{m}.py" for m in self.modules)

    @property
    def reference_paths(self) -> tuple[str, ...]:
        return tuple(f"src/lean_llmd_ref/{m}.py" for m in self.modules)


ROUTER = "llm-d-router"
LLMD = "llm-d"
SIM = "llm-d-inference-sim"
_PLUGINS = "pkg/epp/framework/plugins"

DAYS: tuple[Day, ...] = (
    Day(
        number=1,
        title="The model server on Kubernetes",
        concept=(
            "Run three simulated vLLM replicas as a Deployment behind a Service. Labels tie the "
            "pieces together, probes decide which Pods receive traffic, and the controller puts "
            "back whatever you delete."
        ),
        modules=("manifests",),
        upstream=(
            Ref(
                SIM,
                "manifests/deployment.yaml",
                ("kind: Deployment", "fieldPath: status.podIP", "readinessProbe"),
                "The simulator's own manifest. Note the POD_IP env var from the downward API.",
            ),
            Ref(
                LLMD,
                "guides/recipes/modelserver/base/single-host/default/decode-deployment.yaml",
                ("livenessProbe", "startupProbe", "path: /v1/models"),
                "llm-d's base model-server Deployment: three probes asking three questions.",
            ),
            Ref(
                SIM,
                "docs/latency-simulation.md",
                ("prefill-time-per-token", "inter-token-latency"),
                "What the simulator's latency flags mean; you set three of them today.",
            ),
        ),
    ),
    Day(
        number=2,
        title="Reading the model server's metrics",
        concept=(
            "The router's view of a replica is three numbers scraped from /metrics: queued "
            "requests, running requests, and KV-cache use. Parse the text format, select a "
            "series by spec, and fold it into a Metrics snapshot."
        ),
        modules=("metrics",),
        depends_on=(1,),
        upstream=(
            Ref(
                ROUTER,
                "pkg/epp/framework/interface/datalayer/metrics.go",
                ("type Metrics struct", "WaitingQueueSize", "KVCacheUsagePercent"),
                "The snapshot every scorer reads. Yours is the same struct, minus LoRA.",
            ),
            Ref(
                ROUTER,
                f"{_PLUGINS}/datalayer/extractor/metrics/spec.go",
                ("parseStringToSpec", "getLatestMetric", "labelsMatch", "extractValue"),
                "Specs like name{label=value}; when several series match, the newest wins.",
            ),
            Ref(
                ROUTER,
                f"{_PLUGINS}/datalayer/extractor/metrics/extractor.go",
                ("func (ext *Extractor) Extract", "populateCacheInfoMetrics", "clone :="),
                "Extract works on a clone and swaps it in: readers never see half an update.",
            ),
            Ref(
                ROUTER,
                f"{_PLUGINS}/datalayer/extractor/metrics/factories.go",
                ("vllm:num_requests_waiting", "vllm:kv_cache_usage_perc", "vllm:cache_config_info"),
                "The vLLM metric names. SGLang and TRT-LLM get their own mapping.",
            ),
        ),
    ),
    Day(
        number=3,
        title="Discovering endpoints from an InferencePool",
        concept=(
            "An InferencePool is a Service built for LLMs: a label selector and target ports. "
            "Watch it and the Pods it selects, keep only ready ones, and resync everything "
            "when the selector changes, because no Pod event tells you that."
        ),
        modules=("datastore", "discovery"),
        depends_on=(2,),
        upstream=(
            Ref(
                LLMD,
                "docs/architecture/core/inferencepool.md",
                ("Selector-based Discovery", "targetPorts"),
                "What an InferencePool is for. Read before the code.",
            ),
            Ref(
                ROUTER,
                "pkg/epp/controller/pod_reconciler.go",
                ("func (c *PodReconciler) Reconcile", "updateDatastore", "PoolLabelsMatch"),
                "A Pod event becomes add-or-delete. NotFound means the Pod is gone.",
            ),
            Ref(
                ROUTER,
                "pkg/epp/controller/inferencepool_reconciler.go",
                ("func (c *InferencePoolReconciler) Reconcile", "PoolSet"),
                "A pool event replaces the pool; a deleted pool clears the datastore.",
            ),
            Ref(
                ROUTER,
                "pkg/epp/datastore/datastore.go",
                ("func (ds *datastore) PoolSet", "podResyncAll", "createEndpointNamespacedName"),
                "Why PoolSet lists Pods itself, and why endpoints are named pod-rank-N.",
            ),
            Ref(
                ROUTER,
                "pkg/epp/util/pod/pod.go",
                ("func IsPodReady", "DeletionTimestamp"),
                "Ready means the Ready condition is True and the Pod is not being deleted.",
            ),
        ),
    ),
    Day(
        number=4,
        title="The scheduler: filter, score, pick",
        concept=(
            "Every routing decision is the same pipeline: filters drop endpoints, weighted "
            "scorers rank the rest in [0, 1], a picker chooses. A profile handler decides which "
            "pipelines run; with one profile it is trivial, with prefill/decode it is not."
        ),
        modules=("scheduling", "plugins"),
        depends_on=(3,),
        upstream=(
            Ref(
                ROUTER,
                "pkg/epp/framework/interface/scheduling/plugins.go",
                ("type ProfileHandler interface", "type Filter interface", "type Scorer interface"),
                "The four extension points. Your Protocols are these interfaces.",
            ),
            Ref(
                ROUTER,
                "pkg/epp/scheduling/scheduler.go",
                ("func (s *Scheduler) Schedule", "profileHandler.Pick", "ProcessResults"),
                "The loop: pick profiles, run them, repeat until none are picked.",
            ),
            Ref(
                ROUTER,
                "pkg/epp/scheduling/scheduler_profile.go",
                ("func (p *SchedulerProfile) Run", "runScorerPlugins", "enforceScoreRange"),
                "Filters short-circuit; scores are clamped, weighted and summed.",
            ),
            Ref(
                ROUTER,
                f"{_PLUGINS}/scheduling/scorer/queuedepth/queue.go",
                ("func (s *QueueScorer) Score", "maxQueueSize == minQueueSize"),
                "Min-max normalisation, and what it does when every queue is equal.",
            ),
            Ref(
                ROUTER,
                f"{_PLUGINS}/scheduling/scorer/kvcacheutilization/kvcache_utilization.go",
                ("func (s *KVCacheUtilizationScorer) Score", "1 - endpoint.GetMetrics()"),
                "The simplest scorer in the tree.",
            ),
            Ref(
                ROUTER,
                f"{_PLUGINS}/scheduling/picker/maxscore/picker.go",
                ("func (p *MaxScorePicker) Pick", "RotateScoredEndpoints"),
                "Ties rotate round-robin, so equal endpoints share load.",
            ),
            Ref(
                ROUTER,
                f"{_PLUGINS}/scheduling/profilehandler/single/single_profile_handler.go",
                ("func (h *SingleProfileHandler) Pick", "PrimaryProfileName"),
                "Run every profile once, then stop.",
            ),
        ),
    ),
    Day(
        number=5,
        title="Prefix-cache-aware routing",
        concept=(
            "Send a request where its prefix was last served and the replica skips most of the "
            "prefill. The router cannot see the cache, so it keeps its own guess: chained block "
            "hashes in a per-endpoint LRU, updated with every routing decision."
        ),
        modules=("prefix",),
        depends_on=(4,),
        upstream=(
            Ref(
                LLMD,
                "docs/architecture/advanced/kv-management/prefix-cache-aware-routing.md",
                ("Approximate", "Precise"),
                "The two designs. Today is the approximate one; precise needs KV events.",
            ),
            Ref(
                ROUTER,
                f"{_PLUGINS}/requestcontrol/dataproducer/tokenizer/estimate.go",
                ("bytesPerToken", "binary.LittleEndian.Uint32"),
                "Tokens without a tokenizer: every four bytes of prompt become one uint32.",
            ),
            Ref(
                ROUTER,
                f"{_PLUGINS}/requestcontrol/dataproducer/prefixhash/hashing.go",
                ("computeBlockHashes", "prevBlockHash", "request.TargetModel"),
                "Each block hash folds in the previous one, so equal hashes mean equal prefixes.",
            ),
            Ref(
                ROUTER,
                f"{_PLUGINS}/requestcontrol/dataproducer/approximateprefix/indexer.go",
                ("func (i *indexer) Add", "makeEvictionFn", "idx := len(hashes) - 1"),
                "Blocks are added last-first, so an LRU evicts a prefix from its tail.",
            ),
            Ref(
                ROUTER,
                f"{_PLUGINS}/requestcontrol/dataproducer/approximateprefix/plugin.go",
                ("matchLongestPrefix", "func (p *dataProducer) PreRequest", "minBlockSizeTokens"),
                "Produce before scheduling, record after: the index learns from its own picks.",
            ),
            Ref(
                ROUTER,
                f"{_PLUGINS}/scheduling/scorer/prefix/plugin.go",
                ("func (p *Plugin) Score", "matchRatioScore"),
                "Score = matched blocks / total blocks, with an optional length term.",
            ),
        ),
    ),
    Day(
        number=6,
        title="The endpoint picker behind Envoy",
        concept=(
            "Envoy owns the connections; your EPP only answers one question per request over "
            "gRPC ext-proc: which ip:port. Package it, give it RBAC to watch Pods, put Envoy "
            "next to it in one Pod, and route real traffic on kind."
        ),
        modules=("extproc", "epp", "deploy"),
        depends_on=(5,),
        upstream=(
            Ref(
                LLMD,
                "docs/architecture/core/router/proxy.md",
                ("Standalone Mode", "Gateway Mode"),
                "Today is standalone mode: Envoy in the EPP's Pod, no Gateway API.",
            ),
            Ref(
                ROUTER,
                "pkg/epp/handlers/server.go",
                ("func (s *StreamingServer) Process", "ProcessingRequest_RequestBody"),
                "One gRPC stream per HTTP request; a switch over the message types.",
            ),
            Ref(
                ROUTER,
                "pkg/epp/handlers/request.go",
                ("generateRequestHeaderResponse", "ClearRouteCache: true", "generateMetadata"),
                "The answer: a header, the same value as envoy.lb metadata, and a route reset.",
            ),
            Ref(
                ROUTER,
                "pkg/epp/metadata/consts.go",
                ("x-gateway-destination-endpoint", "envoy.lb"),
                "The two names Envoy and the EPP agree on.",
            ),
            Ref(
                ROUTER,
                "pkg/epp/requestcontrol/director.go",
                ("func (d *Director) HandleRequest", "runPreRequestPlugins", "GetRandomEndpoint"),
                "Glue between the parsed request, the scheduler and the plugins.",
            ),
            Ref(
                ROUTER,
                "config/charts/llm-d-router-standalone/values.yaml",
                ("ORIGINAL_DST", "use_http_header: true", "FULL_DUPLEX_STREAMED"),
                "The Envoy config you simplify: same cluster, buffered instead of streamed.",
            ),
            Ref(
                ROUTER,
                "config/charts/routerlib/templates/_rbac.yaml",
                ('resources: ["pods"]', 'verbs: ["get", "watch", "list"]'),
                "The EPP's Role: read-only on Pods. Compare its verbs with yours.",
            ),
            Ref(
                ROUTER,
                "config/charts/llm-d-router-standalone/templates/rbac.yaml",
                ('resources: ["inferencepools"]',),
                "And read-only on InferencePools, in a second Role.",
            ),
        ),
    ),
    Day(
        number=7,
        title="Measuring it against llm-d",
        concept=(
            "A streaming load generator that records TTFT and time per output token, a "
            "shared-prefix workload, and one experiment: a plain Service, your EPP, and llm-d's "
            "EPP in front of the same simulators."
        ),
        modules=("loadgen",),
        depends_on=(6,),
        upstream=(
            Ref(
                LLMD,
                "guides/optimized-baseline/README.md",
                ("prefix", "load"),
                "The guide your experiment reproduces in miniature.",
            ),
            Ref(
                SIM,
                "docs/latency-simulation.md",
                ("n_cached", "time-factor-under-load"),
                "The model behind every number you measure today. Know it before you claim.",
            ),
            Ref(
                SIM,
                "docs/metrics.md",
                ("vllm:time_to_first_token_seconds", "vllm:prefix_cache_hits_total"),
                "The server-side numbers your client-side ones must agree with.",
            ),
        ),
    ),
    Day(
        number=8,
        title="Precise prefix routing from KV events",
        concept=(
            "Stop guessing. Subscribe to every model server's ZMQ KV-cache events, map the "
            "engine's block hashes onto keys the EPP computes from the same token ids, and "
            "route on what each replica actually holds."
        ),
        modules=("kvevents", "precise"),
        depends_on=(7,),
        upstream=(
            Ref(
                LLMD,
                "docs/architecture/advanced/kv-management/kv-indexer.md",
                ("BlockStored", "BlockRemoved"),
                "The design: events in, an index of which pod holds which block out.",
            ),
            Ref(
                SIM,
                "docs/kv-cache.md",
                ("kv@<POD_IP>:<serving-port>@<model-name>", "parent_block_hash"),
                "The wire format you decode: three frames, a msgpack batch, two encodings.",
            ),
            Ref(
                ROUTER,
                "pkg/kvevents/pool.go",
                ("func (p *Pool) processEventBatch", "GetRequestKey", "TokensToKVBlockKeys"),
                "Engine keys map to request keys through the parent and the event's tokens.",
            ),
            Ref(
                ROUTER,
                "pkg/kvcache/kvblock/token_processor.go",
                ("func hashXXH64", "break // no partial blocks", "getInitHash"),
                "Your request_keys: the xxh64 variant, full blocks only.",
            ),
            Ref(
                ROUTER,
                "pkg/kvevents/subscriber_manager.go",
                ("type SubscriberManager struct", "EnsureSubscriber", "RemoveSubscriber"),
                "One ZMQ subscriber per pod, following pod discovery.",
            ),
            Ref(
                ROUTER,
                f"{_PLUGINS}/requestcontrol/dataproducer/preciseprefixcache/producer.go",
                ("type Producer struct", "PluginConfig"),
                "The producer that writes the same match info day 5's scorer reads.",
            ),
        ),
    ),
    Day(
        number=9,
        title="Prefill/decode disaggregation",
        concept=(
            "Split one request across two servers: a prefill server computes the KV cache, a "
            "decode server pulls it and generates. The EPP picks both; a sidecar in the decode "
            "Pod runs the two-step protocol."
        ),
        modules=("disagg", "pd_sidecar"),
        depends_on=(8,),
        upstream=(
            Ref(
                LLMD,
                "docs/architecture/advanced/disaggregation/README.md",
                ("x-prefiller-host-port", "sidecar"),
                "The request flow end to end. Read this first.",
            ),
            Ref(
                ROUTER,
                f"{_PLUGINS}/scheduling/profilehandler/disagg/disagg_profile_handler.go",
                ("type DisaggProfileHandlerParameters struct", "routing.PrefillEndpointHeader"),
                "Decode always, prefill when a decider approves; the header goes on in PreRequest.",
            ),
            Ref(
                ROUTER,
                f"{_PLUGINS}/scheduling/profilehandler/disagg/prefix_based_pd_decider.go",
                ("computeNeedsRemotePrefill", "nonCachedTokens >= d.config.NonCachedTokens"),
                "Disaggregate only when the uncached suffix is long enough to pay for the hop.",
            ),
            Ref(
                ROUTER,
                f"{_PLUGINS}/scheduling/filter/bylabel/roles.go",
                ('RoleLabel = "llm-d.ai/role"',),
                "The label both filters read.",
            ),
            Ref(
                ROUTER,
                "pkg/sidecar/proxy/connector_nixlv2.go",
                ("handleNIXLV2", "requestFieldDoRemoteDecode", "CapSingleToken"),
                "The sidecar's protocol: prefill with one token, then decode with its params.",
            ),
            Ref(
                SIM,
                "docs/kv-cache.md",
                ("do_remote_prefill", "kv-cache-transfer-time-per-token"),
                "How the simulator prices a KV transfer instead of a prefill.",
            ),
        ),
    ),
    Day(
        number=10,
        title="Flow control: priority, fairness, saturation",
        concept=(
            "When the pool is full, queue in the EPP instead of in the model servers: bands by "
            "priority, turns between tenants, a saturation gate, and a 429 for requests that "
            "waited too long. Priorities come from a CRD."
        ),
        modules=("flowcontrol",),
        depends_on=(9,),
        upstream=(
            Ref(
                LLMD,
                "docs/architecture/core/router/epp/flow-control.md",
                ("Priority Band", "x-llm-d-inference-fairness-id", "x-llm-d-inference-objective"),
                "The three tiers: band by priority, flow by fairness, item by ordering.",
            ),
            Ref(
                ROUTER,
                f"{_PLUGINS}/flowcontrol/saturationdetector/concurrency/detector.go",
                ("func (d *detector) Saturation", "reqCapacity"),
                "Your ConcurrencyDetector, plus a token mode and a per-endpoint filter.",
            ),
            Ref(
                ROUTER,
                f"{_PLUGINS}/flowcontrol/saturationdetector/utilization/README.md",
                ("EndpointScore = max(QueueDepth / QueueThreshold",),
                "The scraped-metrics alternative, and why stale metrics count as full.",
            ),
            Ref(
                ROUTER,
                f"{_PLUGINS}/flowcontrol/fairness/roundrobin/roundrobin.go",
                ("type roundRobinCursor struct", "func (p *roundRobin) Pick"),
                "Turns within a band.",
            ),
            Ref(
                ROUTER,
                "pkg/epp/requestcontrol/admission.go",
                ("ResourceExhausted", "request timed out in queue"),
                "Why an expired request is a 429, not a 503.",
            ),
            Ref(
                ROUTER,
                "config/crd/bases/llm-d.ai_inferenceobjectives.yaml",
                ("kind: InferenceObjective", "priority"),
                "The CRD behind priorities. `lean-llmd cluster up` installs it.",
            ),
        ),
    ),
    Day(
        number=11,
        title="Gateway mode",
        concept=(
            "Replace the Envoy sidecar with a shared Gateway: Gateway API objects, an HTTPRoute "
            "whose backend is the InferencePool, and an EPP that speaks TLS and the full-duplex "
            "streaming protocol a real gateway uses."
        ),
        modules=("gateway",),
        depends_on=(10,),
        upstream=(
            Ref(
                LLMD,
                "docs/infrastructure/gateway/agentgateway.md",
                ("inferenceExtension.enabled=true", "AGENTGATEWAY_VERSION"),
                "The gateway you install. The course uses a newer release (see the day).",
            ),
            Ref(
                LLMD,
                "guides/recipes/gateway/agentgateway/gateway.yaml",
                ("kind: Gateway", "gatewayClassName: agentgateway"),
                "llm-d's Gateway object, next to yours.",
            ),
            Ref(
                ROUTER,
                "pkg/epp/handlers/server.go",
                (
                    "ProcessingRequest_ResponseBody",
                    "modelServerStreaming",
                    "updateStateAndSendIfNeeded",
                ),
                "The full-duplex conversation, responses included.",
            ),
            Ref(
                ROUTER,
                "pkg/common/envoy/chunking.go",
                ("BodyByteLimit = 62000", "StreamedBodyResponse"),
                "How a held body goes back to the proxy.",
            ),
            Ref(
                ROUTER,
                "pkg/common/certs.go",
                ("type CertReloader struct", "tls.LoadX509KeyPair"),
                "Upstream reloads a mounted certificate; you generate one.",
            ),
        ),
    ),
    Day(
        number=12,
        title="Observability and autoscaling",
        concept=(
            "Export the EPP's view of demand as llm_d_epp_* metrics, let Prometheus scrape it, "
            "and let a KEDA ScaledObject scale the model servers on it. No custom controller."
        ),
        modules=("observability",),
        depends_on=(11,),
        upstream=(
            Ref(
                LLMD,
                "docs/architecture/advanced/autoscaling/keda-epp.md",
                ("llm_d_epp_flow_control_queue_size", "AverageValue", "Dual-Metric Strategy"),
                "The design you build: EPP metrics, Prometheus, KEDA, an HPA.",
            ),
            Ref(
                LLMD,
                "guides/workload-autoscaling/keda-epp/optimized-baseline/base/scaledobject.yaml",
                ("kind: ScaledObject", "stabilizationWindowSeconds", "llm_d_epp_request_running"),
                "llm-d's ScaledObject. Compare its windows with yours.",
            ),
            Ref(
                ROUTER,
                "pkg/epp/metrics/llm_d_router_metrics.go",
                ('"request_running"', '"ready_endpoints"', '"flow_control_queue_size"'),
                "The upstream series your registry mirrors.",
            ),
            Ref(
                LLMD,
                "docs/architecture/advanced/autoscaling/README.md",
                ("deprecated", "KEDA"),
                "Why the variant autoscaler (WVA) is gone.",
            ),
        ),
    ),
    Day(
        number=13,
        title="Failure modes",
        concept=(
            "Three ways to lose requests that are not the model's fault: a drain takes too many "
            "replicas, a rollout kills them mid-request, the picker goes down. Disruption "
            "budgets, graceful termination, and failing open."
        ),
        modules=("resilience",),
        depends_on=(12,),
        upstream=(
            Ref(
                LLMD,
                "docs/operations/graceful-shutdown.md",
                ("preStop", "terminationGracePeriodSeconds"),
                "The termination sequence, and vLLM's own drain.",
            ),
            Ref(
                LLMD,
                "docs/architecture/core/inferencepool.md",
                ("failureMode", "FailOpen"),
                "What the pool's failureMode asks a gateway to do.",
            ),
            Ref(
                ROUTER,
                "config/charts/llm-d-router-standalone/values.yaml",
                ("failure_mode_allow", "--drain-strategy"),
                "Upstream's Envoy: fail-open is a chart value.",
            ),
        ),
    ),
    Day(
        number=14,
        title="Optional: real vLLM on a GPU",
        concept=(
            "Swap the simulator for vLLM on a rented GPU: device plugins, nvidia.com/gpu "
            "requests, taints, shared memory, model caches, and a startup probe measured in "
            "minutes. Everything you built routes it unchanged."
        ),
        modules=("gpu",),
        depends_on=(13,),
        upstream=(
            Ref(
                LLMD,
                "guides/recipes/modelserver/components/images/gpu-vllm/release/kustomization.yaml",
                ("docker.io/vllm/vllm-openai", "v0.30.0"),
                "The vLLM image llm-d v0.10.0 ships with.",
            ),
            Ref(
                LLMD,
                "guides/optimized-baseline/modelserver/gpu/vllm/base/patch-vllm.yaml",
                ("nvidia.com/gpu", "/dev/shm", "failureThreshold: 120"),
                "A production GPU model server: resources, shm, and a 60-minute startup budget.",
            ),
            Ref(
                LLMD,
                "guides/precise-prefix-cache-routing/modelserver/gpu/vllm/base/patch-vllm.yaml",
                ("--kv-events-config", "kv@$(POD_IP):$(POD_PORT)"),
                "Real vLLM publishing day 8's events.",
            ),
        ),
    ),
)

DELIVERABLE = (
    "A working endpoint picker you wrote, routing real traffic on Kubernetes, measured "
    "against a plain Service and against llm-d's own EPP on the same simulated replicas."
)
NUMBERS = (
    "p50 and p99 TTFT per router on one shared-prefix workload (simulated latency)",
    "prefix-cache hit rate per router, from the simulators' own counters",
    "requests per replica per router: how even the load spread was",
)


CORE_DAYS = 7
OPTIONAL_DAYS = frozenset({14})


def section(day: Day) -> str:
    """Which part of the course a day belongs to."""
    if day.number <= CORE_DAYS:
        return "core"
    return "optional" if day.number in OPTIONAL_DAYS else "extension"


def get(number: int) -> Day:
    for day in DAYS:
        if day.number == number:
            return day
    raise SystemExit(f"no day {number}; the course has days 1-{len(DAYS)}")


def ready_days() -> list[Day]:
    return [d for d in DAYS if d.ready]
