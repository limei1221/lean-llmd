# 4-Week Kubernetes Learning Path for LLM Inference Engineers

**Goal:** Learn Kubernetes by deploying, debugging, and optimizing distributed LLM inference with [llm-d](https://github.com/llm-d/llm-d).

**Prerequisites:** Familiarity with Linux, Docker, Python, and the basics of vLLM or similar LLM inference engines.

**Approach:** Spend approximately 25% of your time on Kubernetes fundamentals, 50% on hands-on deployment and experiments, and 25% reading llm-d's routing and orchestration code.

> **Hardware note:** Week 1 and much of the configuration inspection can be done locally with kind or minikube without GPUs. Real LLM serving requires GPU-capable infrastructure; prefill/decode disaggregation and wide expert parallelism typically require appropriately configured multi-GPU infrastructure.

## Week 1 — Kubernetes Fundamentals

**Objective:** Learn the Kubernetes building blocks by running a simple service, then a vLLM server.

### Topics
- Pods, Deployments, ReplicaSets, and Services
- Namespaces, ConfigMaps, and Secrets
- CPU/memory requests and limits; GPU resource requests
- Readiness and liveness probes
- `kubectl` inspection, logs, events, and debugging

### Exercises
1. Install **kind** or **minikube** and deploy a small HTTP service.
2. Expose it through a Kubernetes Service, scale replicas, and inspect endpoint selection.
3. Delete a Pod and observe how its Deployment restores the desired state.
4. Introduce a failing readiness probe and use logs, events, and `describe` to diagnose it.
5. On a GPU-enabled cluster, deploy a small vLLM model server and access its API through a Service.

**Deliverable:** A running service (ideally vLLM) reachable through Kubernetes, with working health checks and a short explanation of how its Pods are scheduled and restarted.

## Week 2 — Deploy llm-d and Understand Networking

**Objective:** Understand how requests reach model-serving replicas through Kubernetes networking and llm-d's intelligent routing layer.

### Topics
- Helm and Kustomize
- Kubernetes Services, DNS, and service discovery
- Gateway API and inference-aware routing
- llm-d router / endpoint selection
- InferencePool and related resources used by the selected deployment recipe

### Exercises
1. Follow the [llm-d Quickstart](https://llm-d.ai/docs/getting-started/quickstart) and inspect its manifests before deployment.
2. Deploy an optimized baseline using the [well-lit path guides](https://llm-d.ai/docs/well-lit-paths).
3. List and inspect Pods, Deployments, Services, Gateway resources, and inference-specific custom resources.
4. Trace a request from the client through the gateway/router to the model-serving worker.
5. Run multiple replicas and observe how the routing policy selects an endpoint.

**Deliverable:** A multi-replica inference deployment behind an intelligent router, plus a request-path diagram explaining which Kubernetes objects participate.

## Week 3 — GPU Scheduling and Distributed Inference

**Objective:** Learn Kubernetes GPU placement and how llm-d coordinates separated prefill and decode workloads.

### Topics
- GPU device plugins and accelerator resource allocation
- Node labels, node selectors, affinity, taints, and tolerations
- GPU topology basics: intra-node versus inter-node communication
- Prefill/decode disaggregation and KV-cache transfer
- Independent scaling and configuration of prefill and decode workers

### Exercises
1. Inspect GPU allocatable resources on nodes and the GPU requests on serving Pods.
2. Experiment with node selectors and affinity; deliberately create an unschedulable Pod and diagnose the scheduling events.
3. Study the llm-d [P/D disaggregation guide](https://llm-d.ai/docs/well-lit-paths/foundations/pd-disaggregation).
4. On suitable hardware, deploy distinct prefill and decode pools and trace routing and KV-cache movement.
5. Compare a colocated serving setup to P/D disaggregation using representative prompt/output lengths.

**Deliverable:** Separate prefill/decode worker pools where hardware permits, with an explanation of Kubernetes scheduling, data transfer, and performance trade-offs. Without multi-GPU access, produce a fully annotated manifest and architecture walkthrough instead.

## Week 4 — Autoscaling, Observability, and Performance

**Objective:** Operate an inference deployment under changing load and measure when scaling helps or hurts.

### Topics
- Metrics collection and Prometheus fundamentals
- Inference metrics: TTFT, inter-token latency / TPOT, throughput, queue depth, and GPU utilization
- Horizontal scaling and inference-specific autoscaling signals
- Rolling updates and failure recovery
- Load generation, bottleneck analysis, and SLO-aware operation

### Exercises
1. Collect serving and routing metrics for a baseline deployment.
2. Generate workloads varying concurrency, input length, and output length.
3. Study llm-d's [workload-variant autoscaler](https://github.com/llm-d/llm-d-workload-variant-autoscaler).
4. Configure autoscaling on suitable infrastructure and observe behavior during a load ramp.
5. Kill a worker or introduce a deployment change, then analyze availability and performance during recovery.
6. Compare baseline throughput and latency against a tuned deployment; document costs and trade-offs.

**Deliverable:** A benchmark report with workload parameters, deployment settings, TTFT/TPOT/throughput results, and an explanation of the effect of scaling. If GPU resources are unavailable, focus on metrics integration, manifests, and a reproducible experiment plan.

## Useful `kubectl` Commands

```bash
# Inspect cluster resources
kubectl get pods -A -o wide
kubectl get deployments -A
kubectl get services -A
kubectl get gateway -A

# Inspect one workload (replace placeholders)
kubectl describe pod <pod-name> -n <namespace>
kubectl get pod <pod-name> -n <namespace> -o yaml
kubectl logs <pod-name> -n <namespace>
kubectl get events -n <namespace> --sort-by=.lastTimestamp

# Explore custom resource definitions installed by llm-d
kubectl get crds
```

## Recommended Reading Order

1. [Kubernetes Basics](https://kubernetes.io/docs/tutorials/kubernetes-basics/) — fundamental abstractions and exercises.
2. [llm-d Quickstart](https://llm-d.ai/docs/getting-started/quickstart) — initial deployment.
3. [llm-d Architecture](https://llm-d.ai/docs/architecture) — serving stack and request flows.
4. [llm-d Well-Lit Paths](https://llm-d.ai/docs/well-lit-paths) — practical deployment patterns.
5. [llm-d Router](https://github.com/llm-d/llm-d-router) — routing implementation.
6. [llm-d KV Cache](https://github.com/llm-d/llm-d-kv-cache) — distributed cache coordination.
7. [llm-d Benchmark](https://github.com/llm-d/llm-d-benchmark) — performance methodology.

## Success Criteria

By the end of four weeks, you should be able to:

- Deploy and troubleshoot a GPU-backed model-serving application on Kubernetes.
- Explain how Services, Gateway API, and llm-d route inference requests.
- Understand GPU scheduling and diagnose why a serving Pod is pending or unhealthy.
- Explain the Kubernetes and inference-system trade-offs of P/D disaggregation.
- Measure TTFT, TPOT, throughput, and utilization under load.
- Design and explain an autoscaling and rollout strategy for production LLM inference.

**Guiding principle:** Do not just read YAML or implementation code. Deploy something, change a setting, observe the result, and explain why Kubernetes and llm-d behaved that way.
