# Day 9 — Prefill/decode disaggregation

~4 hours · laptop (Docker + kind)

> **Goal:** split a request across two servers. A *prefill* server computes
> the prompt's KV cache; a *decode* server pulls that cache and generates.
> Your EPP picks both; a sidecar you write, running inside each decode Pod,
> carries out the two-step protocol.

## What you build

| File | What to implement |
|---|---|
| `src/lean_llmd/disagg.py` | `PrefillFilter`, `DecodeFilter`, `PrefixBasedPDDecider`, `DisaggProfileHandler`, `pd_deployments`, `install` |
| `src/lean_llmd/pd_sidecar.py` | `prefill_body`, `decode_body`, `SidecarHandler` (`proxy`, `forward`, ...), `SidecarServer`, `main` |

## Why split at all

Prefill is compute-bound: a long prompt keeps the GPU's math units busy.
Decode is memory-bandwidth-bound: each step reads the whole KV cache to make
one token. In one server, a long prefill stalls every decoding stream in the
batch (that is the inter-token-latency spike you see under mixed load).
Separate pools can be sized, scheduled and even built from different
hardware independently. The price is moving the KV cache over the network,
so it only pays for prompts long enough that prefill dominates.

## The request flow

```
client ─▶ Envoy ─▶ EPP: decode profile picks D; decider says "long and uncached";
                        prefill profile picks P; header x-prefiller-host-port: P
       ─▶ D:8000 (your sidecar)
             1. POST P/v1/completions   max_tokens=1, kv_transfer_params.do_remote_decode
                ◀─ kv_transfer_params (where to pull the cache from)
             2. POST localhost:8200/v1/completions   original body + those params
                ◀─ tokens, streamed back to the client
```

On a GPU the transfer is NIXL over RDMA or TCP. The simulator charges
`--kv-cache-transfer-time-per-token` instead of a prefill.

## Part A — the EPP side

### Step 1. Role filters

Pods carry `llm-d.ai/role`. `PrefillFilter` keeps `prefill`,
`encode-prefill`, `prefill-decode`, `encode-prefill-decode`.
`DecodeFilter` keeps `decode`, `prefill-decode`, `encode-prefill-decode`,
**and Pods with no role at all**: an unlabelled Pod is an ordinary server
that does both, so a pool can grow into P/D gradually.

### Step 2. `PrefixBasedPDDecider`

Disaggregate when the prompt's **uncached** tokens reach a threshold:
`total - match_blocks × block_size ≥ non_cached_tokens`, measured on the
endpoint decode already chose. If that replica already holds most of the
prefix, a local prefill is cheap and a remote one is wasted work.

### Step 3. `DisaggProfileHandler`

Day 4's loop pays off. Call one: run `decode`. Call two: if decode found an
endpoint and the decider approves it, run `prefill`. Call three: nothing.
`process_results` makes decode primary (Envoy sends the request there) and
puts the prefill endpoint into `request.set_headers`, which day 6's
`routing_response` now sends along.

> **Trap:** clear the header when there is no prefill. A client could send
> its own `x-prefiller-host-port` and make your sidecar POST anywhere.

### Step 4. `pd_deployments`

Two Deployments from one simulator template. The decode Pod gets two
containers: your sidecar on port 8000 (the pool's target port, so traffic
*and metric scrapes* reach it first) and the simulator moved to 8200. The
probes move to the sidecar, which proxies `/health` and `/metrics` through.

## Part B — the sidecar

### Step 5. `prefill_body`, `decode_body`

The prefill request is the client's request with `max_tokens: 1`, no
streaming, and `kv_transfer_params: {do_remote_decode: true, ...}`. The
decode request is the client's request plus the `kv_transfer_params` the
prefill server returned. Never modify the client's dict.

### Step 6. `SidecarHandler`

A small HTTP proxy on `http.server`. Completions with the header run the
two steps; everything else is forwarded to the local server unchanged. The
decode response must be streamed back as it arrives (chunked transfer
encoding), or streaming clients see every token at the end.

> **Trap:** drop hop-by-hop headers (`connection`, `transfer-encoding`,
> `content-length`, `host`) when forwarding, in both directions, and drop
> the prefill header itself.

## Run it

```bash
uv run lean-llmd test --day 9
uv run lean-llmd image
```

The Docker test puts your sidecar between two real simulators: the prefill
simulator's own counters must show one `finish_reason="remote_decode"`
request, and the decode simulator's TTFT must reflect a 1 ms/token transfer,
not a 5 ms/token prefill.

On the cluster:

```bash
uv run python -c "
import yaml
from lean_llmd import manifests as m, disagg as dg, discovery as d, deploy
side = ['python', '-m', 'lean_llmd.pd_sidecar', '--port', '8000', '--vllm-port', '8200']
objs = [*dg.pd_deployments('sim', 'lean-llmd', m.sim_deployment(), 'lean-llmd:dev', side),
        *deploy.epp_objects('epp', 'lean-llmd', 'pool',
                            extra_args=('--feature', 'disagg', '--pd-non-cached-tokens', '200'))]
print(yaml.safe_dump_all(objs))" | kubectl apply -f -
kubectl delete deploy sim -n lean-llmd   # the old aggregated replicas
```

1. `kubectl get pods -n lean-llmd -L llm-d.ai/role`. One InferencePool
   selects both roles. Where in your EPP does the split happen?
2. Send a short and a long prompt through `http://epp`. Read
   `kubectl logs <decode-pod> -c sidecar` and both simulators' counters.
3. Change `--pd-non-cached-tokens` to 0 (off) and 50. Send the same long
   prompt twice in a row. Does the second one disaggregate? Why not?
4. Scale prefill and decode independently:
   `kubectl scale deploy/sim-decode --replicas=3`.

## Compare, map

```bash
uv run lean-llmd diff --day 9
uv run lean-llmd map --day 9
```

1. **`docs/architecture/advanced/disaggregation/README.md`**.
2. **`disagg_profile_handler.go`**: upstream sets the header in
   `PreRequest`, after scheduling; you set it in `process_results`. Find
   the encode stage you left out.
3. **`prefix_based_pd_decider.go`**: `computeNeedsRemotePrefill` against
   your `disaggregate`.
4. **`connector_nixlv2.go`**: your sidecar, plus retries, tracing, and
   data-parallel ranks. Find `CapSingleToken`.

## Check yourself

1. With block size 64 (pseudo-tokens) and threshold 200, a 1,200-byte
   prompt whose decode endpoint holds 2 matching blocks: disaggregate?
2. In the Docker test, what TTFT did the decode simulator report for the
   200-token prompt, and what would it have been prefilling locally?
3. Quote the `kv_transfer_params` the prefill simulator returned.
4. Which Pod's `/metrics` does the EPP scrape for a decode endpoint: the
   sidecar's or the simulator's? Follow the request to prove it.
