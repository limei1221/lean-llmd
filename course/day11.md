# Day 11 — Gateway mode

~4 hours · laptop (kind)

> **Goal:** retire the Envoy sidecar. A shared Gateway, configured through
> the Kubernetes Gateway API, calls your EPP for every request routed to an
> InferencePool. A real gateway expects more from the EPP than your Envoy
> did: TLS, and the full-duplex streaming protocol, responses included.

## What you build

| File | What to implement |
|---|---|
| `src/lean_llmd/gateway.py` | `self_signed_certificate`, `streamed_body`, `FullDuplexStreamingServer.Process`, `gateway`, `http_route`, `gateway_mode`, `install` |

## Before you start

```bash
uv run lean-llmd addons gateway
kubectl get gatewayclass                 # agentgateway  ACCEPTED True
```

This installs [agentgateway](https://agentgateway.dev), llm-d's preferred
self-installed gateway. llm-d v0.10.0's docs pin v1.1.0, but its CRDs
exceed Kubernetes 1.37's validation-cost budget and the API server rejects
them; the course uses v1.6.0. Version drift like this is ordinary: read the
error, check the release notes, move on.

## The Gateway API in three objects

| Object | Who owns it | Says |
|---|---|---|
| `GatewayClass` | the gateway's installer | "agentgateway implements Gateways" |
| `Gateway` | the platform team | "listen on port 80" |
| `HTTPRoute` | the app team | "send `/` to InferencePool `pool`" |

The InferencePool's `endpointPickerRef` tells the gateway which Service is
the EPP. Upstream calls this **gateway mode**; day 6 was **standalone
mode**. Same EPP, different proxy.

### Step 1. `gateway`, `http_route`, `gateway_mode`

The HTTPRoute's backendRef names `group: inference.networking.k8s.io`,
`kind: InferencePool`. `gateway_mode` turns day 6's Deployment into a
single-container one.

## The protocol, again

### Step 2. TLS

Gateways dial the EPP with TLS (skipping verification). Day 6's plaintext
EPP gets `received corrupt message of type InvalidContentType` in the
gateway's log, and with `FailOpen` the gateway then silently picks a random
endpoint. Every request still succeeds, so nothing looks wrong. That is
why the kind test sets `FailClose`.

`self_signed_certificate` makes an EC key and a certificate with the
`cryptography` package; `install` hands them to
`grpc.ssl_server_credentials`.

> **Trap:** the kubelet's gRPC readiness probe cannot do TLS. Day 6's
> `serve` now also listens in plaintext on 9003 when TLS is on, and
> `gateway_mode` points the probe there. Upstream does the same.

### Step 3. `FULL_DUPLEX_STREAMED`

In buffered mode Envoy waited for each answer. In full-duplex mode the
proxy keeps streaming and you answer when ready:

| You receive | You send |
|---|---|
| `request_headers` | nothing yet (unless `end_of_stream`: route now) |
| `request_body` chunks | nothing until `end_of_stream`; then the **headers** answer (destination, `clear_route_cache`) **and the whole body back**, in ≤ 62,000-byte `StreamedBodyResponse` chunks |
| `response_headers` | an empty `HeadersResponse` |
| `response_body` chunks | each chunk echoed, same `end_of_stream` |
| trailers | an empty `TrailersResponse` |

> **Trap:** in this mode the EPP *holds* the request body. Forget to stream
> it back and the model server receives a request with no body.

Seeing the response is what lets upstream measure TTFT, count tokens and
rewrite model names. Day 12 uses it to count running requests.

## Run it

```bash
uv run lean-llmd test --day 11
uv run lean-llmd image
```

The kind test deploys simulators, your EPP in gateway mode, a Gateway and
an HTTPRoute, then checks that a shared prefix sticks to one replica
(only your EPP could cause that) and that streaming works end to end.

On the cluster:

```bash
uv run python -c "
import yaml
from lean_llmd import deploy, gateway as gw
objs = [gw.gateway_mode(deploy.epp_deployment('epp', 'lean-llmd', 'pool')),
        gw.gateway('gw', 'lean-llmd'), gw.http_route('llm', 'lean-llmd', 'gw', 'pool')]
print(yaml.safe_dump_all(objs))" | kubectl apply -f -
kubectl get gateway,httproute -n lean-llmd
kubectl get httproute llm -n lean-llmd -o jsonpath='{.status.parents[0].conditions}'
```

1. Send requests to `http://gw` from inside the cluster. Read the gateway
   Pod's log line for one: which endpoint, how long?
2. Delete the EPP's TLS (`--no-secure-serving` via `extra_args`) and watch
   the gateway log. Then set the pool's `failureMode: FailClose` and repeat.
3. A second HTTPRoute on the same Gateway can send `/healthz` to an
   ordinary Service. Write it. That is the "shared infrastructure" argument
   for gateway mode.

## Compare, map

```bash
uv run lean-llmd diff --day 11
uv run lean-llmd map --day 11
```

1. **`agentgateway.md`** and llm-d's `gateway.yaml`.
2. **`handlers/server.go`**: the response cases you skipped on day 6.
3. **`common/envoy/chunking.go`**: `BodyByteLimit` and the chunk builder.
4. **`common/certs.go`**: upstream watches a mounted certificate and
   reloads it. What Kubernetes object would you mount, and what would
   rotate it?

## Check yourself

1. How many messages does your EPP send back for a POST with a 150 KB body
   and a 3-chunk streamed response?
2. Quote the gateway's log line for the TLS failure.
3. In step 2, what status did clients get under FailOpen and under
   FailClose?
4. Why can a shared gateway not use day 6's catch-all Envoy config?
