# Day 6 — The endpoint picker behind Envoy

~4 hours · laptop (kind)

> **Goal:** put days 2–5 behind a real proxy. Envoy owns every client
> connection; for each request it asks your endpoint picker (EPP) one
> question over gRPC, "which ip:port?", and forwards the request there. You
> package the EPP as an image, give it just enough RBAC, run Envoy next to
> it in one Pod, and route real traffic on kind.

## What you build

| File | What to implement |
|---|---|
| `src/lean_llmd/extproc.py` | `parse_request`, `Director`, `destination_metadata`, `routing_response`, `continue_response`, `immediate_response`, `header_map`, `StreamingServer.Process` |
| `src/lean_llmd/epp.py` | `parse_options`, `build_scheduler`, `refresh_metrics_once`, `forget_removed_endpoints`, `build`, `serve`, `run`, `main` |
| `src/lean_llmd/deploy.py` | `epp_rbac`, `envoy_config_map`, `epp_deployment`, `epp_service`, `epp_objects` |

`envoy_config()` in `deploy.py` is **given**: 150 lines of nesting teach
less than the five settings that matter, which are described below. Read it.

## The request path

```
client ──HTTP──▶ Service epp:80 ──▶ Pod epp ┬─ envoy :8081 ──HTTP──▶ sim Pod ip:8000
                                            │     │ ▲
                                            │ gRPC│ │ "x-gateway-destination-endpoint: 10.244.1.7:8000"
                                            │     ▼ │
                                            └─ epp  :9002  (your code: datastore, scheduler)
```

This is llm-d's **standalone mode**: no Gateway API, Envoy as a sidecar.
Gateway mode puts a shared gateway (Istio, agentgateway, GKE) in front
instead, and wires it to the EPP through the InferencePool's
`endpointPickerRef`.

## Part A — the protocol

Envoy's `ext_proc` filter opens one gRPC stream per HTTP request and sends
it the request in pieces: `request_headers`, then `request_body`. You must
answer each message you receive, in order.

Five settings in `envoy_config()` make the scheme work:

1. **`request_body_mode: BUFFERED`.** Envoy waits for the whole body and
   sends it in one message. You need the body (the model, the prompt)
   before you can choose. llm-d uses `FULL_DUPLEX_STREAMED` instead, which
   is faster for big bodies but harder to write.
2. **`response_header_mode: SKIP`, `response_body_mode: NONE`.** You do
   not touch responses, so tokens stream straight back.
3. **Cluster `original_destination_cluster`, type `ORIGINAL_DST`,
   `use_http_header: true`.** Envoy sends the request to whatever
   `ip:port` the `x-gateway-destination-endpoint` header says. No list of
   backends lives in Envoy at all.
4. **`clear_route_cache` in your answer.** Envoy matched a route when the
   headers arrived, before your header existed. Clearing the cache makes it
   match again with your header, which matters whenever routes depend on
   it (a shared gateway routing by model or destination). Here there is one
   catch-all route; question 4 asks you to find out whether it matters.
5. **`failure_mode_allow: false`.** If the EPP is down, fail the request.
   llm-d defaults to fail-open, which only helps with a fallback route
   (day 13 builds one).

Also note `message_timeout: 120s`: how long Envoy waits for each answer.
Today answers come in milliseconds, but on day 10 a request may wait in the
EPP's queue, and Envoy must not give up first.

### Step 1. `parse_request` and the response builders

The answer is a `ProcessingResponse` whose `request_body` (or
`request_headers`) field holds a `CommonResponse` with the header mutation
and `clear_route_cache=True`. Put the same value in
`dynamic_metadata["envoy.lb"]["x-gateway-destination-endpoint"]`; that is
the field the Gateway API Inference Extension's protocol defines, and
gateways other than this static Envoy read it there.

### Step 2. `Director`

Candidates from the datastore → producers' `produce` → `schedule` →
producers' `pre_request` → the primary endpoint's `ip:port`. Hold a lock
around all of it: producers write per-request data onto the **shared**
`Endpoint` objects.

### Step 3. `StreamingServer.Process`

A generator: for each incoming message, yield exactly one response.

- Headers with `end_of_stream` (a GET such as `/v1/models`) have no body.
  Route them now, round-robin.
- A body may arrive in several messages; join them until `end_of_stream`.
- The routing answer also sets any headers plugins left in
  `request.set_headers` (empty today; day 9 uses it).
- A bad body answers 400, an empty pool 503, a `RequestRejected` its own
  status (day 10's 429), anything else 500, all through
  `immediate_response`. Raising instead would reset the stream, and the
  client would get an unexplained error.

## Part B — the process and its objects

### Step 4. `epp.py`

`build` assembles a `Runtime`: a `Datastore`, a scheduler with llm-d's
default profile (queue + KV + prefix, weight 1 each, max-score picker) and
its producers. `run` starts your day-3 `Controller` and the gRPC server,
then loops: scrape every endpoint's metrics every 50 ms (upstream polls at
the same interval), run any `ticks`, update health.

The split exists for days 8–13. `--feature precise` makes `build` import
the sibling module `precise` and call its `install(runtime, argv)`, which
may swap producers, profiles, the servicer or add ticks before `run`. That
is how upstream registers plugins by name, and it means later days never
edit this file. The test `test_build_installs_features_by_module_name`
checks you import by name.

> **Trap:** `python -m lean_llmd.epp` runs this file as `__main__`, so a
> feature that imports `lean_llmd.epp` would get a *second* copy of it. The
> stub's `__main__` block imports `main` from the real module; keep it.

Report gRPC health `SERVING` only once the pool has synced. The Pod's
readiness probe uses Kubernetes' native gRPC probe, so an EPP that does not
know its endpoints yet gets no traffic.

### Step 5. `deploy.py`

- **RBAC.** A ServiceAccount, a Role allowing `get`, `list` and `watch` on
  Pods and InferencePools **in this namespace**, and a RoleBinding. Nothing
  else: the EPP never writes.
- **One Pod, two containers.** They share a network namespace, so Envoy
  reaches the EPP on `127.0.0.1:9002`. Envoy's config comes from a
  ConfigMap mounted as a file.
- **A Service** exposing Envoy's port as 80 and the EPP's gRPC port.

## Part C — on the cluster

```bash
uv run lean-llmd test --day 6        # unit tests, a real gRPC round trip, then kind
uv run lean-llmd image               # rebuild after every code change: the cluster runs the image
uv run lean-llmd render pool | kubectl apply -f -
uv run lean-llmd render epp  | kubectl apply -f -
kubectl rollout status deploy/epp -n lean-llmd
```

1. **Trace a request** (your plan's week-2 exercise). Send one:

   ```bash
   kubectl run c -n lean-llmd --rm -i --restart=Never --image=curlimages/curl -- \
     curl -s http://epp/v1/completions -H 'content-type: application/json' \
     -d '{"model":"mini/sim","prompt":"hello","max_tokens":5}'
   ```

   Then find it at each hop: the client's DNS name → `kubectl get svc epp` →
   `kubectl get endpointslices -l kubernetes.io/service-name=epp` → the
   Envoy container (`kubectl logs deploy/epp -c envoy`) → your EPP's choice →
   which sim's `vllm:request_success_total` went up. Draw it.
2. **Look inside Envoy.** `kubectl port-forward deploy/epp 19000` then
   `curl localhost:19000/clusters | grep original_destination`. The cluster
   learns hosts only as your EPP names them.
3. **RBAC.** `kubectl auth can-i --list -n lean-llmd --as=system:serviceaccount:lean-llmd:epp`.
   Delete the RoleBinding and restart the EPP. Read `kubectl logs deploy/epp -c epp`:
   which call fails first, and with what HTTP status?
4. **Stop the picker.** Send requests in a loop while you run
   `kubectl exec deploy/epp -c epp -- python -c "import os, signal; os.kill(1, signal.SIGTERM)"`.
   The container exits and restarts. What status does the client see
   meanwhile, and which Envoy setting decides that?
5. **Readiness of the EPP.** Delete the InferencePool. Does the EPP Pod stay
   Ready? Should it?

## Run, compare, map

```bash
uv run lean-llmd diff --day 6
uv run lean-llmd map --day 6
```

1. **`docs/architecture/core/router/proxy.md`.** Standalone vs gateway.
2. **`handlers/server.go`, `Process`.** Find the `switch` on message types.
   Upstream also handles response headers and bodies. What does it read
   from the response, and why does a router care?
3. **`handlers/request.go`.** `generateRequestHeaderResponse` and
   `generateMetadata`: your answer, field for field.
4. **`requestcontrol/director.go`.** `HandleRequest`. List what upstream
   does that yours does not (admission, flow control, ...).
5. **The standalone chart's `values.yaml`.** Diff its Envoy config with
   `envoy_config()`.

## Check yourself

1. How many gRPC messages does Envoy send your EPP for one POST with a
   small body, and how many for a GET to `/v1/models`?
2. In step 2, how many hosts does `original_destination_cluster` list after
   you have sent requests through it, and where did they come from?
3. In step 3, quote the error your EPP logged.
4. Remove `clear_route_cache` from your answer, rebuild, and send a
   request. What status comes back? Explain it from *when* Envoy reads the
   header: at route matching, or at host selection in the cluster.
5. The EPP serialises scheduling with one lock. At 2 ms per decision, what
   is the ceiling in requests per second for one EPP replica? Name one way
   to raise it.
