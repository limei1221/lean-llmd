# Day 8 — Precise prefix routing from KV events

~4 hours · laptop (Docker + kind)

> **Goal:** replace day 5's guess with the truth. Every model server
> publishes what it caches and evicts on a ZMQ socket; your EPP subscribes
> to all of them, keeps an index of which replica holds which block, and
> routes on it.

## What you build

| File | What to implement |
|---|---|
| `src/lean_llmd/kvevents.py` | `decode_event`, `decode_batch`, `parse_topic`, `with_kv_events` |
| `src/lean_llmd/precise.py` | `init_key`, `request_keys`, `KVBlockIndex`, `EventProcessor`, `Subscriber`, `SubscriberManager`, `RenderTokenizer`, `PrecisePrefixCacheProducer`, `install` |

## Why day 5 was only approximate

Day 5's index learns from its own decisions. It cannot see that a replica
evicted a block under memory pressure, or that it holds a prefix because
someone else sent it there. It hashes bytes, not the server's tokens, so its
blocks only roughly line up with the server's. Precise routing fixes all
three, at a price: a subscription per replica and a tokenizer call per
request.

## Part A — the events

The simulator (and vLLM with `--kv-events-config`) publishes on a ZMQ
**PUB** socket. Each message has three frames:

| Frame | Content |
|---|---|
| topic | `kv@10.244.1.7:8000@mini/sim` — the replica's `ip:port` and the model |
| sequence | 8 bytes, big-endian, increasing |
| payload | msgpack `[timestamp, [event, ...], data_parallel_rank]` |

Three event types matter:

| Event | Fields you use |
|---|---|
| `BlockStored` | `block_hashes` (the engine's), `parent_block_hash`, `token_ids`, `block_size` |
| `BlockRemoved` | `block_hashes` |
| `AllBlocksCleared` | — |

### Step 1. `decode_event`, `decode_batch`, `parse_topic`

Two encodings exist: positional arrays (the default) and maps with named
fields (vLLM's newer format, `--use-vllm-map-event-format`). Handle both,
and skip event types you do not know rather than failing the batch. The
model name may contain `@`.

### Step 2. `with_kv_events`

The simulator publishes nowhere useful by default. Patch its Deployment:
`--zmq-endpoint tcp://*:5557` (the `*` makes it **bind**, so subscribers
dial each Pod) and `--event-batch-size 1` (otherwise it batches for up to a
second, and your index lags).

## Part B — the index

### Step 3. Request keys

The engine's block hashes come from its own hash function and seed. You
cannot recompute them, and you must not depend on them: vLLM and SGLang
differ, and a seed change would break you. So the EPP computes its **own**
keys from token ids, with the same chaining idea as day 5:

```
start = xxh64(le64(xxh64(seed)) + le64(len(model)) + model)
key_i = xxh64(le64(key_{i-1}) + tokens_i as le32)     # full blocks only
```

Every `BlockStored` carries its tokens, so the EPP can compute the keys of
the blocks it describes and remember which engine hash means which key.

> **Trap:** a `BlockStored` usually extends a cached prefix. Its first
> block's parent is `parent_block_hash`, an **engine** hash. Look up the
> request key you stored for it and continue the chain from there. If you
> never saw the parent, skip the event: guessing would corrupt the index.

### Step 4. `KVBlockIndex`

Three maps: engine key → request key, request key → pods, pod → engine
keys (to clear a pod). `longest_prefix` counts, per pod, the blocks it holds
**consecutively from the first**. A block after a gap is useless, because
vLLM can only reuse a prefix. Day 5 stopped at the first block *nobody*
held; this is stricter, per pod.

### Step 5. `EventProcessor`, `Subscriber`, `SubscriberManager`

One SUB socket per replica, subscribed to the `kv@` prefix. Poll with a
timeout so `stop()` is noticed. The manager follows the datastore every tick:
new endpoints get a subscriber, departed ones are stopped **and cleared from
the index**.

> **Trap:** ZMQ PUB/SUB drops messages sent before a subscriber connects.
> A new EPP replica starts with an empty index until traffic refills it.
> Upstream adds a replay socket for exactly this; the simulator has one too
> (`--kv-events-replay-endpoint`).

## Part C — the producer

### Step 6. `RenderTokenizer` and `PrecisePrefixCacheProducer`

To look a request up you need **the server's** token ids. Ask the server:
`POST /v1/completions/render` returns them without generating. llm-d's
`token-producer` does the same against vLLM's `/render`.

The producer writes the same `PrefixCacheMatchInfo` day 5 wrote, so
`PrefixCacheScorer` needs no change. It learns nothing in `pre_request`:
the server's events will say what it actually cached.

### Step 7. `install`

`--feature precise` replaces the approximate producer with this one and
adds a tick that syncs subscribers. Flags: `--kv-events-port`,
`--kv-block-size` (the server's block size, 16 here).

## Run it

```bash
uv run lean-llmd test --day 8
uv run lean-llmd image
```

The Docker test is the strict one: a real simulator with a 16-block cache.
Store a prompt, check the index says 6 blocks and the server says 96 cached
tokens; force evictions with another prompt; check the index and the server
still agree.

Then on the cluster (sims publishing events, your EPP with the feature on):

```bash
uv run python -c "
import yaml
from lean_llmd import manifests as m, kvevents as ev, deploy
objs = [ev.with_kv_events(m.sim_deployment()),
        *deploy.epp_objects('epp', 'lean-llmd', 'pool', extra_args=('--feature', 'precise'))]
print(yaml.safe_dump_all(objs))" | kubectl apply -f -
kubectl rollout status deploy/sim deploy/epp -n lean-llmd
```

1. **Watch the events.** Subscribe to one simulator from inside the cluster
   and send it a request from another terminal:

   ```bash
   IP=$(kubectl get pods -n lean-llmd -l app=sim -o jsonpath='{.items[0].status.podIP}')
   kubectl run sub -n lean-llmd --rm -i --restart=Never --image=lean-llmd:dev \
     --image-pull-policy=IfNotPresent --command -- python -c "
   import zmq
   from lean_llmd.kvevents import decode_batch
   s = zmq.Context().socket(zmq.SUB); s.connect('tcp://$IP:5557'); s.subscribe(b'kv@')
   while True:
       topic, seq, payload = s.recv_multipart()
       print(topic.decode(), decode_batch(payload).events)"
   ```

2. **Benchmark it.**

   ```bash
   uv run lean-llmd bench --routers epp --output results/bench-approx.json
   uv run lean-llmd bench --routers epp --sim-kv-events \
     --epp-args "--feature precise" --output results/bench-precise.json
   ```

3. **Shrink the cache** (`kv_cache_blocks=64`) and repeat both. Which
   design loses more hits, and why?

## Compare, map

```bash
uv run lean-llmd diff --day 8
uv run lean-llmd map --day 8
```

1. **`kv-indexer.md`**: the design.
2. **The simulator's `docs/kv-cache.md`**: the wire format, both encodings.
3. **`pkg/kvevents/pool.go`, `processEventBatch`**: find
   `GetRequestKey` and `TokensToKVBlockKeys`. Upstream also tracks device
   tiers (GPU, CPU offload) and deduplicates events; you did neither.
4. **`token_processor.go`**: `hashXXH64` is your `request_keys`. What is
   the *default* hash algorithm, and why might upstream prefer it?
5. **`subscriber_manager.go`**: `EnsureSubscriber` / `RemoveSubscriber`.

## Check yourself

1. A prompt is 100 simulator tokens. How many `BlockStored` blocks does it
   produce at block size 16, and how many request keys do you compute?
2. Send prompt A, then A + 40 more tokens. Quote the second request's
   `BlockStored`: what is its `parent_block_hash`, and which of your keys
   does it map to?
3. In the Docker test, how many of the first prompt's 6 blocks survived the
   eviction on your run?
4. With 3 replicas and 4 tenants, what hit rate did precise routing reach in
   experiment 2, against approximate?
5. Why is "tokenize via the first endpoint" safe here but risky with a mix
   of model versions in one pool?
