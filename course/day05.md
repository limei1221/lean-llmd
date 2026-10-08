# Day 5 — Prefix-cache-aware routing

~4 hours · laptop (Docker + kind)

> **Goal:** send each request to the replica most likely to hold its
> prompt's prefix in KV cache. This is what an LLM router has that a
> Service never will, and on the simulator it turns a 1.2 s prefill
> into 40 ms.

## What you build

| File | What to implement |
|---|---|
| `src/lean_llmd/prefix.py` | `prompt_bytes`, `estimate_tokens`, `block_hashes`, `Indexer`, `ApproxPrefixCacheProducer`, `PrefixCacheScorer` |

## The idea

vLLM caches KV in fixed-size blocks and reuses a block when a new prompt
starts with the same tokens. The cache is per replica. Requests that share a
system prompt, a long document, or a chat history only hit if they land on
the replica that served the prefix before.

The router cannot see the replicas' caches. The **approximate** design
keeps its own guess instead: "I sent these blocks to that replica, so it
probably still has them." (The **precise** design subscribes to KV-cache
events from every replica; it is a later day.)

## Part A — keys

### Step 1. `prompt_bytes` and `estimate_tokens`

The real token ids need a tokenizer, so llm-d's default `estimate` backend
treats every 4 bytes of prompt as one pseudo-token. Equal byte prefixes give
equal token prefixes, which is all a cache key needs.

### Step 2. `block_hashes`

```
prev = xxh64(model_name)
for each block of block_size tokens (the last may be short):
    content = xxh64(block as little-endian uint32 bytes)
    prev    = xxh64(le64(content) + le64(prev))
    emit prev
```

Because each hash folds in the previous one, hash *i* stands for the
**whole prefix** up to block *i*. Two prompts that share 3 blocks share
exactly 3 hashes. The model name is in every key, so two models never match.

## Part B — the index

### Step 3. `Indexer`

Two maps: hash → set of servers (to answer lookups) and server → LRU of
hashes (to bound memory and to mimic the server's own eviction).

> **Trap:** insert a request's hashes **last first**. Then the first block
> is the most recently used and the tail is evicted first. That is how vLLM
> evicts too, because a block's prefix is always at least as popular as
> the block. Insert in order and the LRU evicts block 0 first, which
> invalidates every hash after it.

When an LRU evicts a hash, remove that server from the hash's set, and drop
empty sets.

### Step 4. `ApproxPrefixCacheProducer`

It runs at two points around the scheduler:

- **`produce`** (before): hash the request, find how many leading blocks
  each server holds (`match_longest_prefix` stops at the first hash nobody
  holds), and write a `PrefixCacheMatchInfo` onto every endpoint.
- **`pre_request`** (after): add the hashes to the chosen endpoint's LRU.
  The index learns only from its own decisions.

Two numbers come from the replicas' metrics when `auto_tune` is on:

- block size from `cache_block_size`, never below 64 tokens (llm-d clamps
  it there, because hashing per 16 tokens costs EPP CPU on long prompts);
- the LRU's capacity from `cache_num_blocks`.

### Step 5. `PrefixCacheScorer`

`match_blocks / total_blocks`. Weighted equally with the two load scorers,
a full prefix match is worth as much as an empty queue.

## Part C — on the simulators

```bash
uv run lean-llmd test --day 5
```

The Docker test starts three simulators, routes a shared-prefix workload
through your producer and scorer, and reads `cached_tokens` from each
response, which comes from the **simulator's** cache. Prefix-aware routing
must clearly beat random.

Now look at why a Service cannot do this. Find the rules kube-proxy wrote
for yesterday's Service on a node:

```bash
docker exec lean-llmd-worker iptables-save | grep 'lean-llmd/sim'
```

Find `-m statistic --mode random --probability`. Then answer: does the
choice happen per request, or per TCP connection? It is made when the
connection opens, so a client that keeps its connection alive (most do)
sends every request to the same Pod. Prove it with the simulators' own
counters:

```bash
uv run lean-llmd image      # the course image, with Python and httpx (any time from today)
served() { uv run python -c "
from lean_llmd_course.kube import sim_counters
print(sorted(int(c['served']) for c in sim_counters('lean-llmd').values()))"; }
send() { kubectl run send -n lean-llmd --rm -i --restart=Never --quiet \
  --image=lean-llmd:dev --image-pull-policy=IfNotPresent --command -- python -c "$1"; }

served
send "
import httpx
with httpx.Client() as c:          # one keep-alive connection
    for _ in range(30): c.post('http://sim/v1/completions', json={'model': 'mini/sim', 'prompt': 'x', 'max_tokens': 1})"
served
send "
import httpx
for _ in range(30):                # a new connection each time
    httpx.post('http://sim/v1/completions', json={'model': 'mini/sim', 'prompt': 'x', 'max_tokens': 1})"
served
```

## Run, compare, map

```bash
uv run lean-llmd diff --day 5
uv run lean-llmd map --day 5
```

1. **`prefix-cache-aware-routing.md`.** Approximate vs precise.
2. **`tokenizer/estimate.go`.** `packBytes`, and how chat messages become
   bytes (`appendChatMessage`). Yours is simpler; what does upstream add?
3. **`prefixhash/hashing.go`.** Your `block_hashes`, plus `CacheSalt`: why
   would two tenants want different keys for the same prompt?
4. **`approximateprefix/indexer.go`.** Find `idx := len(hashes) - 1`.
5. **`approximateprefix/plugin.go`.** `minBlockSizeTokens` and the issue it
   cites.

## Check yourself

1. A prompt is 1,000 bytes. How many pseudo-tokens, and how many 64-token
   blocks? How many are full?
2. The simulators use 16-token blocks of *real-ish* tokens (about one per
   word); your index uses 64-token blocks of 4 bytes. Roughly how many
   simulator blocks does one of your blocks cover for English text with
   6-character words? What does that do to the LRU capacity you copy from
   `cache_num_blocks`?
3. In the Docker test, what hit rates do prefix-aware and random routing
   reach on your machine?
4. Why does `pre_request` index the request *before* the replica has
   actually cached anything? What goes wrong if the request then fails?
