"""Day 5 - prefix-cache-aware routing.

Oracles: llm-d's hashing spec re-derived here with xxhash, properties of a
chained hash and of an LRU, and three live simulators. Each simulator keeps
its own KV cache and reports ``cached_tokens`` per request, so whether your
routing actually lands prefixes where they are cached is measured by the
server, not by your index.
"""

import random
import struct

import httpx
import pytest
import xxhash

import k8s


@pytest.fixture
def prefix(mod):
    return mod("prefix")


@pytest.fixture
def make(mod):
    ds, metrics = mod("datastore"), mod("metrics")

    def endpoint(name, address="10.0.0.1", port=8000, block_size=0, num_blocks=0):
        meta = ds.EndpointMetadata(
            ds.NamespacedName("ns", f"{name}-rank-0"), name, address, port, {}
        )
        m = metrics.Metrics(cache_block_size=block_size, cache_num_blocks=num_blocks)
        return ds.Endpoint(meta, m)

    return endpoint


def req(mod, prompt, rid="r", model="mini/sim"):
    s = mod("scheduling")
    return s.InferenceRequest(rid, model, {"model": model, "prompt": prompt})


def test_estimate_tokens_packs_four_bytes_little_endian(prefix):
    assert prefix.estimate_tokens(b"") == []
    assert prefix.estimate_tokens(b"abcd") == [0x64636261]
    assert prefix.estimate_tokens(b"abcde") == [0x64636261, 0x65]


def test_prompt_bytes(prefix):
    assert prefix.prompt_bytes({"prompt": "hello"}) == b"hello"
    assert prefix.prompt_bytes({"prompt": ["ab", "cd"]}) == b"abcd"
    chat = {
        "messages": [{"role": "system", "content": "be brief"}, {"role": "user", "content": "hi"}]
    }
    assert prefix.prompt_bytes(chat) == b"systembe briefuserhi"
    assert prefix.prompt_bytes({"input": "embeddings"}) == b""


def test_block_hashes_follow_llm_ds_chain(prefix):
    tokens = [1, 2, 3, 4, 5]
    prev = xxhash.xxh64_intdigest(b"m")
    expected = []
    for block in ([1, 2], [3, 4], [5]):
        content = xxhash.xxh64_intdigest(struct.pack(f"<{len(block)}I", *block))
        prev = xxhash.xxh64_intdigest(struct.pack("<QQ", content, prev))
        expected.append(prev)
    assert prefix.block_hashes("m", tokens, 2, 100) == expected
    assert prefix.block_hashes("m", tokens, 2, 2) == expected[:2]
    assert prefix.block_hashes("m", [], 2, 100) == []


def test_equal_hashes_mean_equal_prefixes(prefix):
    rng = random.Random(0)
    shared = [rng.randrange(2**32) for _ in range(64)]
    a = shared + [rng.randrange(2**32) for _ in range(40)]
    b = shared + [rng.randrange(2**32) for _ in range(40)]
    ha, hb = prefix.block_hashes("m", a, 16, 100), prefix.block_hashes("m", b, 16, 100)
    assert ha[:4] == hb[:4]
    assert all(x != y for x, y in zip(ha[4:], hb[4:], strict=False))
    # Same block content after a different prefix is a different key.
    c = [rng.randrange(2**32) for _ in range(16)] + shared[16:32]
    assert prefix.block_hashes("m", c, 16, 100)[1] != ha[1]
    # The model is part of every key.
    assert set(prefix.block_hashes("other", a, 16, 100)).isdisjoint(ha)


def test_the_lru_evicts_a_prefix_from_its_tail(prefix):
    index = prefix.Indexer()
    index.add([11, 12, 13], "s1", capacity=2)
    assert index.get(11) == {"s1"} and index.get(12) == {"s1"}
    assert index.get(13) == set(), "the head of a prefix must outlive its tail"
    assert index.size("s1") == 2


def test_the_index_tracks_several_servers(prefix):
    index = prefix.Indexer(default_lru_size=10)
    index.add([1, 2], "s1")
    index.add([1], "s2")
    assert index.get(1) == {"s1", "s2"}
    returned = index.get(1)
    returned.add("intruder")
    assert index.get(1) == {"s1", "s2"}, "get must return a copy"
    index.remove_server("s1")
    assert index.get(1) == {"s2"} and index.get(2) == set()
    assert index.servers() == ["s2"]


def test_eviction_cleans_the_reverse_map(prefix):
    index = prefix.Indexer()
    index.add([1, 2], "s1", capacity=2)
    index.add([3, 4], "s1")  # capacity is fixed when the LRU is created
    assert index.get(1) == set() and index.get(2) == set()
    assert index.get(3) == {"s1"} and index.get(4) == {"s1"}


def test_longest_prefix_stops_at_the_first_miss(prefix):
    producer = prefix.ApproxPrefixCacheProducer()
    producer.indexer.add([1, 2, 3], "s1")
    producer.indexer.add([1], "s2")
    producer.indexer.add([3], "s3")
    # Upstream counts every walked hash a server holds; chaining makes gaps rare.
    assert producer.match_longest_prefix([1, 2, 3]) == {"s1": 3, "s2": 1, "s3": 1}
    assert producer.match_longest_prefix([1, 9, 3]) == {"s1": 1, "s2": 1}
    assert producer.match_longest_prefix([9, 1, 2]) == {}


def test_block_size_is_tuned_and_clamped(prefix, make):
    producer = prefix.ApproxPrefixCacheProducer()
    assert producer.get_block_size([make("a", block_size=16)]) == prefix.MIN_BLOCK_SIZE_TOKENS
    assert producer.get_block_size([make("a", block_size=128)]) == 128
    assert producer.get_block_size([]) == prefix.MIN_BLOCK_SIZE_TOKENS
    fixed = prefix.ApproxPrefixCacheProducer(block_size_tokens=200, auto_tune=False)
    assert fixed.get_block_size([make("a", block_size=128)]) == 200


def test_the_producer_learns_from_its_own_picks(mod, prefix, make):
    s = mod("scheduling")
    producer, scorer = prefix.ApproxPrefixCacheProducer(), prefix.PrefixCacheScorer()
    a, b = make("a"), make("b")
    shared = "x" * 1024  # four 256-byte blocks
    first = req(mod, shared + "first question", "r1")
    producer.produce(first, [a, b])
    assert scorer.score(first, [a, b]) == {a: 0.0, b: 0.0}
    producer.pre_request(first, s.SchedulingResult({"p": s.ProfileRunResult([b])}, "p"))

    second = req(mod, shared + "another question", "r2")
    producer.produce(second, [a, b])
    info = b.attributes[prefix.PREFIX_MATCH_KEY]
    assert (info.match_blocks, info.total_blocks, info.block_size_tokens) == (4, 5, 64)
    assert scorer.score(second, [a, b]) == {a: 0.0, b: 0.8}

    other_model = req(mod, shared + "another question", "r3", model="other/model")
    producer.produce(other_model, [a, b])
    assert scorer.score(other_model, [a, b])[b] == 0.0


def test_the_lru_size_comes_from_the_servers_cache(mod, prefix, make):
    s = mod("scheduling")
    producer = prefix.ApproxPrefixCacheProducer()
    small = make("small", num_blocks=2)
    request = req(mod, "y" * 2048, "r1")  # eight blocks
    producer.produce(request, [small])
    producer.pre_request(request, s.SchedulingResult({"p": s.ProfileRunResult([small])}, "p"))
    assert producer.indexer.size("ns/small-rank-0") == 2


def test_missing_info_scores_zero(mod, prefix, make):
    a = make("a")
    assert prefix.PrefixCacheScorer().score(req(mod, "hi"), [a]) == {a: 0.0}


WORDS = [
    "alpha",
    "bravo",
    "charlie",
    "delta",
    "echo",
    "foxtrot",
    "golf",
    "hotel",
    "india",
    "juliet",
    "kilo",
    "lima",
]


def workload(groups=6, per_group=6, prefix_words=300, suffix_words=30, seed=0):
    rng = random.Random(seed)
    prefixes = [" ".join(rng.choice(WORDS) for _ in range(prefix_words)) for _ in range(groups)]
    prompts = [
        prefixes[g] + " " + " ".join(rng.choice(WORDS) for _ in range(suffix_words))
        for g in range(groups)
        for _ in range(per_group)
    ]
    rng.shuffle(prompts)
    return prompts


def route_and_measure(mod, picker, urls):
    """Route a shared-prefix workload with your scheduler; return the simulators' hit rate."""
    ds, s, p, prefix = mod("datastore"), mod("scheduling"), mod("plugins"), mod("prefix")
    metrics = mod("metrics")
    endpoints = []
    for i, url in enumerate(urls):
        host, port = url.removeprefix("http://").split(":")
        meta = ds.EndpointMetadata(
            ds.NamespacedName("ns", f"sim{i}-rank-0"), f"sim{i}", host, int(port), {}
        )
        endpoints.append(ds.Endpoint(meta))
    producer = prefix.ApproxPrefixCacheProducer()
    profile = s.SchedulerProfile(
        scorers=[s.WeightedScorer(prefix.PrefixCacheScorer(), 1.0)], picker=picker
    )
    scheduler = s.Scheduler(p.SingleProfileHandler(), {"default": profile})
    cached = total = 0
    with httpx.Client(timeout=30) as client:
        for e in endpoints:
            fams = metrics.scrape(f"http://{e.metadata.host_port}/metrics", client)
            e.update_metrics(metrics.extract(fams, metrics.VLLM_MAPPING, e.metrics, 0.0)[0])
        for i, prompt in enumerate(workload()):
            request = req(mod, prompt, f"r{i}")
            producer.produce(request, endpoints)
            result = scheduler.schedule(request, endpoints)
            producer.pre_request(request, result)
            target = result.primary().target_endpoints[0].metadata.host_port
            body = {"model": "mini/sim", "prompt": prompt, "max_tokens": 1}
            usage = client.post(f"http://{target}/v1/completions", json=body).json()["usage"]
            cached += usage["prompt_tokens_details"]["cached_tokens"]
            total += usage["prompt_tokens"]
    return cached / total


@pytest.mark.docker
def test_prefix_routing_lands_requests_where_the_cache_is(mod):
    manifests, p = mod("manifests"), mod("plugins")
    config = manifests.SimConfig(
        prefill_overhead_ms=0, prefill_per_token_ms=0, inter_token_latency_ms=0
    )
    args = manifests.sim_args(manifests.MODEL, config)

    def fresh_hit_rate(picker):
        with (
            k8s.sim_container(args) as u1,
            k8s.sim_container(args) as u2,
            k8s.sim_container(args) as u3,
        ):
            return route_and_measure(mod, picker, [u1, u2, u3])

    aware = fresh_hit_rate(p.MaxScorePicker())
    blind = fresh_hit_rate(p.RandomPicker(seed=0))
    assert aware > 0.6, f"prefix-aware routing hit only {aware:.0%} of prompt tokens"
    assert aware > blind + 0.15, f"aware {aware:.0%} vs random {blind:.0%}"
