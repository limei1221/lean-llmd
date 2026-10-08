"""Day 8 - precise prefix routing from KV events.

Oracles: the simulator's event format as documented (hand-encoded msgpack),
llm-d's xxh64 key chain re-derived with xxhash, and a live simulator. Its
own ``cached_tokens`` must agree with what your index says it holds, through
stores and evictions.
"""

import json
import struct
import time

import httpx
import msgpack
import pytest
import xxhash

import k8s


@pytest.fixture
def ev(mod):
    return mod("kvevents")


@pytest.fixture
def pr(mod):
    return mod("precise")


def batch(*events, rank=0):
    return msgpack.packb([1791480000.5, list(events), rank])


def stored(hashes, parent, tokens, block_size=4):
    return ["BlockStored", hashes, parent, tokens, block_size, None, "GPU", None, None]


def test_decode_the_array_format(ev):
    payload = batch(
        stored([11, 12], 0, list(range(8))),
        ["BlockRemoved", [11], "GPU"],
        ["AllBlocksCleared"],
        ["SomethingNew", 1, 2],
    )
    decoded = ev.decode_batch(payload)
    assert decoded.ts == 1791480000.5 and decoded.data_parallel_rank == 0
    assert decoded.events == [
        ev.BlockStored([11, 12], 0, list(range(8)), 4),
        ev.BlockRemoved([11]),
        ev.AllBlocksCleared(),
    ]


def test_decode_the_map_format(ev):
    payload = batch(
        {
            "type": "BlockStored",
            "block_hashes": [5],
            "parent_block_hash": None,
            "token_ids": [1, 2, 3, 4],
            "block_size": 4,
            "medium": "GPU",
        },
        {"type": "BlockRemoved", "block_hashes": [5]},
    )
    assert ev.decode_batch(payload).events == [
        ev.BlockStored([5], 0, [1, 2, 3, 4], 4),
        ev.BlockRemoved([5]),
    ]


def test_parse_topic(ev):
    assert ev.parse_topic("kv@10.0.0.1:8000@mini/sim") == ("10.0.0.1:8000", "mini/sim")
    assert ev.parse_topic("kv@10.0.0.1:8000@odd@name") == ("10.0.0.1:8000", "odd@name")
    for bad in ("x@10.0.0.1:8000@m", "kv@10.0.0.1:8000", "kv@@m"):
        with pytest.raises(ValueError):
            ev.parse_topic(bad)


def test_with_kv_events_binds_a_port(ev, mod):
    manifests = mod("manifests")
    original = manifests.sim_deployment()
    patched = ev.with_kv_events(original, port=6000)
    (c,) = patched["spec"]["template"]["spec"]["containers"]
    args = c["args"]
    assert args[args.index("--zmq-endpoint") + 1] == "tcp://*:6000"
    assert args[args.index("--event-batch-size") + 1] == "1"
    assert {"name": "kv-events", "containerPort": 6000, "protocol": "TCP"} in c["ports"]
    assert "--zmq-endpoint" not in original["spec"]["template"]["spec"]["containers"][0]["args"]


def test_request_keys_follow_llm_ds_xxh64_chain(pr):
    seed_hash = xxhash.xxh64_intdigest(b"")
    start = xxhash.xxh64_intdigest(struct.pack("<QQ", seed_hash, 1) + b"m")
    tokens = list(range(10))
    k1 = xxhash.xxh64_intdigest(struct.pack("<Q", start) + struct.pack("<4I", 0, 1, 2, 3))
    k2 = xxhash.xxh64_intdigest(struct.pack("<Q", k1) + struct.pack("<4I", 4, 5, 6, 7))
    assert pr.init_key("m") == start
    assert pr.request_keys(None, tokens, "m", 4) == [k1, k2], "the partial tail has no key"
    assert pr.request_keys(k1, tokens[4:], "m", 4) == [k2], "a parent continues the chain"


def test_the_index_matches_from_the_first_block(pr):
    index = pr.KVBlockIndex()
    index.add([101, 102, 103], [1, 2, 3], "a")
    index.add([201], [1], "b")
    index.add([303], [3], "c")
    assert index.longest_prefix([1, 2, 3]) == {"a": 3, "b": 1}
    assert index.longest_prefix([9, 1]) == {}
    index.evict(102, "a")
    assert index.longest_prefix([1, 2, 3]) == {"a": 1, "b": 1}, "a gap ends the run"
    index.evict(999, "a")  # unknown keys are fine
    index.clear("a")
    assert index.longest_prefix([1]) == {"b": 1}
    assert index.get_request_key(201) == 1


def test_events_build_the_index(pr):
    index = pr.KVBlockIndex()
    processor = pr.EventProcessor(index, block_size=4)
    topic = "kv@10.0.0.1:8000@m"
    tokens = list(range(12))
    processor.process(topic, batch(stored([11, 12], 0, tokens[:8])))
    processor.process(topic, batch(stored([13], 12, tokens[8:12])))
    keys = pr.request_keys(None, tokens, "m", 4)
    assert index.longest_prefix(keys) == {"10.0.0.1:8000": 3}
    processor.process(topic, batch(stored([99], 777, [1, 2, 3, 4])))  # unknown parent: skipped
    processor.process(topic, batch(["BlockRemoved", [12], "GPU"]))
    assert index.longest_prefix(keys) == {"10.0.0.1:8000": 1}
    processor.process(topic, batch(["AllBlocksCleared"]))
    assert index.longest_prefix(keys) == {}


def test_the_producer_writes_real_matches(mod, pr):
    ds, s, prefix = mod("datastore"), mod("scheduling"), mod("prefix")

    def endpoint(ip):
        meta = ds.EndpointMetadata(ds.NamespacedName("ns", ip), ip, ip, 8000, {})
        return ds.Endpoint(meta)

    class FixedTokens:
        def tokenize(self, request, host_port):
            return list(range(10))

    index = pr.KVBlockIndex()
    keys = pr.request_keys(None, list(range(10)), "m", 4)
    index.add([1, 2], keys, "10.0.0.2:8000")
    a, b = endpoint("10.0.0.1"), endpoint("10.0.0.2")
    producer = pr.PrecisePrefixCacheProducer(index, FixedTokens(), block_size=4)
    producer.produce(s.InferenceRequest("r", "m", {"model": "m", "prompt": "x"}), [a, b])
    assert a.attributes[prefix.PREFIX_MATCH_KEY] == prefix.PrefixCacheMatchInfo(0, 2, 4)
    assert b.attributes[prefix.PREFIX_MATCH_KEY] == prefix.PrefixCacheMatchInfo(2, 2, 4)


def test_install_swaps_the_approximate_producer(mod, pr):
    epp, prefix = mod("epp"), mod("prefix")
    rt = epp.build(epp.Options("pool", "ns", features=("precise",)))
    assert not any(isinstance(p, prefix.ApproxPrefixCacheProducer) for p in rt.producers)
    assert any(isinstance(p, pr.PrecisePrefixCacheProducer) for p in rt.producers)
    assert rt.prefix is None and rt.ticks


@pytest.mark.docker
def test_the_index_agrees_with_the_simulators_cache(mod, ev, pr):
    """Store, hit, evict: at every step, your index must say what the server's cache says."""
    manifests = mod("manifests")
    s = mod("scheduling")
    config = manifests.SimConfig(kv_cache_blocks=16, block_size=16)
    args = [
        *manifests.sim_args(manifests.MODEL, config),
        "--zmq-endpoint",
        "tcp://*:5557",
        "--event-batch-size",
        "1",
    ]
    with k8s.sim_container(args, extra_ports=(5557,)) as url:
        host, port = url.removeprefix("http://").split(":")
        zmq_port = k8s.published_port(url, 5557)
        index = pr.KVBlockIndex()
        subscriber = pr.Subscriber(f"tcp://{host}:{zmq_port}", pr.EventProcessor(index, 16))
        subscriber.start()
        time.sleep(0.5)  # a SUB socket only sees messages sent after it connected
        pod = "127.0.0.1:8000"  # the topic carries POD_IP and the simulator's own port
        tokenizer = pr.RenderTokenizer()

        def ask(prompt):
            body = {"model": manifests.MODEL, "prompt": prompt, "max_tokens": 1}
            return httpx.post(url + "/v1/completions", json=body).json()["usage"]

        def held(prompt):
            req = s.InferenceRequest("r", manifests.MODEL, {"prompt": prompt})
            tokens = tokenizer.tokenize(req, f"{host}:{port}")
            keys = pr.request_keys(None, tokens, manifests.MODEL, 16)
            return index.longest_prefix(keys).get(pod, 0)

        try:
            first = " ".join(f"alpha{i % 9}" for i in range(100))  # 6 full blocks
            ask(first)
            k8s.wait_until(
                lambda: held(first) == 6, timeout=10, interval=0.2, what="six blocks indexed"
            )
            assert ask(first)["prompt_tokens_details"]["cached_tokens"] == 6 * 16
            # A different prompt that needs 14 blocks forces the cache (16) to evict.
            ask(" ".join(f"beta{i % 7}" for i in range(230)))
            k8s.wait_until(
                lambda: held(first) < 6,
                timeout=10,
                interval=0.2,
                what="evictions to reach the index",
            )
            remaining = held(first)
            assert ask(first)["prompt_tokens_details"]["cached_tokens"] == remaining * 16
        finally:
            subscriber.stop()


@pytest.mark.kind
def test_precise_routing_on_kind(mod, ev, package):
    if not k8s.image_present_in_kind():
        pytest.skip("the lean-llmd:dev image is not in kind; run: uv run lean-llmd image")
    manifests, disc, deploy = mod("manifests"), mod("discovery"), mod("deploy")
    flags = ("--feature", "precise")
    with k8s.temp_namespace("day08") as ns:
        k8s.apply(
            [
                ev.with_kv_events(manifests.sim_deployment(namespace=ns, replicas=3)),
                disc.inference_pool("pool", ns, {"app": "sim"}, manifests.SIM_PORT, "epp"),
                *deploy.epp_objects("epp", ns, "pool", package=package, extra_args=flags),
            ]
        )
        for name in ("sim", "epp"):
            k8s.kubectl("rollout", "status", f"deployment/{name}", "-n", ns, "--timeout=240s")
        out = k8s.run_python_in_cluster(
            ns,
            """
            import httpx, json, random, time
            rng = random.Random(0)
            prefixes = [" ".join(f"t{g}w{rng.randrange(50)}" for _ in range(300)) for g in range(4)]
            cached = total = 0
            for i in range(24):
                prompt = prefixes[i % 4] + " " + " ".join(str(rng.random()) for _ in range(10))
                usage = httpx.post("http://epp/v1/completions", timeout=30, json={
                    "model": "mini/sim", "prompt": prompt, "max_tokens": 1}).json()["usage"]
                if i >= 4:
                    cached += usage["prompt_tokens_details"]["cached_tokens"]
                    total += usage["prompt_tokens"]
                time.sleep(0.2)  # let the BlockStored event arrive before the next request
            print(json.dumps({"hit": cached / total}))
            """,
        )
    hit = json.loads(out.strip().splitlines()[-1])["hit"]
    assert hit > 0.8, f"after one cold request per prefix, the rest should hit: {hit:.0%}"
