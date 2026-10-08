"""Day 7 - a streaming load generator.

Oracles: Python's ``statistics`` module for percentiles, hand-built records
for the summary, and a live simulator whose own counters (TTFT histogram,
prefix-cache hits) your client-side numbers must agree with.
"""

import asyncio
import json
import math
import re
import statistics

import httpx
import pytest

import k8s


@pytest.fixture
def lg(mod):
    return mod("loadgen")


def test_percentile_matches_statistics(lg):
    data = [5.0, 1.0, 9.0, 3.0, 7.0, 2.0, 8.0]
    cut_points = statistics.quantiles(data, n=100, method="inclusive")
    for p in (1, 25, 50, 90, 99):
        assert lg.percentile(data, p) == pytest.approx(cut_points[p - 1])
    assert lg.percentile(data, 0) == 1.0 and lg.percentile(data, 100) == 9.0
    assert lg.percentile([4.0], 99) == 4.0
    assert math.isnan(lg.percentile([], 50))


def test_the_workload_shares_one_prefix_per_group(lg):
    specs = lg.shared_prefix_workload(3, 4, prefix_words=20, suffix_words=5, max_tokens=7, seed=1)
    assert len(specs) == 12 and all(s.max_tokens == 7 for s in specs)
    by_group = {}
    for s in specs:
        words = s.prompt.split(" ")
        assert len(words) == 25
        by_group.setdefault(s.group, []).append(words)
    assert sorted(by_group) == [0, 1, 2]
    prefixes = {g: {tuple(w[:20]) for w in ws} for g, ws in by_group.items()}
    assert all(len(p) == 1 for p in prefixes.values()), "a group must share its prefix"
    assert len({next(iter(p)) for p in prefixes.values()}) == 3, "groups must differ"
    assert [s.group for s in specs] != sorted(s.group for s in specs), "requests must be shuffled"


def test_the_workload_is_reproducible(lg):
    a = lg.shared_prefix_workload(2, 3, 10, 3, 5, seed=7)
    assert a == lg.shared_prefix_workload(2, 3, 10, 3, 5, seed=7)
    assert a != lg.shared_prefix_workload(2, 3, 10, 3, 5, seed=8)


def test_summarize(lg):
    R = lg.RequestRecord
    records = [
        R(
            group=0,
            start=0,
            ok=True,
            ttft=0.1,
            e2e=0.5,
            output_tokens=5,
            prompt_tokens=100,
            cached_tokens=80,
        ),
        R(
            group=0,
            start=0,
            ok=True,
            ttft=0.3,
            e2e=0.3,
            output_tokens=1,
            prompt_tokens=100,
            cached_tokens=0,
        ),
        R(group=1, start=0, ok=False, error="boom"),
    ]
    s = lg.summarize(records, wall_seconds=2.0)
    assert (s.requests, s.errors) == (3, 1)
    assert s.ttft_p50 == pytest.approx(0.2)
    assert s.tpot_mean == pytest.approx(0.1), (
        "(0.5 - 0.1) / (5 - 1); one-token requests have no TPOT"
    )
    assert s.e2e_p50 == pytest.approx(0.4)
    assert s.throughput_rps == pytest.approx(1.0)
    assert s.cache_hit_rate == pytest.approx(0.4)


def sse(*chunks):
    return "".join(f"data: {json.dumps(c)}\n\n" for c in chunks) + "data: [DONE]\n\n"


def test_send_one_reads_the_stream(lg):
    body = sse(
        {"choices": [{"index": 0, "text": ""}], "usage": None},
        {"choices": [{"index": 0, "text": "Hi"}], "usage": None},
        {"choices": [{"index": 0, "text": " there"}], "usage": None},
        {
            "choices": [],
            "usage": {
                "prompt_tokens": 12,
                "completion_tokens": 2,
                "prompt_tokens_details": {"cached_tokens": 8},
            },
        },
    )
    seen = {}

    def handler(request):
        seen.update(json.loads(request.content))
        return httpx.Response(200, text=body, headers={"content-type": "text/event-stream"})

    async def go():
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            return await lg.send_one(
                client, "http://x/v1/completions", "m", lg.RequestSpec("p", 2, 3)
            )

    record = asyncio.run(go())
    assert seen["stream"] is True and seen["stream_options"] == {"include_usage": True}
    assert seen["ignore_eos"] is True
    assert (seen["model"], seen["prompt"], seen["max_tokens"]) == ("m", "p", 2)
    assert record.ok and record.group == 3
    assert (record.prompt_tokens, record.output_tokens, record.cached_tokens) == (12, 2, 8)
    assert 0 < record.ttft <= record.e2e


def test_send_one_reports_failures(lg):
    async def go(handler):
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            return await lg.send_one(
                client, "http://x/v1/completions", "m", lg.RequestSpec("p", 2, 0)
            )

    failed = asyncio.run(go(lambda r: httpx.Response(503, text="no endpoints")))
    assert not failed.ok and "503" in failed.error

    def explode(request):
        raise httpx.ConnectError("refused")

    broken = asyncio.run(go(explode))
    assert not broken.ok and broken.error


def histogram_mean(text, name):
    total = float(re.search(rf"^{name}_sum{{.*}} (\S+)$", text, re.M).group(1))
    count = float(re.search(rf"^{name}_count{{.*}} (\S+)$", text, re.M).group(1))
    return total / count


@pytest.mark.docker
def test_client_numbers_agree_with_the_server(mod, lg):
    manifests = mod("manifests")
    config = manifests.SimConfig(
        prefill_overhead_ms=30, prefill_per_token_ms=1, inter_token_latency_ms=5
    )
    specs = lg.shared_prefix_workload(2, 5, prefix_words=200, suffix_words=10, max_tokens=8)
    with k8s.sim_container(manifests.sim_args(manifests.MODEL, config)) as url:
        records = asyncio.run(lg.run(url + "/v1/completions", manifests.MODEL, specs, 1))
        text = httpx.get(url + "/metrics").text
    assert all(r.ok for r in records), [r.error for r in records]
    hits = float(re.search(r"^vllm:prefix_cache_hits_total{.*} (\S+)$", text, re.M).group(1))
    assert sum(r.cached_tokens for r in records) == hits
    server_ttft = histogram_mean(text, "vllm:time_to_first_token_seconds")
    client_ttft = statistics.mean(r.ttft for r in records)
    assert client_ttft >= server_ttft - 0.005, "the client cannot see a token before it exists"
    assert client_ttft - server_ttft < 0.05, (
        f"client {client_ttft:.3f}s vs server {server_ttft:.3f}s"
    )
