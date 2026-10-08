"""Day 7 - a streaming load generator: TTFT, time per output token, cache hits.

Run inside the cluster as a Job:
``python -m lean_llmd_ref.loadgen --url http://epp/v1/completions --model mini/sim``.
It prints one JSON summary line, which ``lean-llmd bench`` collects.
"""

import argparse
import asyncio
import json
import math
import random
import sys
import time
from dataclasses import asdict, dataclass

import httpx

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
    "mike",
    "november",
    "oscar",
    "papa",
    "quebec",
    "romeo",
    "sierra",
    "tango",
    "uniform",
    "victor",
    "whiskey",
    "xray",
    "yankee",
    "zulu",
]


@dataclass(frozen=True)
class RequestSpec:
    prompt: str
    max_tokens: int
    group: int


@dataclass
class RequestRecord:
    group: int
    start: float
    ok: bool
    ttft: float = 0.0
    e2e: float = 0.0
    output_tokens: int = 0
    prompt_tokens: int = 0
    cached_tokens: int = 0
    error: str = ""


@dataclass(frozen=True)
class Summary:
    requests: int
    errors: int
    ttft_p50: float
    ttft_p99: float
    tpot_mean: float
    e2e_p50: float
    throughput_rps: float
    cache_hit_rate: float


def _words(rng: random.Random, n: int) -> str:
    return " ".join(rng.choice(WORDS) for _ in range(n))


def shared_prefix_workload(
    groups: int,
    requests_per_group: int,
    prefix_words: int,
    suffix_words: int,
    max_tokens: int,
    seed: int = 0,
) -> list[RequestSpec]:
    """``groups * requests_per_group`` requests, shuffled with ``seed``.

    Every request in group ``g`` starts with that group's own random prefix
    of ``prefix_words`` words (think: one system prompt per tenant), followed
    by a space and its own random suffix of ``suffix_words`` words. Words come
    from ``WORDS``. Same seed, same workload.
    """
    rng = random.Random(seed)
    prefixes = [_words(rng, prefix_words) for _ in range(groups)]
    specs = [
        RequestSpec(f"{prefixes[g]} {_words(rng, suffix_words)}", max_tokens, g)
        for g in range(groups)
        for _ in range(requests_per_group)
    ]
    rng.shuffle(specs)
    return specs


async def send_one(
    client: httpx.AsyncClient, url: str, model: str, spec: RequestSpec
) -> RequestRecord:
    """POST one streaming completion and time it.

    Ask for ``stream: true`` with ``stream_options.include_usage``, and
    ``ignore_eos: true`` so every request generates exactly ``max_tokens``
    (runs then differ only in routing, not in output lengths). Read the
    server-sent events (``data: {...}`` lines, ending with ``data: [DONE]``):

    - TTFT is from just before sending to the first chunk whose choice has
      non-empty ``text``.
    - The usage chunk gives ``completion_tokens``, ``prompt_tokens`` and
      ``prompt_tokens_details.cached_tokens``.
    - Any exception or non-200 status gives ``ok=False`` with the error.
    """
    body = {
        "model": model,
        "prompt": spec.prompt,
        "max_tokens": spec.max_tokens,
        "stream": True,
        "stream_options": {"include_usage": True},
        "ignore_eos": True,
    }
    start = time.perf_counter()
    record = RequestRecord(group=spec.group, start=start, ok=False)
    try:
        async with client.stream("POST", url, json=body) as response:
            if response.status_code != 200:
                record.error = f"HTTP {response.status_code}: {(await response.aread())[:200]!r}"
                return record
            async for line in response.aiter_lines():
                if not line.startswith("data: "):
                    continue
                data = line[len("data: ") :]
                if data == "[DONE]":
                    break
                chunk = json.loads(data)
                choices = chunk.get("choices") or []
                if record.ttft == 0.0 and any(c.get("text") for c in choices):
                    record.ttft = time.perf_counter() - start
                usage = chunk.get("usage")
                if usage:
                    record.output_tokens = usage.get("completion_tokens", 0)
                    record.prompt_tokens = usage.get("prompt_tokens", 0)
                    details = usage.get("prompt_tokens_details") or {}
                    record.cached_tokens = details.get("cached_tokens") or 0
        record.e2e = time.perf_counter() - start
        record.ok = True
    except Exception as e:
        record.error = f"{type(e).__name__}: {e}"
    return record


async def run(
    url: str, model: str, specs: list[RequestSpec], concurrency: int
) -> list[RequestRecord]:
    """Send every spec with at most ``concurrency`` in flight; records in spec order."""
    semaphore = asyncio.Semaphore(concurrency)
    limits = httpx.Limits(max_connections=concurrency, max_keepalive_connections=concurrency)
    async with httpx.AsyncClient(timeout=120.0, limits=limits) as client:

        async def bounded(spec: RequestSpec) -> RequestRecord:
            async with semaphore:
                return await send_one(client, url, model, spec)

        return list(await asyncio.gather(*(bounded(s) for s in specs)))


def percentile(values: list[float], p: float) -> float:
    """The ``p``-th percentile (0-100) with linear interpolation between ranks.

    Sort; the rank is ``(len - 1) * p / 100``; interpolate between the two
    neighbours (numpy's default). Empty input is NaN.
    """
    if not values:
        return math.nan
    ordered = sorted(values)
    rank = (len(ordered) - 1) * p / 100
    low = math.floor(rank)
    high = math.ceil(rank)
    return ordered[low] + (ordered[high] - ordered[low]) * (rank - low)


def summarize(records: list[RequestRecord], wall_seconds: float) -> Summary:
    """Aggregate the successful records.

    - TPOT per request is ``(e2e - ttft) / (output_tokens - 1)``, only for
      requests with more than one output token; report the mean.
    - Throughput is successful requests per wall-clock second.
    - Cache hit rate is total cached prompt tokens over total prompt tokens.
    """
    ok = [r for r in records if r.ok]
    tpots = [(r.e2e - r.ttft) / (r.output_tokens - 1) for r in ok if r.output_tokens > 1]
    prompt_tokens = sum(r.prompt_tokens for r in ok)
    return Summary(
        requests=len(records),
        errors=len(records) - len(ok),
        ttft_p50=percentile([r.ttft for r in ok], 50),
        ttft_p99=percentile([r.ttft for r in ok], 99),
        tpot_mean=sum(tpots) / len(tpots) if tpots else math.nan,
        e2e_p50=percentile([r.e2e for r in ok], 50),
        throughput_rps=len(ok) / wall_seconds if wall_seconds > 0 else math.nan,
        cache_hit_rate=sum(r.cached_tokens for r in ok) / prompt_tokens if prompt_tokens else 0.0,
    )


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="loadgen", description=__doc__)
    p.add_argument("--url", required=True)
    p.add_argument("--model", default="mini/sim")
    p.add_argument("--groups", type=int, default=8)
    p.add_argument("--requests-per-group", type=int, default=25)
    p.add_argument("--prefix-words", type=int, default=600)
    p.add_argument("--suffix-words", type=int, default=40)
    p.add_argument("--max-tokens", type=int, default=20)
    p.add_argument("--concurrency", type=int, default=8)
    p.add_argument("--seed", type=int, default=0)
    a = p.parse_args(argv)
    specs = shared_prefix_workload(
        a.groups, a.requests_per_group, a.prefix_words, a.suffix_words, a.max_tokens, a.seed
    )
    started = time.perf_counter()
    records = asyncio.run(run(a.url, a.model, specs, a.concurrency))
    summary = summarize(records, time.perf_counter() - started)
    errors = [r.error for r in records if not r.ok][:5]
    print(json.dumps({"summary": asdict(summary), "sample_errors": errors}), flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
