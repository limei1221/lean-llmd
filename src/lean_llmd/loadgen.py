"""Day 7 - a streaming load generator.

Your implementation. Every function below raises until you write it.

    uv run lean-llmd test --day 7         # your code
    uv run lean-llmd test --day 7 --ref   # proves the tests are passable
    uv run lean-llmd diff --day 7         # only after you have a passing run
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
    raise NotImplementedError("day 7: implement shared_prefix_workload()")


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
    raise NotImplementedError("day 7: implement send_one()")


async def run(
    url: str, model: str, specs: list[RequestSpec], concurrency: int
) -> list[RequestRecord]:
    """Send every spec with at most ``concurrency`` in flight; records in spec order."""
    raise NotImplementedError("day 7: implement run()")


def percentile(values: list[float], p: float) -> float:
    """The ``p``-th percentile (0-100) with linear interpolation between ranks.

    Sort; the rank is ``(len - 1) * p / 100``; interpolate between the two
    neighbours (numpy's default). Empty input is NaN.
    """
    raise NotImplementedError("day 7: implement percentile()")


def summarize(records: list[RequestRecord], wall_seconds: float) -> Summary:
    """Aggregate the successful records.

    - TPOT per request is ``(e2e - ttft) / (output_tokens - 1)``, only for
      requests with more than one output token; report the mean.
    - Throughput is successful requests per wall-clock second.
    - Cache hit rate is total cached prompt tokens over total prompt tokens.
    """
    raise NotImplementedError("day 7: implement summarize()")


def main(argv: list[str] | None = None) -> int:
    raise NotImplementedError("day 7: implement main()")


if __name__ == "__main__":
    sys.exit(main())
