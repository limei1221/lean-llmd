"""Day 2 - reading a model server's state from /metrics.

Your implementation. Every function below raises until you write it.

    uv run lean-llmd test --day 2         # your code
    uv run lean-llmd test --day 2 --ref   # proves the tests are passable
    uv run lean-llmd diff --day 2         # only after you have a passing run
"""

import math
import re
from dataclasses import dataclass, field, replace

import httpx

Families = dict[str, list["Sample"]]


@dataclass
class Metrics:
    """What the router knows about one model server. Upstream: ``datalayer.Metrics``."""

    waiting_queue_size: int = 0
    running_requests_size: int = 0
    kv_cache_usage_percent: float = 0.0
    cache_block_size: int = 0
    cache_num_blocks: int = 0
    update_time: float = 0.0

    def clone(self) -> "Metrics":
        raise NotImplementedError("day 2: implement clone()")


@dataclass(frozen=True)
class Sample:
    """One line of the text format: ``name{labels} value [timestamp_ms]``."""

    name: str
    labels: dict[str, str] = field(default_factory=dict)
    value: float = 0.0
    timestamp_ms: int | None = None


def parse_text(text: str) -> Families:
    """Parse the Prometheus text exposition format into samples grouped by name.

    - Skip blank lines and comments (``# HELP``, ``# TYPE``, anything ``#``).
    - Each sample line is ``name``, an optional ``{label="value",...}`` block,
      a value, and an optional integer timestamp in milliseconds.
    - Label values may contain ``,``, ``}``, spaces and the escapes ``\\\\``,
      ``\\"`` and ``\\n``.
    - Values may be ``NaN``, ``+Inf`` or ``-Inf``.

    The key is the sample's own name, so a histogram's ``x_bucket``,
    ``x_sum`` and ``x_count`` are three keys.
    """
    raise NotImplementedError("day 2: implement parse_text()")


@dataclass(frozen=True)
class Spec:
    """Which series to read: a metric name and labels it must carry."""

    name: str
    labels: dict[str, str] = field(default_factory=dict)


def parse_spec(spec: str) -> Spec | None:
    """Parse ``name`` or ``name{label=value, other="value"}`` into a ``Spec``.

    An empty string means "not configured" and returns ``None``. Label values
    may be quoted or bare. Raise ``ValueError`` for an invalid metric or label
    name.
    """
    raise NotImplementedError("day 2: implement parse_spec()")


def get_latest_metric(spec: Spec, families: Families) -> Sample:
    """The matching sample with the newest timestamp.

    A sample matches when it carries every label in ``spec.labels`` with the
    same value; extra labels are fine. A missing timestamp counts as 0, and
    among equal timestamps the first sample wins. Raise ``LookupError`` if
    the family is missing, empty, or nothing matches.
    """
    raise NotImplementedError("day 2: implement get_latest_metric()")


@dataclass(frozen=True)
class MetricMapping:
    """Which series hold the numbers ``Metrics`` needs, for one engine."""

    total_queued_requests: Spec | None
    total_running_requests: Spec | None
    kv_cache_utilization: Spec | None
    cache_info: Spec | None


VLLM_MAPPING = MetricMapping(
    total_queued_requests=Spec("vllm:num_requests_waiting"),
    total_running_requests=Spec("vllm:num_requests_running"),
    kv_cache_utilization=Spec("vllm:kv_cache_usage_perc"),
    cache_info=Spec("vllm:cache_config_info"),
)


def extract(
    families: Families, mapping: MetricMapping, current: Metrics, now: float
) -> tuple[Metrics, list[str]]:
    """Fold ``families`` into a copy of ``current``; return it and any errors.

    - Never mutate ``current``: readers may be holding it.
    - Each configured spec is read independently. A missing series adds an
      error string and leaves that field as it was; the others still update.
    - Queue and running counts are ints; KV-cache usage is a fraction 0..1.
    - ``cache_info`` is an info metric: its value is 1 and the numbers are in
      its ``block_size`` and ``num_gpu_blocks`` labels.
    - Set ``update_time = now`` only if at least one field was read.
    """
    raise NotImplementedError("day 2: implement extract()")


def scrape(url: str, client: httpx.Client | None = None, timeout: float = 1.0) -> Families:
    """GET ``url`` (a model server's ``/metrics``) and parse it. Raise on a non-2xx status."""
    raise NotImplementedError("day 2: implement scrape()")
