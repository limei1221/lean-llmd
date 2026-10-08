"""Day 2 - reading a model server's state from its /metrics endpoint.

Shaped like llm-d-router's core metrics extractor: a ``Spec`` names a series,
``get_latest_metric`` finds it, ``extract`` folds the series a scorer needs
into a ``Metrics`` snapshot without touching the one readers already hold.
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
        return replace(self)


@dataclass(frozen=True)
class Sample:
    """One line of the text format: ``name{labels} value [timestamp_ms]``."""

    name: str
    labels: dict[str, str] = field(default_factory=dict)
    value: float = 0.0
    timestamp_ms: int | None = None


_NAME = r"[a-zA-Z_:][a-zA-Z0-9_:]*"
_LABEL = r"[a-zA-Z_][a-zA-Z0-9_]*"
_ESCAPES = {"\\": "\\", '"': '"', "n": "\n"}


def _parse_labels(text: str, pos: int) -> tuple[dict[str, str], int]:
    """Parse ``{a="x",b="y"}`` starting at the ``{``; return labels and the index after ``}``."""
    labels: dict[str, str] = {}
    pos += 1
    while True:
        while pos < len(text) and text[pos] in " ,":
            pos += 1
        if text[pos] == "}":
            return labels, pos + 1
        match = re.compile(_LABEL).match(text, pos)
        if match is None:
            raise ValueError(f"bad label name at {pos}: {text!r}")
        name = match.group()
        pos = match.end()
        while text[pos] == " ":
            pos += 1
        if text[pos] != "=":
            raise ValueError(f"expected = after {name!r}: {text!r}")
        pos += 1
        while text[pos] == " ":
            pos += 1
        if text[pos] != '"':
            raise ValueError(f"expected a quoted value for {name!r}: {text!r}")
        pos += 1
        value: list[str] = []
        while text[pos] != '"':
            if text[pos] == "\\":
                value.append(_ESCAPES.get(text[pos + 1], "\\" + text[pos + 1]))
                pos += 2
            else:
                value.append(text[pos])
                pos += 1
        labels[name] = "".join(value)
        pos += 1


def _parse_value(token: str) -> float:
    lowered = token.lower()
    if lowered in ("+inf", "inf"):
        return math.inf
    if lowered == "-inf":
        return -math.inf
    if lowered == "nan":
        return math.nan
    return float(token)


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
    families: Families = {}
    name_re = re.compile(_NAME)
    for raw in text.splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        match = name_re.match(line)
        if match is None:
            raise ValueError(f"bad metric name: {raw!r}")
        name = match.group()
        pos = match.end()
        labels: dict[str, str] = {}
        if pos < len(line) and line[pos] == "{":
            labels, pos = _parse_labels(line, pos)
        rest = line[pos:].split()
        if not rest:
            raise ValueError(f"no value: {raw!r}")
        timestamp = int(rest[1]) if len(rest) > 1 else None
        sample = Sample(name, labels, _parse_value(rest[0]), timestamp)
        families.setdefault(name, []).append(sample)
    return families


@dataclass(frozen=True)
class Spec:
    """Which series to read: a metric name and labels it must carry."""

    name: str
    labels: dict[str, str] = field(default_factory=dict)


_SPEC_RE = re.compile(r"^\s*(\S+?)\s*(?:\{([^}]*)\})?\s*$")
_LABEL_PAIR_RE = re.compile(r'^\s*(\S+?)\s*=\s*"([^"]*)"\s*$')
_ADD_QUOTES_RE = re.compile(r'(\w+)\s*=\s*([^",}\s]+)')


def parse_spec(spec: str) -> Spec | None:
    """Parse ``name`` or ``name{label=value, other="value"}`` into a ``Spec``.

    An empty string means "not configured" and returns ``None``. Label values
    may be quoted or bare. Raise ``ValueError`` for an invalid metric or label
    name.
    """
    if spec == "":
        return None
    quoted = _ADD_QUOTES_RE.sub(r'\1="\2"', spec)
    match = _SPEC_RE.match(quoted)
    if match is None or not re.fullmatch(_NAME, match.group(1)):
        raise ValueError(f"not a valid metric specification: {spec!r}")
    labels: dict[str, str] = {}
    block = (match.group(2) or "").strip()
    if block:
        for pair in block.split(","):
            label = _LABEL_PAIR_RE.match(pair)
            if label is None or not re.fullmatch(_LABEL, label.group(1)):
                raise ValueError(f"invalid label pair {pair!r} in specification: {spec!r}")
            labels[label.group(1)] = label.group(2)
    return Spec(match.group(1), labels)


def get_latest_metric(spec: Spec, families: Families) -> Sample:
    """The matching sample with the newest timestamp.

    A sample matches when it carries every label in ``spec.labels`` with the
    same value; extra labels are fine. A missing timestamp counts as 0, and
    among equal timestamps the first sample wins. Raise ``LookupError`` if
    the family is missing, empty, or nothing matches.
    """
    samples = families.get(spec.name)
    if not samples:
        raise LookupError(f"metric family {spec.name!r} not found")
    latest: Sample | None = None
    recent = -1
    for sample in samples:
        if all(sample.labels.get(k) == v for k, v in spec.labels.items()):
            ts = sample.timestamp_ms or 0
            if ts > recent:
                recent = ts
                latest = sample
    if latest is None:
        raise LookupError(f"no matching metric found for {spec.name!r} with labels {spec.labels}")
    return latest


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
    clone = current.clone()
    errors: list[str] = []
    updated = False

    def read(spec: Spec | None) -> Sample | None:
        if spec is None:
            return None
        try:
            return get_latest_metric(spec, families)
        except LookupError as e:
            errors.append(str(e))
            return None

    if (sample := read(mapping.total_queued_requests)) is not None:
        clone.waiting_queue_size = int(sample.value)
        updated = True
    if (sample := read(mapping.total_running_requests)) is not None:
        clone.running_requests_size = int(sample.value)
        updated = True
    if (sample := read(mapping.kv_cache_utilization)) is not None:
        clone.kv_cache_usage_percent = sample.value
        updated = True
    if (sample := read(mapping.cache_info)) is not None:
        try:
            clone.cache_block_size = int(sample.labels.get("block_size", "0"))
            clone.cache_num_blocks = int(sample.labels.get("num_gpu_blocks", "0"))
        except ValueError as e:
            errors.append(str(e))
        updated = True
    if updated:
        clone.update_time = now
    return clone, errors


def scrape(url: str, client: httpx.Client | None = None, timeout: float = 1.0) -> Families:
    """GET ``url`` (a model server's ``/metrics``) and parse it. Raise on a non-2xx status."""
    if client is None:
        response = httpx.get(url, timeout=timeout)
    else:
        response = client.get(url, timeout=timeout)
    response.raise_for_status()
    return parse_text(response.text)
