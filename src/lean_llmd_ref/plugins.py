"""Day 4 - the plugins: two load scorers, a label filter, two pickers, one profile handler.

Each mirrors the llm-d-router plugin of the same ``name``. A scorer returns a
score in [0, 1] per endpoint, where higher is better.
"""

import itertools
import random
import threading

from lean_llmd_ref.datastore import Endpoint, labels_match
from lean_llmd_ref.scheduling import (
    InferenceRequest,
    ProfileRunResult,
    SchedulerProfile,
    SchedulingResult,
    ScoredEndpoint,
)


class QueueScorer:
    """Prefer short waiting queues. Upstream: ``queue-scorer``."""

    name = "queue-scorer"

    def score(self, request: InferenceRequest, endpoints: list[Endpoint]) -> dict[Endpoint, float]:
        """Min-max normalise ``waiting_queue_size``: the shortest scores 1, the longest 0.

        If every queue is the same length, every endpoint scores 1.
        """
        sizes = [e.metrics.waiting_queue_size for e in endpoints]
        if not sizes:
            return {}
        low, high = min(sizes), max(sizes)
        if low == high:
            return dict.fromkeys(endpoints, 1.0)
        return {e: (high - e.metrics.waiting_queue_size) / (high - low) for e in endpoints}


class KVCacheUtilizationScorer:
    """Prefer free KV cache. Upstream: ``kv-cache-utilization-scorer``."""

    name = "kv-cache-utilization-scorer"

    def score(self, request: InferenceRequest, endpoints: list[Endpoint]) -> dict[Endpoint, float]:
        """``1 - kv_cache_usage_percent`` (usage is a fraction, 0..1)."""
        return {e: 1.0 - e.metrics.kv_cache_usage_percent for e in endpoints}


class LabelSelectorFilter:
    """Keep endpoints whose Pod labels match ``selector``. Upstream: ``label-selector-filter``."""

    name = "label-selector-filter"

    def __init__(self, selector: dict[str, str]) -> None:
        self.selector = dict(selector)

    def filter(self, request: InferenceRequest, endpoints: list[Endpoint]) -> list[Endpoint]:
        return [e for e in endpoints if labels_match(self.selector, e.metadata.labels)]


class MaxScorePicker:
    """Pick the highest score. Upstream: ``max-score-picker``, the default picker."""

    name = "max-score-picker"

    def __init__(self, max_num_of_endpoints: int = 1) -> None:
        self.max_num_of_endpoints = max_num_of_endpoints
        self._counter = itertools.count()
        self._lock = threading.Lock()

    def pick(self, scored: list[ScoredEndpoint]) -> ProfileRunResult:
        """The ``max_num_of_endpoints`` best endpoints, best first.

        Sort by score, highest first, keeping the input order among equals.
        Then rotate every run of equal scores left by ``n % len(run)``, where
        ``n`` counts calls to ``pick`` (0 on the first call). Without the
        rotation, equal endpoints would always resolve to the same one.
        """
        with self._lock:
            n = next(self._counter)
        ordered = sorted(scored, key=lambda s: -s.score)
        start = 0
        while start < len(ordered) and start < self.max_num_of_endpoints:
            end = start + 1
            while end < len(ordered) and ordered[end].score == ordered[start].score:
                end += 1
            run = ordered[start:end]
            shift = n % len(run)
            ordered[start:end] = run[shift:] + run[:shift]
            start = end
        return ProfileRunResult([s.endpoint for s in ordered[: self.max_num_of_endpoints]])


class RandomPicker:
    """Ignore scores and pick at random. Upstream: ``random-picker``."""

    name = "random-picker"

    def __init__(self, max_num_of_endpoints: int = 1, seed: int | None = None) -> None:
        self.max_num_of_endpoints = max_num_of_endpoints
        self._rng = random.Random(seed)
        self._lock = threading.Lock()

    def pick(self, scored: list[ScoredEndpoint]) -> ProfileRunResult:
        """Shuffle, then take the first ``max_num_of_endpoints``."""
        shuffled = list(scored)
        with self._lock:
            self._rng.shuffle(shuffled)
        return ProfileRunResult([s.endpoint for s in shuffled[: self.max_num_of_endpoints]])


class SingleProfileHandler:
    """Run each profile once; the only profile is primary. Upstream: ``single-profile-handler``."""

    name = "single-profile-handler"

    def pick(
        self,
        request: InferenceRequest,
        profiles: dict[str, SchedulerProfile],
        results: dict[str, ProfileRunResult | None],
    ) -> dict[str, SchedulerProfile]:
        """All profiles on the first call; none once every profile has a result."""
        if len(results) == len(profiles):
            return {}
        return profiles

    def process_results(
        self, request: InferenceRequest, results: dict[str, ProfileRunResult | None]
    ) -> SchedulingResult:
        """Wrap the single result. ``ValueError`` if there is not exactly one, or it failed."""
        if len(results) != 1:
            raise ValueError("single profile handler expects exactly one profile")
        ((name, result),) = results.items()
        if result is None:
            raise ValueError(f"failed to run scheduler profile {name!r}")
        return SchedulingResult(dict(results), name)
