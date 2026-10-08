"""Day 4 - the scheduling framework: profiles of filters, weighted scorers and a picker.

Shaped like llm-d-router's ``pkg/epp/scheduling`` and its plugin interfaces.
Plugins live in ``plugins.py``; this file only runs them.
"""

from dataclasses import dataclass, field
from typing import Any, Protocol

from lean_llmd_ref.datastore import Endpoint


@dataclass
class InferenceRequest:
    """The parsed request every plugin sees. ``body`` is the OpenAI JSON.

    ``set_headers`` are headers plugins want added to the request on its way
    to the model server (day 9 puts the chosen prefill endpoint there).
    """

    request_id: str
    target_model: str
    body: dict[str, Any]
    headers: dict[str, str] = field(default_factory=dict)
    set_headers: dict[str, str] = field(default_factory=dict)


@dataclass
class ScoredEndpoint:
    endpoint: Endpoint
    score: float


@dataclass
class ProfileRunResult:
    target_endpoints: list[Endpoint]


@dataclass
class SchedulingResult:
    profile_results: dict[str, ProfileRunResult | None]
    primary_profile_name: str

    def primary(self) -> ProfileRunResult:
        result = self.profile_results[self.primary_profile_name]
        assert result is not None
        return result


class Filter(Protocol):
    name: str

    def filter(self, request: InferenceRequest, endpoints: list[Endpoint]) -> list[Endpoint]: ...


class Scorer(Protocol):
    name: str

    def score(
        self, request: InferenceRequest, endpoints: list[Endpoint]
    ) -> dict[Endpoint, float]: ...


class Picker(Protocol):
    name: str

    def pick(self, scored: list[ScoredEndpoint]) -> ProfileRunResult: ...


class ProfileHandler(Protocol):
    name: str

    def pick(
        self,
        request: InferenceRequest,
        profiles: dict[str, "SchedulerProfile"],
        results: dict[str, ProfileRunResult | None],
    ) -> dict[str, "SchedulerProfile"]: ...

    def process_results(
        self, request: InferenceRequest, results: dict[str, ProfileRunResult | None]
    ) -> SchedulingResult: ...


@dataclass
class WeightedScorer:
    scorer: Scorer
    weight: float = 1.0


class NoEndpointsError(Exception):
    """Every endpoint was filtered out. The EPP answers 503."""


class SchedulingError(Exception):
    """No profile produced a result."""


def enforce_score_range(score: float) -> float:
    """Clamp ``score`` into [0, 1]. A buggy scorer must not outvote the others."""
    return min(max(score, 0.0), 1.0)


class SchedulerProfile:
    """One pipeline: filters, then weighted scorers, then a picker."""

    def __init__(
        self,
        filters: list[Filter] | None = None,
        scorers: list[WeightedScorer] | None = None,
        picker: Picker | None = None,
    ) -> None:
        if picker is None:
            raise ValueError("a profile needs a picker")
        self.filters = filters or []
        self.scorers = scorers or []
        self.picker = picker

    def run(self, request: InferenceRequest, candidates: list[Endpoint]) -> ProfileRunResult:
        """Filter, score, pick.

        - Filters run in order, each on the previous one's output; stop as
          soon as one returns nothing, and raise ``NoEndpointsError``.
        - An endpoint's score is the sum over scorers of
          ``enforce_score_range(score) * weight``; one no scorer mentions gets 0.
        - The picker gets ``ScoredEndpoint``s in the filtered order.
        """
        endpoints = self._run_filter_plugins(request, candidates)
        if not endpoints:
            raise NoEndpointsError("no endpoints available for the given request")
        scores = self._run_scorer_plugins(request, endpoints)
        scored = [ScoredEndpoint(e, scores[e]) for e in endpoints]
        return self.picker.pick(scored)

    def _run_filter_plugins(
        self, request: InferenceRequest, endpoints: list[Endpoint]
    ) -> list[Endpoint]:
        """Each filter sees the previous one's output; stop as soon as one returns nothing."""
        for f in self.filters:
            endpoints = f.filter(request, endpoints)
            if not endpoints:
                break
        return endpoints

    def _run_scorer_plugins(
        self, request: InferenceRequest, endpoints: list[Endpoint]
    ) -> dict[Endpoint, float]:
        """Sum of ``clamp(score) * weight`` per endpoint; an endpoint no scorer mentions gets 0."""
        total = dict.fromkeys(endpoints, 0.0)
        for weighted in self.scorers:
            for endpoint, score in weighted.scorer.score(request, endpoints).items():
                total[endpoint] += enforce_score_range(score) * weighted.weight
        return total


class Scheduler:
    """Runs the profiles a ``ProfileHandler`` picks until it picks none."""

    def __init__(self, profile_handler: ProfileHandler, profiles: dict[str, SchedulerProfile]):
        self.profile_handler = profile_handler
        self.profiles = profiles

    def schedule(self, request: InferenceRequest, candidates: list[Endpoint]) -> SchedulingResult:
        """The scheduling cycle.

        Loop: ask the handler which profiles to run given the results so far;
        stop when it returns none. A profile that raises gets a ``None``
        result (remember the error). If nothing ran at all, raise
        ``SchedulingError``. Otherwise return ``process_results``; if that
        raises and a profile had failed, raise the profile's error instead,
        since it is the real cause.
        """
        results: dict[str, ProfileRunResult | None] = {}
        errors: dict[str, Exception] = {}
        while True:
            picked = self.profile_handler.pick(request, self.profiles, results)
            if not picked:
                break
            for name, profile in picked.items():
                try:
                    results[name] = profile.run(request, candidates)
                    errors.pop(name, None)
                except Exception as e:
                    results[name] = None
                    errors[name] = e
        if not results:
            raise SchedulingError(f"failed to run any scheduler profile for {request.request_id}")
        try:
            return self.profile_handler.process_results(request, results)
        except Exception:
            if errors:
                raise next(iter(errors.values())) from None
            raise
