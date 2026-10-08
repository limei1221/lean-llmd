"""Day 4 - the scheduling framework: filter, score, pick.

Your implementation. Every function below raises until you write it.

    uv run lean-llmd test --day 4         # your code
    uv run lean-llmd test --day 4 --ref   # proves the tests are passable
    uv run lean-llmd diff --day 4         # only after you have a passing run
"""

from dataclasses import dataclass, field
from typing import Any, Protocol

from lean_llmd.datastore import Endpoint


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
        raise NotImplementedError("day 4: implement primary()")


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
    raise NotImplementedError("day 4: implement enforce_score_range()")


class SchedulerProfile:
    """One pipeline: filters, then weighted scorers, then a picker."""

    def __init__(
        self,
        filters: list[Filter] | None = None,
        scorers: list[WeightedScorer] | None = None,
        picker: Picker | None = None,
    ) -> None:
        raise NotImplementedError("day 4: implement __init__()")

    def run(self, request: InferenceRequest, candidates: list[Endpoint]) -> ProfileRunResult:
        """Filter, score, pick.

        - Filters run in order, each on the previous one's output; stop as
          soon as one returns nothing, and raise ``NoEndpointsError``.
        - An endpoint's score is the sum over scorers of
          ``enforce_score_range(score) * weight``; one no scorer mentions gets 0.
        - The picker gets ``ScoredEndpoint``s in the filtered order.
        """
        raise NotImplementedError("day 4: implement run()")


class Scheduler:
    """Runs the profiles a ``ProfileHandler`` picks until it picks none."""

    def __init__(self, profile_handler: ProfileHandler, profiles: dict[str, SchedulerProfile]):
        raise NotImplementedError("day 4: implement __init__()")

    def schedule(self, request: InferenceRequest, candidates: list[Endpoint]) -> SchedulingResult:
        """The scheduling cycle.

        Loop: ask the handler which profiles to run given the results so far;
        stop when it returns none. A profile that raises gets a ``None``
        result (remember the error). If nothing ran at all, raise
        ``SchedulingError``. Otherwise return ``process_results``; if that
        raises and a profile had failed, raise the profile's error instead,
        since it is the real cause.
        """
        raise NotImplementedError("day 4: implement schedule()")
