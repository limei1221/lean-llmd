"""Day 4 - the scheduler: filter, score, pick.

Oracles: values worked out by hand (including the example in llm-d's own
scheduling doc), and properties: ties share load, filters short-circuit, a
profile handler's loop ends.
"""

import collections

import pytest


@pytest.fixture
def s(mod):
    return mod("scheduling")


@pytest.fixture
def p(mod):
    return mod("plugins")


@pytest.fixture
def make(mod):
    ds, metrics = mod("datastore"), mod("metrics")

    def endpoint(name, queue=0, kv=0.0, labels=None):
        meta = ds.EndpointMetadata(
            id=ds.NamespacedName("ns", f"{name}-rank-0"),
            pod_name=name,
            address=f"10.0.0.{len(name)}",
            port=8000,
            labels=labels or {},
        )
        return ds.Endpoint(
            meta, metrics.Metrics(waiting_queue_size=queue, kv_cache_usage_percent=kv)
        )

    return endpoint


@pytest.fixture
def request_(s):
    return s.InferenceRequest("r1", "mini/sim", {"model": "mini/sim", "prompt": "hi"})


class Fixed:
    """A scorer that returns preset scores."""

    def __init__(self, name, scores):
        self.name = name
        self.scores = scores

    def score(self, request, endpoints):
        return {
            e: self.scores[e.metadata.pod_name]
            for e in endpoints
            if e.metadata.pod_name in self.scores
        }


class Recording:
    """A filter that records what it saw and keeps a chosen subset."""

    def __init__(self, keep):
        self.name = "recording"
        self.keep = keep
        self.seen = None

    def filter(self, request, endpoints):
        self.seen = [e.metadata.pod_name for e in endpoints]
        return [e for e in endpoints if e.metadata.pod_name in self.keep]


def test_queue_scorer_normalises_min_to_max(p, make, request_):
    a, b, c = make("a", queue=0), make("b", queue=5), make("c", queue=10)
    scores = p.QueueScorer().score(request_, [a, b, c])
    assert scores == {a: 1.0, b: 0.5, c: 0.0}


def test_equal_queues_score_one(p, make, request_):
    a, b = make("a", queue=3), make("b", queue=3)
    assert p.QueueScorer().score(request_, [a, b]) == {a: 1.0, b: 1.0}
    assert p.QueueScorer().score(request_, []) == {}


def test_kv_cache_scorer_prefers_free_cache(p, make, request_):
    a, b = make("a", kv=0.25), make("b", kv=1.0)
    assert p.KVCacheUtilizationScorer().score(request_, [a, b]) == {a: 0.75, b: 0.0}


def test_enforce_score_range(s):
    assert [s.enforce_score_range(x) for x in (-0.5, 0.0, 0.3, 1.0, 7.0)] == [0, 0, 0.3, 1, 1]


def test_weights_multiply_and_sum(s, p, make, request_):
    """llm-d's scheduling doc: A (weight 2) gives 0.8, B (weight 1) gives 0.5, total 2.1."""
    x, y = make("x"), make("y")
    profile = s.SchedulerProfile(
        scorers=[
            s.WeightedScorer(Fixed("A", {"x": 0.8, "y": 0.1}), 2.0),
            s.WeightedScorer(Fixed("B", {"x": 0.5, "y": 0.9}), 1.0),
        ],
        picker=p.MaxScorePicker(),
    )
    seen = {}

    class Spy:
        name = "spy"

        def pick(self, scored):
            seen.update({e.endpoint.metadata.pod_name: e.score for e in scored})
            return s.ProfileRunResult([scored[0].endpoint])

    profile.picker = Spy()
    profile.run(request_, [x, y])
    assert seen["x"] == pytest.approx(2.1)
    assert seen["y"] == pytest.approx(1.1)


def test_out_of_range_scores_are_clamped(s, p, make, request_):
    x, y = make("x"), make("y")
    profile = s.SchedulerProfile(
        scorers=[
            s.WeightedScorer(Fixed("broken", {"x": 9.0, "y": -3.0}), 1.0),
            s.WeightedScorer(Fixed("fine", {"x": 0.0, "y": 1.0}), 1.5),
        ],
        picker=p.MaxScorePicker(),
    )
    # Unclamped, x would win 9 to -1.5. Clamped, it is 1.0 against 1.5.
    assert profile.run(request_, [x, y]).target_endpoints == [y]


def test_an_endpoint_no_scorer_mentions_scores_zero(s, p, make, request_):
    x, y = make("x"), make("y")
    profile = s.SchedulerProfile(
        scorers=[s.WeightedScorer(Fixed("only-y", {"y": 0.1}), 1.0)], picker=p.MaxScorePicker()
    )
    assert profile.run(request_, [x, y]).target_endpoints == [y]


def test_filters_chain_and_short_circuit(s, p, make, request_):
    a, b, c = make("a"), make("b"), make("c")
    first, second = Recording(keep={"a", "b"}), Recording(keep={"b"})
    profile = s.SchedulerProfile(filters=[first, second], picker=p.MaxScorePicker())
    assert profile.run(request_, [a, b, c]).target_endpoints == [b]
    assert second.seen == ["a", "b"]

    empty, never = Recording(keep=set()), Recording(keep={"a"})
    profile = s.SchedulerProfile(filters=[empty, never], picker=p.MaxScorePicker())
    with pytest.raises(s.NoEndpointsError):
        profile.run(request_, [a, b, c])
    assert never.seen is None, "a filter ran after the candidates were already gone"


def test_label_selector_filter(p, make, request_):
    a = make("a", labels={"llm-d.ai/role": "decode"})
    b = make("b", labels={"llm-d.ai/role": "prefill"})
    kept = p.LabelSelectorFilter({"llm-d.ai/role": "decode"}).filter(request_, [a, b])
    assert kept == [a]


def test_max_score_picker_takes_the_best(s, p, make):
    a, b, c = make("a"), make("b"), make("c")
    scored = [s.ScoredEndpoint(a, 0.2), s.ScoredEndpoint(b, 0.9), s.ScoredEndpoint(c, 0.5)]
    assert p.MaxScorePicker().pick(scored).target_endpoints == [b]
    assert p.MaxScorePicker(max_num_of_endpoints=2).pick(list(scored)).target_endpoints == [b, c]


def test_ties_rotate_round_robin(s, p, make):
    a, b, c = make("a"), make("b"), make("c")
    picker = p.MaxScorePicker()
    picks = [
        picker.pick([s.ScoredEndpoint(e, 1.0) for e in (a, b, c)]).target_endpoints[0]
        for _ in range(6)
    ]
    assert picks == [a, b, c, a, b, c]


def test_rotation_never_promotes_a_lower_score(s, p, make):
    a, b, c = make("a"), make("b"), make("c")
    picker = p.MaxScorePicker()
    for _ in range(5):
        scored = [s.ScoredEndpoint(c, 0.1), s.ScoredEndpoint(a, 0.9), s.ScoredEndpoint(b, 0.9)]
        assert picker.pick(scored).target_endpoints[0] in (a, b)


def test_random_picker_is_roughly_uniform(s, p, make):
    endpoints = [make(n) for n in ("a", "b", "c")]
    picker = p.RandomPicker(seed=1)
    counts = collections.Counter(
        picker.pick([s.ScoredEndpoint(e, i) for i, e in enumerate(endpoints)]).target_endpoints[0]
        for _ in range(3000)
    )
    assert all(900 < counts[e] < 1100 for e in endpoints), counts


def test_single_profile_scheduling(s, p, make, request_):
    a, b = make("a", queue=4), make("b", queue=1)
    profile = s.SchedulerProfile(
        scorers=[s.WeightedScorer(p.QueueScorer(), 1.0)], picker=p.MaxScorePicker()
    )
    scheduler = s.Scheduler(p.SingleProfileHandler(), {"default": profile})
    result = scheduler.schedule(request_, [a, b])
    assert result.primary_profile_name == "default"
    assert result.primary().target_endpoints == [b]


def test_a_failed_profile_surfaces_its_own_error(s, p, make, request_):
    profile = s.SchedulerProfile(filters=[Recording(keep=set())], picker=p.MaxScorePicker())
    scheduler = s.Scheduler(p.SingleProfileHandler(), {"default": profile})
    with pytest.raises(s.NoEndpointsError):
        scheduler.schedule(request_, [make("a")])


def test_no_profiles_is_a_scheduling_error(s, p, make, request_):
    scheduler = s.Scheduler(p.SingleProfileHandler(), {})
    with pytest.raises(s.SchedulingError):
        scheduler.schedule(request_, [make("a")])


def test_the_handler_loop_runs_profiles_in_rounds(s, p, make, request_):
    """A prefill/decode-shaped handler: decode first, then prefill only if decode ran."""
    pre, dec = make("pre", labels={"role": "prefill"}), make("dec", labels={"role": "decode"})
    calls = []

    class TwoStep:
        name = "two-step"

        def pick(self, request, profiles, results):
            calls.append(sorted(results))
            if "decode" not in results:
                return {"decode": profiles["decode"]}
            if "prefill" not in results:
                return {"prefill": profiles["prefill"]}
            return {}

        def process_results(self, request, results):
            return s.SchedulingResult(dict(results), "decode")

    def only(role):
        return s.SchedulerProfile(
            filters=[p.LabelSelectorFilter({"role": role})], picker=p.MaxScorePicker()
        )

    scheduler = s.Scheduler(TwoStep(), {"decode": only("decode"), "prefill": only("prefill")})
    result = scheduler.schedule(request_, [pre, dec])
    assert calls == [[], ["decode"], ["decode", "prefill"]]
    assert result.primary().target_endpoints == [dec]
    assert result.profile_results["prefill"].target_endpoints == [pre]
