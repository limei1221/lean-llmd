"""Day 2 - reading a model server's state from /metrics.

Oracles: prometheus_client's own parser for the text format, values worked
out by hand, and a live simulator whose limits we set (it cannot run more
than ``max_num_seqs`` requests, so under load the rest must queue).
"""

import concurrent.futures
import math
import time

import httpx
import pytest
from prometheus_client.parser import text_string_to_metric_families

import k8s

VLLM_TEXT = """\
# HELP vllm:num_requests_running Number of requests currently running on GPU.
# TYPE vllm:num_requests_running gauge
vllm:num_requests_running{model_name="mini/sim"} 3
# HELP vllm:num_requests_waiting Number of requests waiting to be processed.
# TYPE vllm:num_requests_waiting gauge
vllm:num_requests_waiting{model_name="mini/sim"} 5
# TYPE vllm:kv_cache_usage_perc gauge
vllm:kv_cache_usage_perc{model_name="mini/sim"} 0.25
# TYPE vllm:cache_config_info gauge
vllm:cache_config_info{block_size="16",cache_dtype="auto",num_cpu_blocks="0",num_gpu_blocks="512"} 1
# TYPE vllm:time_to_first_token_seconds histogram
vllm:time_to_first_token_seconds_bucket{model_name="mini/sim",le="0.1"} 4
vllm:time_to_first_token_seconds_bucket{model_name="mini/sim",le="+Inf"} 9
vllm:time_to_first_token_seconds_sum{model_name="mini/sim"} 2.5
vllm:time_to_first_token_seconds_count{model_name="mini/sim"} 9
"""

TRICKY_TEXT = r"""
# a comment that is neither HELP nor TYPE

weird{path="/a,b}c",quote="say \"hi\"",slash="C:\\dir",nl="x\ny"} 1.5
spaced{ a = "1" , b="2" } -3
nan_metric NaN
inf_metric{s="up"} +Inf
neg_inf -Inf
stamped{k="v"} 7 1712345678901
"""


def flatten_with_prometheus_client(text):
    out = []
    for family in text_string_to_metric_families(text):
        for s in family.samples:
            ts = None if s.timestamp is None else round(float(s.timestamp) * 1000)
            out.append((s.name, tuple(sorted(s.labels.items())), s.value, ts))
    return out


def flatten_mine(families):
    out = []
    for name, samples in families.items():
        for s in samples:
            assert s.name == name
            out.append((s.name, tuple(sorted(s.labels.items())), s.value, s.timestamp_ms))
    return out


def same_samples(a, b):
    def key(x):
        return (x[0], x[1])

    a, b = sorted(a, key=key), sorted(b, key=key)
    assert [key(x) for x in a] == [key(x) for x in b]
    for x, y in zip(a, b, strict=True):
        assert (math.isnan(x[2]) and math.isnan(y[2])) or x[2] == y[2], (x, y)
        assert x[3] == y[3], (x, y)


@pytest.fixture
def metrics(mod):
    return mod("metrics")


@pytest.mark.parametrize("text", [VLLM_TEXT, TRICKY_TEXT], ids=["vllm", "tricky"])
def test_parse_text_agrees_with_prometheus_client(metrics, text):
    same_samples(flatten_mine(metrics.parse_text(text)), flatten_with_prometheus_client(text))


def test_histogram_samples_are_keyed_by_their_own_names(metrics):
    families = metrics.parse_text(VLLM_TEXT)
    assert len(families["vllm:time_to_first_token_seconds_bucket"]) == 2
    assert families["vllm:time_to_first_token_seconds_count"][0].value == 9


def test_parse_spec(metrics):
    assert metrics.parse_spec("") is None
    assert metrics.parse_spec("vllm:num_requests_waiting") == metrics.Spec(
        "vllm:num_requests_waiting", {}
    )
    bare = metrics.parse_spec("vllm:lora_requests_info{max_lora=4, running=x}")
    assert bare == metrics.Spec("vllm:lora_requests_info", {"max_lora": "4", "running": "x"})
    quoted = metrics.parse_spec('a_b{c="d", e="f"}')
    assert quoted == metrics.Spec("a_b", {"c": "d", "e": "f"})
    for bad in ["9starts_with_digit", "ok{9bad=x}", "has space{a=b}"]:
        with pytest.raises(ValueError):
            metrics.parse_spec(bad)


def test_the_newest_matching_sample_wins(metrics):
    text = 'm{pod="a"} 1 1000\nm{pod="b"} 2 3000\nm{pod="a"} 3 2000\n'
    families = metrics.parse_text(text)
    assert metrics.get_latest_metric(metrics.Spec("m", {}), families).value == 2
    assert metrics.get_latest_metric(metrics.Spec("m", {"pod": "a"}), families).value == 3


def test_without_timestamps_the_first_match_wins(metrics):
    families = metrics.parse_text('m{a="1",b="x"} 10\nm{a="1",b="y"} 20\n')
    assert metrics.get_latest_metric(metrics.Spec("m", {"a": "1"}), families).value == 10


def test_a_missing_series_is_a_lookup_error(metrics):
    families = metrics.parse_text('m{a="1"} 1\n')
    with pytest.raises(LookupError):
        metrics.get_latest_metric(metrics.Spec("absent", {}), families)
    with pytest.raises(LookupError):
        metrics.get_latest_metric(metrics.Spec("m", {"a": "2"}), families)


def test_extract_reads_every_vllm_field(metrics):
    current = metrics.Metrics()
    updated, errors = metrics.extract(
        metrics.parse_text(VLLM_TEXT), metrics.VLLM_MAPPING, current, now=42.0
    )
    assert errors == []
    assert updated.waiting_queue_size == 5
    assert updated.running_requests_size == 3
    assert updated.kv_cache_usage_percent == 0.25
    assert updated.cache_block_size == 16
    assert updated.cache_num_blocks == 512
    assert updated.update_time == 42.0
    assert current == metrics.Metrics(), "extract must not mutate the snapshot readers hold"


def test_a_missing_metric_keeps_its_old_value_and_reports_an_error(metrics):
    current = metrics.Metrics(waiting_queue_size=9, kv_cache_usage_percent=0.5)
    text = 'vllm:num_requests_running{model_name="m"} 2\n'
    updated, errors = metrics.extract(
        metrics.parse_text(text), metrics.VLLM_MAPPING, current, now=1.0
    )
    assert updated.running_requests_size == 2
    assert updated.waiting_queue_size == 9
    assert updated.kv_cache_usage_percent == 0.5
    assert len(errors) == 3, errors
    assert updated.update_time == 1.0


def test_nothing_read_means_no_new_update_time(metrics):
    current = metrics.Metrics(update_time=5.0)
    updated, errors = metrics.extract({}, metrics.VLLM_MAPPING, current, now=9.0)
    assert updated.update_time == 5.0
    assert len(errors) == 4


@pytest.mark.docker
def test_a_saturated_simulator_queues_what_it_cannot_run(metrics, mod):
    manifests = mod("manifests")
    config = manifests.SimConfig(max_num_seqs=2, inter_token_latency_ms=100)
    with k8s.sim_container(manifests.sim_args(manifests.MODEL, config)) as url:
        idle, _ = metrics.extract(
            metrics.scrape(url + "/metrics"), metrics.VLLM_MAPPING, metrics.Metrics(), 0.0
        )
        assert (idle.running_requests_size, idle.waiting_queue_size) == (0, 0)
        assert idle.cache_block_size == config.block_size
        assert idle.cache_num_blocks == config.kv_cache_blocks

        def complete():
            body = {
                "model": manifests.MODEL,
                "prompt": "hello " * 50,
                "max_tokens": 30,
                "ignore_eos": True,  # exactly 30 tokens: nobody finishes early
            }
            return httpx.post(url + "/v1/completions", json=body, timeout=30).status_code

        with concurrent.futures.ThreadPoolExecutor(6) as pool:
            jobs = [pool.submit(complete) for _ in range(6)]
            time.sleep(1.0)
            busy, errors = metrics.extract(
                metrics.scrape(url + "/metrics"), metrics.VLLM_MAPPING, metrics.Metrics(), 1.0
            )
            assert [j.result() for j in jobs] == [200] * 6
    assert errors == []
    assert busy.running_requests_size == 2, "the simulator runs at most max_num_seqs"
    assert busy.waiting_queue_size == 4
    assert busy.kv_cache_usage_percent > 0


def test_scrape_raises_on_an_error_status(metrics):
    transport = httpx.MockTransport(lambda request: httpx.Response(503, text="down"))
    with httpx.Client(transport=transport) as client, pytest.raises(httpx.HTTPStatusError):
        metrics.scrape("http://sim/metrics", client)
