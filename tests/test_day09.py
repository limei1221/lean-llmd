"""Day 9 - prefill/decode disaggregation.

Oracles: hand-built endpoints for the role filters, decider and handler; two
live simulators for the sidecar. The prefill simulator's own counters say
whether it ran a remote-decode prefill, and the decode simulator's TTFT says
whether it pulled the KV cache instead of computing it.
"""

import re
import threading

import httpx
import pytest

import k8s


@pytest.fixture
def dg(mod):
    return mod("disagg")


@pytest.fixture
def sc(mod):
    return mod("pd_sidecar")


@pytest.fixture
def make(mod):
    ds = mod("datastore")

    def endpoint(name, role=None):
        labels = {"app": "sim"} if role is None else {"app": "sim", "llm-d.ai/role": role}
        ip = f"10.0.0.{len(name)}"
        meta = ds.EndpointMetadata(ds.NamespacedName("ns", name), name, ip, 8000, labels)
        return ds.Endpoint(meta)

    return endpoint


def req(mod, prompt):
    return mod("scheduling").InferenceRequest("r", "m", {"model": "m", "prompt": prompt})


def test_role_filters(mod, dg, make):
    roles = ["prefill", "decode", "prefill-decode", "encode-prefill", None, "encode"]
    eps = [make(f"e{i}", r) for i, r in enumerate(roles)]
    r = req(mod, "x")
    assert [e.metadata.labels.get("llm-d.ai/role") for e in dg.PrefillFilter().filter(r, eps)] == [
        "prefill",
        "prefill-decode",
        "encode-prefill",
    ]
    assert [e.metadata.labels.get("llm-d.ai/role") for e in dg.DecodeFilter().filter(r, eps)] == [
        "decode",
        "prefill-decode",
        None,
    ], "an unlabelled pod decodes"


def test_the_decider_counts_uncached_tokens(mod, dg, make):
    prefix = mod("prefix")
    decider = dg.PrefixBasedPDDecider(non_cached_tokens=100)
    long_prompt = "x" * 800  # 200 pseudo-tokens
    cold = make("cold")
    assert decider.disaggregate(req(mod, long_prompt), cold)
    assert not decider.disaggregate(req(mod, "x" * 200), cold), "50 tokens is below the bar"
    warm = make("warm")
    warm.attributes[prefix.PREFIX_MATCH_KEY] = prefix.PrefixCacheMatchInfo(2, 3, 64)
    assert not decider.disaggregate(req(mod, long_prompt), warm), "200 - 128 cached = 72 < 100"
    assert not dg.PrefixBasedPDDecider(0).disaggregate(req(mod, long_prompt), cold)


def scheduler(mod, dg, threshold):
    s, p = mod("scheduling"), mod("plugins")
    profiles = {
        "decode": s.SchedulerProfile([dg.DecodeFilter()], [], p.MaxScorePicker()),
        "prefill": s.SchedulerProfile([dg.PrefillFilter()], [], p.MaxScorePicker()),
    }
    return s.Scheduler(dg.DisaggProfileHandler(dg.PrefixBasedPDDecider(threshold)), profiles)


def test_short_prompts_decode_only(mod, dg, make):
    pre, dec = make("pre", "prefill"), make("dec", "decode")
    r = req(mod, "short")
    result = scheduler(mod, dg, 100).schedule(r, [pre, dec])
    assert result.primary_profile_name == "decode"
    assert result.primary().target_endpoints == [dec]
    assert "prefill" not in result.profile_results
    assert dg.PREFILL_ENDPOINT_HEADER not in r.set_headers


def test_long_prompts_get_a_prefill_endpoint(mod, dg, make):
    pre, dec = make("pre", "prefill"), make("dec", "decode")
    r = req(mod, "y" * 2000)
    r.set_headers[dg.PREFILL_ENDPOINT_HEADER] = "spoofed:1"
    result = scheduler(mod, dg, 100).schedule(r, [pre, dec])
    assert result.primary().target_endpoints == [dec]
    assert result.profile_results["prefill"].target_endpoints == [pre]
    assert r.set_headers[dg.PREFILL_ENDPOINT_HEADER] == pre.metadata.host_port


def test_no_decoder_is_an_error(mod, dg, make):
    with pytest.raises(mod("scheduling").NoEndpointsError):
        scheduler(mod, dg, 100).schedule(req(mod, "x"), [make("pre", "prefill")])


def test_the_prefill_request_asks_for_one_token(sc):
    body = {
        "model": "m",
        "prompt": "p",
        "max_tokens": 50,
        "stream": True,
        "stream_options": {"include_usage": True},
    }
    pb = sc.prefill_body(body)
    assert (pb["max_tokens"], pb["stream"]) == (1, False)
    assert "stream_options" not in pb
    assert pb["kv_transfer_params"]["do_remote_decode"] is True
    assert pb["kv_transfer_params"]["do_remote_prefill"] is False
    assert body["max_tokens"] == 50, "do not modify the client's body"
    params = {"do_remote_prefill": True, "remote_host": "h"}
    db = sc.decode_body(body, {"kv_transfer_params": params})
    assert db["kv_transfer_params"] == params and db["max_tokens"] == 50
    assert "kv_transfer_params" not in sc.decode_body(body, {})


def test_install_builds_two_profiles(mod, dg):
    epp = mod("epp")
    opts = epp.Options(
        "pool", "ns", features=("disagg",), feature_args=("--pd-non-cached-tokens", "64")
    )
    rt = epp.build(opts)
    assert set(rt.profiles) == {"decode", "prefill"}
    assert isinstance(rt.profile_handler, dg.DisaggProfileHandler)
    assert rt.profile_handler.decider.non_cached_tokens == 64


def test_pd_deployments(mod, dg):
    manifests = mod("manifests")
    prefill, decode = dg.pd_deployments(
        "sim", "ns", manifests.sim_deployment(), "img:1", ["python", "-m", "x.pd_sidecar"]
    )
    assert prefill["metadata"]["name"] == "sim-prefill"
    assert decode["metadata"]["name"] == "sim-decode"
    for d, role in ((prefill, "prefill"), (decode, "decode")):
        labels = d["spec"]["template"]["metadata"]["labels"]
        assert labels["llm-d.ai/role"] == role and labels["app"] == "sim"
        assert d["spec"]["selector"]["matchLabels"]["llm-d.ai/role"] == role
    sidecar, sim = decode["spec"]["template"]["spec"]["containers"]
    assert sidecar["name"] == "sidecar" and sidecar["ports"][0]["containerPort"] == 8000
    assert "readinessProbe" in sidecar and "readinessProbe" not in sim
    assert sim["args"][sim["args"].index("--port") + 1] == "8200"
    assert "--kv-cache-transfer-time-per-token" in sim["args"]


def counter(text, name, **labels):
    total = 0.0
    for line in text.splitlines():
        if line.startswith(name + "{") and all(f'{k}="{v}"' in line for k, v in labels.items()):
            total += float(line.rsplit(" ", 1)[1])
    return total


def ttft_mean(text):
    def value(suffix):
        name = f"vllm:time_to_first_token_seconds_{suffix}"
        return float(re.search(rf"^{name}\{{.*\}} (\S+)$", text, re.M).group(1))

    return value("sum") / value("count")


@pytest.mark.docker
def test_the_sidecar_runs_prefill_remotely(mod, sc):
    manifests = mod("manifests")
    slow_prefill = manifests.SimConfig(prefill_overhead_ms=10, prefill_per_token_ms=5)
    args = manifests.sim_args(manifests.MODEL, slow_prefill)
    decode_args = [*args, "--kv-cache-transfer-time-per-token", "1ms"]
    with k8s.sim_container(args) as prefill_url, k8s.sim_container(decode_args) as decode_url:
        server = sc.SidecarServer(0, decode_url)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        sidecar = f"http://127.0.0.1:{server.server_address[1]}"
        prompt = " ".join(f"w{i}" for i in range(200))
        body = {"model": manifests.MODEL, "prompt": prompt, "max_tokens": 4, "ignore_eos": True}
        try:
            pd = httpx.post(
                sidecar + "/v1/completions",
                json=body,
                timeout=30,
                headers={"x-prefiller-host-port": prefill_url.removeprefix("http://")},
            )
            assert pd.status_code == 200 and pd.json()["usage"]["completion_tokens"] == 4
            prefill_metrics = httpx.get(prefill_url + "/metrics").text
            remote = counter(
                prefill_metrics, "vllm:request_success_total", finish_reason="remote_decode"
            )
            assert remote == 1
            remote_ttft = ttft_mean(httpx.get(sidecar + "/metrics").text)
            assert remote_ttft < 0.5, "transfer 200 tokens at 1 ms, not prefill them at 5 ms"

            stream = dict(body, stream=True, prompt=prompt + " again")
            with httpx.stream("POST", sidecar + "/v1/completions", json=stream, timeout=30) as r:
                chunks = [line for line in r.iter_lines() if line.startswith("data: ")]
            assert chunks[-1] == "data: [DONE]" and len(chunks) >= 3, "streaming passes through"
            prefill_after = httpx.get(prefill_url + "/metrics").text
            served = counter(prefill_after, "vllm:request_success_total")
            assert served == 1, "no header, no prefill"
        finally:
            server.shutdown()


@pytest.mark.kind
def test_pd_on_kind(mod, dg, package):
    if not k8s.image_present_in_kind():
        pytest.skip("the lean-llmd:dev image is not in kind; run: uv run lean-llmd image")
    manifests, disc, deploy = mod("manifests"), mod("discovery"), mod("deploy")
    sidecar = ["python", "-m", f"{package}.pd_sidecar", "--port", "8000", "--vllm-port", "8200"]
    flags = ("--feature", "disagg", "--pd-non-cached-tokens", "200")
    with k8s.temp_namespace("day09") as ns:
        k8s.apply(
            [
                *dg.pd_deployments(
                    "sim", ns, manifests.sim_deployment(namespace=ns), "lean-llmd:dev", sidecar
                ),
                disc.inference_pool("pool", ns, {"app": "sim"}, manifests.SIM_PORT, "epp"),
                *deploy.epp_objects("epp", ns, "pool", package=package, extra_args=flags),
            ]
        )
        for name in ("sim-prefill", "sim-decode", "epp"):
            k8s.kubectl("rollout", "status", f"deployment/{name}", "-n", ns, "--timeout=240s")

        def remote_prefills():
            (pod,) = k8s.get_json("pods", "-n", ns, "-l", "llm-d.ai/role=prefill")["items"]
            text = k8s.pod_metrics(ns, pod["metadata"]["name"])
            return counter(text, "vllm:request_success_total", finish_reason="remote_decode")

        def send(prompt):
            out = k8s.run_python_in_cluster(
                ns,
                f"""
                import httpx
                r = httpx.post("http://epp/v1/completions", timeout=30, json={{
                    "model": "mini/sim", "prompt": {prompt!r}, "max_tokens": 3}})
                print(r.status_code)
                """,
            )
            return int(out.strip().splitlines()[-1])

        before = remote_prefills()
        assert send("short question") == 200
        assert remote_prefills() == before, "a short prompt decodes in place"
        assert send(" ".join(f"word{i}" for i in range(400))) == 200
        assert remote_prefills() == before + 1, "a long cold prompt is prefilled remotely"
