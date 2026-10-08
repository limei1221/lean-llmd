"""Day 14 (optional) - real vLLM on a GPU.

Nothing here needs a GPU: the API server validates your objects with a
server-side dry run (it accepts ``nvidia.com/gpu`` without any GPU nodes),
and the pure tests check that a vLLM Pod drops into the course exactly where
a simulator Pod did. The lab itself runs on a rented GPU machine.
"""

import json

import pytest

import k8s


@pytest.fixture
def gpu(mod):
    return mod("gpu")


def flags(args):
    return {a: args[i + 1] for i, a in enumerate(args) if a.startswith("--") and i + 1 < len(args)}


def test_vllm_args(gpu):
    args = gpu.vllm_args("Qwen/Qwen3-0.6B", max_model_len=4096, tensor_parallel_size=2)
    assert args[0] == "Qwen/Qwen3-0.6B", "the model is positional and first"
    f = flags(args)
    assert f["--port"] == "8000" and f["--max-model-len"] == "4096"
    assert f["--tensor-parallel-size"] == "2"
    assert "--enable-prefix-caching" in args
    assert "--kv-events-config" not in args
    with_events = gpu.vllm_args("m/x", kv_events_port=5557)
    config = json.loads(with_events[with_events.index("--kv-events-config") + 1])
    assert config["endpoint"] == "tcp://*:5557" and config["publisher"] == "zmq"
    assert config["enable_kv_cache_events"] is True
    assert config["topic"] == "kv@$(POD_IP):8000@m/x", "the topic day 8 parses, POD_IP expanded"


def test_a_vllm_pod_drops_in_for_the_simulator(gpu, mod):
    sim = mod("manifests").sim_deployment(name="model")
    d = gpu.vllm_deployment("model", "ns", "Qwen/Qwen3-0.6B", gpus=2, cache_claim="cache")
    assert (
        d["spec"]["template"]["metadata"]["labels"] == sim["spec"]["template"]["metadata"]["labels"]
    )
    assert d["spec"]["selector"] == sim["spec"]["selector"]
    (c,) = d["spec"]["template"]["spec"]["containers"]
    assert c["command"] == ["vllm", "serve"] and c["args"][0] == "Qwen/Qwen3-0.6B"
    assert (
        c["resources"]["limits"]["nvidia.com/gpu"]
        == 2
        == c["resources"]["requests"]["nvidia.com/gpu"]
    )
    assert {"name": "http", "containerPort": 8000, "protocol": "TCP"} in c["ports"]
    env = {e["name"]: e["valueFrom"] for e in c["env"]}
    assert env["POD_IP"] == {"fieldRef": {"fieldPath": "status.podIP"}}
    assert env["HF_TOKEN"]["secretKeyRef"] == {"name": "llm-d-hf-token", "key": "HF_TOKEN"}
    volumes = {v["name"]: v for v in d["spec"]["template"]["spec"]["volumes"]}
    assert volumes["shm"]["emptyDir"]["medium"] == "Memory"
    assert volumes["model-cache"]["persistentVolumeClaim"]["claimName"] == "cache"
    assert c["readinessProbe"]["httpGet"]["path"] == "/v1/models"
    startup = c["startupProbe"]
    assert startup["periodSeconds"] * startup["failureThreshold"] >= 1800, "30 minutes to load"
    tolerations = d["spec"]["template"]["spec"]["tolerations"]
    assert {"key": "nvidia.com/gpu", "operator": "Exists", "effect": "NoSchedule"} in tolerations


def test_secret_and_cache(gpu):
    s = gpu.hf_token_secret("ns", "hf_abc")
    assert s["kind"] == "Secret" and s["stringData"] == {"HF_TOKEN": "hf_abc"}
    assert s["metadata"]["name"] == "llm-d-hf-token"
    pvc = gpu.model_cache_pvc("cache", "ns", "50Gi")
    assert pvc["spec"]["resources"]["requests"]["storage"] == "50Gi"
    assert pvc["spec"]["accessModes"] == ["ReadWriteOnce"]


@pytest.mark.kind
def test_the_api_server_accepts_the_gpu_objects(gpu):
    objects = [
        gpu.hf_token_secret("default", "hf_fake"),
        gpu.model_cache_pvc("cache", "default"),
        gpu.vllm_deployment("model", "default", "Qwen/Qwen3-0.6B", cache_claim="cache"),
    ]
    out = k8s.apply(objects, dry_run=True)
    assert out.count("created (server dry run)") == 3
