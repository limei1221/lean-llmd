"""Day 8 - precise prefix-cache routing from KV events.

Shaped like llm-d-router's ``pkg/kvcache`` (the index and token processor)
and ``pkg/kvevents`` (the pool and subscribers). Instead of guessing where a
prefix went, the EPP subscribes to every model server's KV events and
indexes what each one actually holds.

The engine's block hashes depend on its hash seed and implementation; the
EPP cannot recompute them. So it computes its own *request keys* from token
ids, and each BlockStored event (which carries its tokens) maps the engine's
keys onto the EPP's. Run it with ``--feature precise``.
"""

import argparse
import struct
import threading
from typing import Any

import httpx
import xxhash
import zmq

from lean_llmd_ref.datastore import Endpoint
from lean_llmd_ref.epp import Runtime
from lean_llmd_ref.kvevents import (
    KV_EVENTS_PORT,
    TOPIC_PREFIX,
    AllBlocksCleared,
    BlockRemoved,
    BlockStored,
    decode_batch,
    parse_topic,
)
from lean_llmd_ref.prefix import (
    PREFIX_MATCH_KEY,
    ApproxPrefixCacheProducer,
    PrefixCacheMatchInfo,
)
from lean_llmd_ref.scheduling import InferenceRequest, SchedulingResult

DEFAULT_BLOCK_SIZE = 16


def init_key(model: str, seed: str = "") -> int:
    """The chain's starting key for ``model``.

    ``xxh64(le64(xxh64(seed)) + le64(len(model)) + model)``: the seed's hash,
    then the model name, length-prefixed (llm-d's xxh64 token processor).
    """
    seed_hash = xxhash.xxh64_intdigest(seed.encode())
    name = model.encode()
    return xxhash.xxh64_intdigest(struct.pack("<QQ", seed_hash, len(name)) + name)


def request_keys(
    parent: int | None, token_ids: list[int], model: str, block_size: int, seed: str = ""
) -> list[int]:
    """Request keys for every **full** block of ``token_ids``; a partial tail has none.

    Start from ``parent``, or from ``init_key(model, seed)`` when it is
    ``None``. Each key is ``xxh64(le64(previous) + tokens as le32)``.
    """
    previous = init_key(model, seed) if parent is None else parent
    keys: list[int] = []
    for start in range(0, len(token_ids) - block_size + 1, block_size):
        block = token_ids[start : start + block_size]
        previous = xxhash.xxh64_intdigest(
            struct.pack("<Q", previous) + struct.pack(f"<{len(block)}I", *block)
        )
        keys.append(previous)
    return keys


class KVBlockIndex:
    """Which pods hold which request keys, and how engine keys map onto them.

    Upstream: ``kvblock.Index`` (in-memory backend).
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._engine_to_request: dict[int, int] = {}
        self._pods_by_key: dict[int, set[str]] = {}
        self._engine_keys_by_pod: dict[str, set[int]] = {}

    def add(self, engine_keys: list[int], request_keys: list[int], pod: str) -> None:
        """Record that ``pod`` holds these blocks; pair engine and request keys by position."""
        with self._lock:
            for engine_key, key in zip(engine_keys, request_keys, strict=False):
                self._engine_to_request[engine_key] = key
                self._pods_by_key.setdefault(key, set()).add(pod)
                self._engine_keys_by_pod.setdefault(pod, set()).add(engine_key)

    def get_request_key(self, engine_key: int) -> int | None:
        with self._lock:
            return self._engine_to_request.get(engine_key)

    def evict(self, engine_key: int, pod: str) -> None:
        """``pod`` dropped this block. Unknown keys are ignored."""
        with self._lock:
            key = self._engine_to_request.get(engine_key)
            self._engine_keys_by_pod.get(pod, set()).discard(engine_key)
            if key is None:
                return
            pods = self._pods_by_key.get(key)
            if pods is not None:
                pods.discard(pod)
                if not pods:
                    del self._pods_by_key[key]

    def clear(self, pod: str) -> None:
        """Forget everything ``pod`` holds (it cleared its cache, or it is gone)."""
        with self._lock:
            for engine_key in self._engine_keys_by_pod.pop(pod, set()):
                key = self._engine_to_request.get(engine_key)
                pods = self._pods_by_key.get(key) if key is not None else None
                if pods is not None:
                    pods.discard(pod)
                    if not pods and key is not None:
                        del self._pods_by_key[key]

    def longest_prefix(self, keys: list[int]) -> dict[str, int]:
        """Per pod, how many keys it holds **consecutively from the first**.

        Unlike day 5's approximate index, a gap ends a pod's run: a block
        after a missing one is useless to vLLM.
        """
        with self._lock:
            runs: dict[str, int] = {}
            alive: set[str] | None = None
            for i, key in enumerate(keys):
                pods = self._pods_by_key.get(key, set())
                alive = set(pods) if alive is None else alive & pods
                if not alive:
                    break
                for pod in alive:
                    runs[pod] = i + 1
            return runs

    def pods(self) -> list[str]:
        with self._lock:
            return list(self._engine_keys_by_pod)


class EventProcessor:
    """Apply KV-event batches to the index. Upstream: ``kvevents.Pool``."""

    def __init__(self, index: KVBlockIndex, block_size: int = DEFAULT_BLOCK_SIZE) -> None:
        self.index = index
        self.block_size = block_size

    def process(self, topic: str, payload: bytes) -> None:
        """Decode one message and apply each event for the pod named in the topic.

        - BlockStored: the parent's request key comes from the index (0 means
          no parent); if the parent is unknown, skip the event. Compute request
          keys from the event's tokens and add them.
        - BlockRemoved: evict each engine key.
        - AllBlocksCleared: clear the pod.
        """
        pod, model = parse_topic(topic)
        for event in decode_batch(payload).events:
            if isinstance(event, BlockStored):
                parent: int | None = None
                if event.parent_block_hash:
                    parent = self.index.get_request_key(event.parent_block_hash)
                    if parent is None:
                        continue
                keys = request_keys(parent, event.token_ids, model, self.block_size)
                self.index.add(event.block_hashes, keys, pod)
            elif isinstance(event, BlockRemoved):
                for engine_key in event.block_hashes:
                    self.index.evict(engine_key, pod)
            elif isinstance(event, AllBlocksCleared):
                self.index.clear(pod)


class Subscriber:
    """One ZMQ SUB socket dialing one model server, feeding an ``EventProcessor``."""

    def __init__(self, endpoint: str, processor: EventProcessor) -> None:
        self.endpoint = endpoint
        self.processor = processor
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self.run, daemon=True)

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()

    def run(self) -> None:
        """Receive until stopped.

        Connect a SUB socket to ``endpoint``, subscribe to the ``kv@`` prefix,
        and poll with a short timeout so ``stop`` is noticed. Each message is
        ``[topic, seq, payload]``: hand topic and payload to the processor,
        and skip messages it cannot parse rather than dying.
        """
        context: zmq.Context[zmq.Socket[bytes]] = zmq.Context.instance()
        socket = context.socket(zmq.SUB)
        socket.setsockopt(zmq.LINGER, 0)
        socket.setsockopt(zmq.SUBSCRIBE, TOPIC_PREFIX.encode())
        socket.connect(self.endpoint)
        poller = zmq.Poller()
        poller.register(socket, zmq.POLLIN)
        try:
            while not self._stop.is_set():
                if not dict(poller.poll(200)):
                    continue
                frames = socket.recv_multipart()
                if len(frames) < 3:
                    continue
                try:
                    self.processor.process(frames[0].decode(), frames[-1])
                except (ValueError, IndexError, TypeError):
                    continue
        finally:
            socket.close()


class SubscriberManager:
    """Keep exactly one subscriber per live endpoint. Upstream: ``SubscriberManager``."""

    def __init__(self, processor: EventProcessor, port: int = KV_EVENTS_PORT) -> None:
        self.processor = processor
        self.port = port
        self.subscribers: dict[str, Subscriber] = {}

    def sync(self, endpoints: list[Endpoint]) -> None:
        """Start subscribers for new endpoints; stop and clear the ones that left.

        A subscriber is keyed by the endpoint's ``host_port`` (the pod name
        topics use) and dials ``tcp://<address>:<port>``.
        """
        live = {e.metadata.host_port: e for e in endpoints}
        for pod in [p for p in self.subscribers if p not in live]:
            self.subscribers.pop(pod).stop()
            self.processor.index.clear(pod)
        for pod, endpoint in live.items():
            if pod not in self.subscribers:
                subscriber = Subscriber(
                    f"tcp://{endpoint.metadata.address}:{self.port}", self.processor
                )
                subscriber.start()
                self.subscribers[pod] = subscriber


class RenderTokenizer:
    """Token ids from a model server's own ``/render`` endpoint, so they match its cache."""

    def __init__(self, client: httpx.Client | None = None) -> None:
        self.client = client or httpx.Client(timeout=2.0)

    def tokenize(self, request: InferenceRequest, host_port: str) -> list[int]:
        """POST the request's model and prompt (or messages) to the render endpoint.

        ``/v1/chat/completions/render`` for a body with ``messages``,
        ``/v1/completions/render`` otherwise; the answer is a list whose first
        element has ``token_ids``.
        """
        body: dict[str, Any] = {"model": request.target_model}
        if "messages" in request.body:
            path = "/v1/chat/completions/render"
            body["messages"] = request.body["messages"]
        else:
            path = "/v1/completions/render"
            body["prompt"] = request.body.get("prompt", "")
        response = self.client.post(f"http://{host_port}{path}", json=body)
        response.raise_for_status()
        return list(response.json()[0]["token_ids"])


class PrecisePrefixCacheProducer:
    """Write each endpoint's real prefix match before scheduling.

    Upstream: ``precise-prefix-cache-producer``. It writes the same
    ``PrefixCacheMatchInfo`` day 5's scorer reads, so the scorer is unchanged.
    """

    name = "precise-prefix-cache-producer"

    def __init__(
        self,
        index: KVBlockIndex,
        tokenizer: RenderTokenizer,
        block_size: int = DEFAULT_BLOCK_SIZE,
    ) -> None:
        self.index = index
        self.tokenizer = tokenizer
        self.block_size = block_size

    def produce(self, request: InferenceRequest, endpoints: list[Endpoint]) -> None:
        """Tokenize with the first endpoint's renderer, look the keys up, write match info.

        An endpoint is identified by its ``host_port``. If tokenizing fails,
        write a zero match for everyone rather than failing the request.
        """
        keys: list[int] = []
        if endpoints:
            try:
                tokens = self.tokenizer.tokenize(request, endpoints[0].metadata.host_port)
                keys = request_keys(None, tokens, request.target_model, self.block_size)
            except (httpx.HTTPError, KeyError, IndexError, ValueError):
                keys = []
        runs = self.index.longest_prefix(keys)
        for endpoint in endpoints:
            matched = runs.get(endpoint.metadata.host_port, 0)
            endpoint.attributes[PREFIX_MATCH_KEY] = PrefixCacheMatchInfo(
                matched, len(keys), self.block_size
            )

    def pre_request(self, request: InferenceRequest, result: SchedulingResult) -> None:
        """Nothing to learn here: the model server's events will say what it cached."""


def install(rt: Runtime, argv: list[str]) -> None:
    """The ``precise`` feature: swap the approximate producer for this one.

    Flags: ``--kv-events-port`` (5557) and ``--kv-block-size`` (16, the
    model server's block size). Subscribers follow the datastore every tick.
    """
    p = argparse.ArgumentParser(prog="precise")
    p.add_argument("--kv-events-port", type=int, default=KV_EVENTS_PORT)
    p.add_argument("--kv-block-size", type=int, default=DEFAULT_BLOCK_SIZE)
    a, _ = p.parse_known_args(argv)
    index = KVBlockIndex()
    manager = SubscriberManager(EventProcessor(index, a.kv_block_size), a.kv_events_port)
    producer = PrecisePrefixCacheProducer(index, RenderTokenizer(), a.kv_block_size)
    rt.producers[:] = [
        producer if isinstance(x, ApproxPrefixCacheProducer) else x for x in rt.producers
    ]
    if producer not in rt.producers:
        rt.producers.append(producer)
    rt.prefix = None
    rt.ticks.append(lambda: manager.sync(rt.datastore.pod_list()))
