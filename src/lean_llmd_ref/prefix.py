"""Day 5 - approximate prefix-cache-aware routing.

Shaped like llm-d-router's ``approx-prefix-cache-producer`` and
``prefix-cache-scorer``. The router cannot see a model server's KV cache, so
it remembers where it sent each prefix: block hashes in a per-endpoint LRU.
"""

import struct
import threading
from collections import OrderedDict
from dataclasses import dataclass
from typing import Any

import xxhash

from lean_llmd_ref.datastore import Endpoint
from lean_llmd_ref.scheduling import InferenceRequest, SchedulingResult

BYTES_PER_TOKEN = 4
DEFAULT_BLOCK_SIZE_TOKENS = 16
MIN_BLOCK_SIZE_TOKENS = 64
DEFAULT_LRU_CAPACITY_PER_SERVER = 31250
DEFAULT_MAX_PREFIX_BLOCKS = 2048
PREFIX_MATCH_KEY = "prefix-cache-match-info"


def prompt_bytes(body: dict[str, Any]) -> bytes:
    """The bytes of a request that a prefix cache would see.

    - ``/v1/completions``: the ``prompt`` string; a list of strings is joined
      with no separator.
    - ``/v1/chat/completions``: for each message, its ``role`` then its
      string ``content``, concatenated in order.
    - Anything else: empty.
    """
    if "messages" in body:
        out = bytearray()
        for message in body["messages"]:
            out += str(message.get("role", "")).encode()
            content = message.get("content")
            if isinstance(content, str):
                out += content.encode()
        return bytes(out)
    prompt = body.get("prompt", "")
    if isinstance(prompt, list):
        prompt = "".join(p for p in prompt if isinstance(p, str))
    return str(prompt).encode()


def estimate_tokens(raw: bytes) -> list[int]:
    """Pseudo-tokens without a tokenizer: zero-pad to a multiple of 4, read little-endian uint32s.

    Upstream's ``estimate`` backend. Equal byte prefixes give equal token
    prefixes, which is all a prefix cache key needs.
    """
    if not raw:
        return []
    if len(raw) % BYTES_PER_TOKEN:
        raw = raw + b"\0" * (BYTES_PER_TOKEN - len(raw) % BYTES_PER_TOKEN)
    return list(struct.unpack(f"<{len(raw) // BYTES_PER_TOKEN}I", raw))


def block_hashes(
    target_model: str, token_ids: list[int], block_size: int, max_blocks: int
) -> list[int]:
    """Chained 64-bit block hashes, at most ``max_blocks`` of them.

    - Split ``token_ids`` into blocks of ``block_size``; the last block may be short.
    - A block's content id is xxh64 of its tokens as little-endian uint32 bytes.
    - The chain starts at ``prev = xxh64(target_model as UTF-8)``.
    - Each hash is ``xxh64(le64(content_id) + le64(prev))``, then becomes ``prev``.

    So hash ``i`` identifies the whole prefix ``0..i``, and the model name is
    part of every key.
    """
    prev = xxhash.xxh64_intdigest(target_model.encode())
    hashes: list[int] = []
    for start in range(0, len(token_ids), block_size):
        if len(hashes) >= max_blocks:
            break
        block = token_ids[start : start + block_size]
        content_id = xxhash.xxh64_intdigest(struct.pack(f"<{len(block)}I", *block))
        prev = xxhash.xxh64_intdigest(struct.pack("<QQ", content_id, prev))
        hashes.append(prev)
    return hashes


class Indexer:
    """Which servers probably hold which block. Upstream: ``approximateprefix.indexer``.

    ``hash_to_servers`` answers lookups; one LRU per server bounds memory and
    evicts like the server's own cache would.
    """

    def __init__(self, default_lru_size: int = DEFAULT_LRU_CAPACITY_PER_SERVER) -> None:
        self.default_lru_size = default_lru_size
        self._lock = threading.Lock()
        self._hash_to_servers: dict[int, set[str]] = {}
        self._server_lru: dict[str, OrderedDict[int, None]] = {}
        self._capacity: dict[str, int] = {}

    def add(self, hashes: list[int], server: str, capacity: int = 0) -> None:
        """Record that ``server`` now holds every block in ``hashes``.

        A server's LRU is created on first use with ``capacity`` entries (or
        the default if ``capacity <= 0``). Insert the hashes **last first**,
        so the first block ends up most recently used: eviction then trims a
        prefix from its tail, never its head. Evicting a hash from a server's
        LRU removes that server from the hash's set (and drops an empty set).
        """
        with self._lock:
            lru = self._server_lru.get(server)
            if lru is None:
                lru = self._server_lru[server] = OrderedDict()
                self._capacity[server] = capacity if capacity > 0 else self.default_lru_size
            for h in reversed(hashes):
                lru[h] = None
                lru.move_to_end(h)
                self._hash_to_servers.setdefault(h, set()).add(server)
                while len(lru) > self._capacity[server]:
                    evicted, _ = lru.popitem(last=False)
                    servers = self._hash_to_servers.get(evicted)
                    if servers is not None:
                        servers.discard(server)
                        if not servers:
                            del self._hash_to_servers[evicted]

    def get(self, block_hash: int) -> set[str]:
        """A copy of the set of servers holding ``block_hash`` (empty if none)."""
        with self._lock:
            return set(self._hash_to_servers.get(block_hash, ()))

    def remove_server(self, server: str) -> None:
        """Forget ``server`` entirely (its Pod is gone)."""
        with self._lock:
            lru = self._server_lru.pop(server, None)
            self._capacity.pop(server, None)
            for h in lru or ():
                servers = self._hash_to_servers.get(h)
                if servers is not None:
                    servers.discard(server)
                    if not servers:
                        del self._hash_to_servers[h]

    def servers(self) -> list[str]:
        """Every server the index has an LRU for."""
        with self._lock:
            return list(self._server_lru)

    def size(self, server: str) -> int:
        """How many blocks the index thinks ``server`` holds."""
        with self._lock:
            return len(self._server_lru.get(server, ()))


@dataclass(frozen=True)
class PrefixCacheMatchInfo:
    """Written onto each endpoint before scheduling; read by the scorer."""

    match_blocks: int
    total_blocks: int
    block_size_tokens: int


class ApproxPrefixCacheProducer:
    """Produce per-endpoint prefix matches; learn from each routing decision.

    Upstream: ``approx-prefix-cache-producer``. ``produce`` runs before the
    scheduler, ``pre_request`` after it picks.
    """

    name = "approx-prefix-cache-producer"

    def __init__(
        self,
        block_size_tokens: int = DEFAULT_BLOCK_SIZE_TOKENS,
        lru_capacity_per_server: int = DEFAULT_LRU_CAPACITY_PER_SERVER,
        max_prefix_blocks: int = DEFAULT_MAX_PREFIX_BLOCKS,
        auto_tune: bool = True,
    ) -> None:
        self.block_size_tokens = block_size_tokens
        self.lru_capacity_per_server = lru_capacity_per_server
        self.max_prefix_blocks = max_prefix_blocks
        self.auto_tune = auto_tune
        self.indexer = Indexer(lru_capacity_per_server)
        self._lock = threading.Lock()
        self._pending: dict[str, list[int]] = {}

    def get_block_size(self, endpoints: list[Endpoint]) -> int:
        """Tokens per block.

        With ``auto_tune``, use the first endpoint's ``cache_block_size``
        metric if it is positive; otherwise ``block_size_tokens``. Never less
        than ``MIN_BLOCK_SIZE_TOKENS``.
        """
        size = self.block_size_tokens
        if self.auto_tune and endpoints and endpoints[0].metrics.cache_block_size > 0:
            size = endpoints[0].metrics.cache_block_size
        return max(size, MIN_BLOCK_SIZE_TOKENS)

    def match_longest_prefix(self, hashes: list[int]) -> dict[str, int]:
        """For each server, how many leading blocks it holds.

        Walk the hashes in order; stop at the first hash no server holds.
        A server's count is the number of walked hashes it appears in.
        """
        matched: dict[str, int] = {}
        for h in hashes:
            servers = self.indexer.get(h)
            if not servers:
                break
            for server in servers:
                matched[server] = matched.get(server, 0) + 1
        return matched

    def produce(self, request: InferenceRequest, endpoints: list[Endpoint]) -> None:
        """Hash the request and write a ``PrefixCacheMatchInfo`` onto every endpoint.

        Store it in ``endpoint.attributes[PREFIX_MATCH_KEY]``; a server is
        identified by ``str(endpoint.metadata.id)``. Remember the hashes under
        ``request.request_id`` for ``pre_request``.
        """
        block_size = self.get_block_size(endpoints)
        tokens = estimate_tokens(prompt_bytes(request.body))
        hashes = block_hashes(request.target_model, tokens, block_size, self.max_prefix_blocks)
        matched = self.match_longest_prefix(hashes)
        for endpoint in endpoints:
            info = PrefixCacheMatchInfo(
                matched.get(str(endpoint.metadata.id), 0), len(hashes), block_size
            )
            endpoint.attributes[PREFIX_MATCH_KEY] = info
        with self._lock:
            self._pending[request.request_id] = hashes

    def pre_request(self, request: InferenceRequest, result: SchedulingResult) -> None:
        """The request is going to the primary profile's first endpoint: index it there.

        The LRU capacity is that endpoint's ``cache_num_blocks`` when
        ``auto_tune`` and positive, else ``lru_capacity_per_server``. Forget
        the pending hashes either way.
        """
        with self._lock:
            hashes = self._pending.pop(request.request_id, None)
        targets = result.primary().target_endpoints
        if hashes is None or not targets:
            return
        target = targets[0]
        capacity = self.lru_capacity_per_server
        if self.auto_tune and target.metrics.cache_num_blocks > 0:
            capacity = target.metrics.cache_num_blocks
        self.indexer.add(hashes, str(target.metadata.id), capacity)


class PrefixCacheScorer:
    """Prefer the endpoint holding the longest prefix. Upstream: ``prefix-cache-scorer``."""

    name = "prefix-cache-scorer"

    def score(self, request: InferenceRequest, endpoints: list[Endpoint]) -> dict[Endpoint, float]:
        """``match_blocks / total_blocks`` from each endpoint's ``PrefixCacheMatchInfo``.

        No info, or ``total_blocks == 0``, scores 0.
        """
        scores: dict[Endpoint, float] = {}
        for endpoint in endpoints:
            info = endpoint.attributes.get(PREFIX_MATCH_KEY)
            if not isinstance(info, PrefixCacheMatchInfo) or info.total_blocks == 0:
                scores[endpoint] = 0.0
            else:
                scores[endpoint] = info.match_blocks / info.total_blocks
        return scores
