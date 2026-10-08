"""Day 8 - KV-cache events: what a model server cached and evicted.

Your implementation. Every function below raises until you write it.

    uv run lean-llmd test --day 8         # your code
    uv run lean-llmd test --day 8 --ref   # proves the tests are passable
    uv run lean-llmd diff --day 8         # only after you have a passing run
"""

import copy
from dataclasses import dataclass
from typing import Any

import msgpack

Obj = dict[str, Any]
KV_EVENTS_PORT = 5557
TOPIC_PREFIX = "kv@"


@dataclass(frozen=True)
class BlockStored:
    block_hashes: list[int]
    parent_block_hash: int
    token_ids: list[int]
    block_size: int


@dataclass(frozen=True)
class BlockRemoved:
    block_hashes: list[int]


@dataclass(frozen=True)
class AllBlocksCleared:
    pass


Event = BlockStored | BlockRemoved | AllBlocksCleared


@dataclass(frozen=True)
class EventBatch:
    ts: float
    events: list[Event]
    data_parallel_rank: int | None = None


def decode_event(raw: Any) -> Event | None:
    """One event, in either wire format; ``None`` for a tag this code does not know.

    - Array format: ``[tag, ...fields]``. BlockStored is
      ``["BlockStored", block_hashes, parent_block_hash, token_ids,
      block_size, lora_id, medium, lora_name, extra_keys]``; BlockRemoved is
      ``["BlockRemoved", block_hashes, medium]``.
    - Map format: a dict with ``"type"`` and named fields; a missing or
      ``None`` ``parent_block_hash`` means 0.
    """
    raise NotImplementedError("day 8: implement decode_event()")


def decode_batch(payload: bytes) -> EventBatch:
    """Decode a msgpack batch; skip events ``decode_event`` does not know."""
    raise NotImplementedError("day 8: implement decode_batch()")


def parse_topic(topic: str) -> tuple[str, str]:
    """``kv@10.0.0.1:8000@org/model`` to ``("10.0.0.1:8000", "org/model")``.

    The model name may itself contain ``@``. Raise ``ValueError`` for a topic
    that does not start with ``kv@`` or has no model part.
    """
    raise NotImplementedError("day 8: implement parse_topic()")


def with_kv_events(deployment: Obj, port: int = KV_EVENTS_PORT) -> Obj:
    """A copy of a simulator Deployment that publishes KV events for subscribers to dial.

    Add ``--zmq-endpoint tcp://*:<port>`` (``*`` makes the simulator bind,
    so each EPP connects to each Pod) and ``--event-batch-size 1`` (publish
    every event at once; the default batches for up to a second) to the first
    container's args, and expose the port as ``kv-events``. Do not modify the
    input.
    """
    raise NotImplementedError("day 8: implement with_kv_events()")
