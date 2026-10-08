"""Day 8 - KV-cache events: what a model server says it cached and evicted.

Shaped like llm-d-router's ``pkg/kvevents``. vLLM (and the simulator) publish
each batch on a ZMQ PUB socket as three frames: a topic
``kv@<pod-ip>:<port>@<model>``, an 8-byte sequence number, and a msgpack
payload ``[ts, [event, ...], data_parallel_rank]``.
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
    if isinstance(raw, dict):
        tag = raw.get("type")
        if tag == "BlockStored":
            return BlockStored(
                list(raw.get("block_hashes") or []),
                int(raw.get("parent_block_hash") or 0),
                list(raw.get("token_ids") or []),
                int(raw.get("block_size") or 0),
            )
        if tag == "BlockRemoved":
            return BlockRemoved(list(raw.get("block_hashes") or []))
        if tag == "AllBlocksCleared":
            return AllBlocksCleared()
        return None
    if not isinstance(raw, list | tuple) or not raw:
        return None
    tag = raw[0]
    if tag == "BlockStored":
        return BlockStored(list(raw[1]), int(raw[2] or 0), list(raw[3]), int(raw[4]))
    if tag == "BlockRemoved":
        return BlockRemoved(list(raw[1]))
    if tag == "AllBlocksCleared":
        return AllBlocksCleared()
    return None


def decode_batch(payload: bytes) -> EventBatch:
    """Decode a msgpack batch; skip events ``decode_event`` does not know."""
    raw = msgpack.unpackb(payload, raw=False, strict_map_key=False)
    events = [e for e in (decode_event(r) for r in raw[1]) if e is not None]
    rank = raw[2] if len(raw) > 2 else None
    return EventBatch(float(raw[0]), events, rank)


def parse_topic(topic: str) -> tuple[str, str]:
    """``kv@10.0.0.1:8000@org/model`` to ``("10.0.0.1:8000", "org/model")``.

    The model name may itself contain ``@``. Raise ``ValueError`` for a topic
    that does not start with ``kv@`` or has no model part.
    """
    if not topic.startswith(TOPIC_PREFIX):
        raise ValueError(f"not a KV-events topic: {topic!r}")
    parts = topic[len(TOPIC_PREFIX) :].split("@", 1)
    if len(parts) != 2 or not parts[0] or not parts[1]:
        raise ValueError(f"malformed KV-events topic: {topic!r}")
    return parts[0], parts[1]


def with_kv_events(deployment: Obj, port: int = KV_EVENTS_PORT) -> Obj:
    """A copy of a simulator Deployment that publishes KV events for subscribers to dial.

    Add ``--zmq-endpoint tcp://*:<port>`` (``*`` makes the simulator bind,
    so each EPP connects to each Pod) and ``--event-batch-size 1`` (publish
    every event at once; the default batches for up to a second) to the first
    container's args, and expose the port as ``kv-events``. Do not modify the
    input.
    """
    out = copy.deepcopy(deployment)
    container = out["spec"]["template"]["spec"]["containers"][0]
    container["args"] = [
        *container.get("args", []),
        "--zmq-endpoint",
        f"tcp://*:{port}",
        "--event-batch-size",
        "1",
    ]
    container.setdefault("ports", []).append(
        {"name": "kv-events", "containerPort": port, "protocol": "TCP"}
    )
    return out
