"""Day 9 - the P/D sidecar in each decode Pod.

Your implementation. Every function below raises until you write it.

    uv run lean-llmd test --day 9         # your code
    uv run lean-llmd test --day 9 --ref   # proves the tests are passable
    uv run lean-llmd diff --day 9         # only after you have a passing run
"""

import argparse
import json
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

import httpx

PREFILL_ENDPOINT_HEADER = "x-prefiller-host-port"
COMPLETION_PATHS = ("/v1/completions", "/v1/chat/completions")
HOP_BY_HOP = {"connection", "keep-alive", "transfer-encoding", "content-length", "host"}


def prefill_body(body: dict[str, Any]) -> dict[str, Any]:
    """The request the prefill server gets: same prompt, one token, no streaming.

    Copy ``body``; set ``max_tokens`` (and ``max_completion_tokens`` if
    present) to 1, ``stream`` to false, drop ``stream_options``, and set
    ``kv_transfer_params`` to ``{"do_remote_decode": True, "do_remote_prefill":
    False, "remote_engine_id": None, "remote_block_ids": None, "remote_host":
    None, "remote_port": None}``.
    """
    raise NotImplementedError("day 9: implement prefill_body()")


def decode_body(body: dict[str, Any], prefill_response: dict[str, Any]) -> dict[str, Any]:
    """The original request plus the prefill server's ``kv_transfer_params`` (if any)."""
    raise NotImplementedError("day 9: implement decode_body()")


class SidecarHandler(BaseHTTPRequestHandler):
    """One HTTP request. ``server.vllm`` is the local model server's base URL."""

    protocol_version = "HTTP/1.1"
    server: "SidecarServer"

    def log_message(self, format: str, *args: Any) -> None:
        raise NotImplementedError("day 9: implement log_message()")

    def do_GET(self) -> None:
        raise NotImplementedError("day 9: implement do_GET()")

    def do_POST(self) -> None:
        raise NotImplementedError("day 9: implement do_POST()")

    def proxy(self, raw: bytes | None) -> None:
        """Run P/D if this is a completion with a prefill header; else pass it through.

        P/D: POST ``prefill_body`` to ``http://<prefiller><path>``. A non-200
        answer goes straight back to the client. Otherwise send
        ``decode_body`` to the local server and stream its answer back. The
        prefill header is never forwarded.
        """
        raise NotImplementedError("day 9: implement proxy()")

    def forward(self, raw: bytes | None) -> None:
        """Send the request to the local server and stream the response back chunked."""
        raise NotImplementedError("day 9: implement forward()")

    def send_buffered(self, status: int, headers: httpx.Headers, content: bytes) -> None:
        raise NotImplementedError("day 9: implement send_buffered()")


class SidecarServer(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, port: int, vllm: str) -> None:
        raise NotImplementedError("day 9: implement __init__()")


def main(argv: list[str] | None = None) -> int:
    raise NotImplementedError("day 9: implement main()")


if __name__ == "__main__":
    sys.exit(main())
