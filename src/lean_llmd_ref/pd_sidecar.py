"""Day 9 - the P/D sidecar: runs in each decode Pod, in front of its model server.

Shaped like llm-d-router's ``pkg/sidecar/proxy`` (the ``nixlv2`` connector).
A request carrying ``x-prefiller-host-port`` is first sent to that prefill
server with ``max_tokens: 1`` and ``do_remote_decode``; its answer's
``kv_transfer_params`` tell the local decode server where to pull the KV
cache from. Everything else is proxied to the local server unchanged.

    python -m lean_llmd_ref.pd_sidecar --port 8000 --vllm-port 8200
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
    out = dict(body)
    out["max_tokens"] = 1
    if "max_completion_tokens" in out:
        out["max_completion_tokens"] = 1
    out["stream"] = False
    out.pop("stream_options", None)
    out["kv_transfer_params"] = {
        "do_remote_decode": True,
        "do_remote_prefill": False,
        "remote_engine_id": None,
        "remote_block_ids": None,
        "remote_host": None,
        "remote_port": None,
    }
    return out


def decode_body(body: dict[str, Any], prefill_response: dict[str, Any]) -> dict[str, Any]:
    """The original request plus the prefill server's ``kv_transfer_params`` (if any)."""
    out = dict(body)
    params = prefill_response.get("kv_transfer_params")
    if params is not None:
        out["kv_transfer_params"] = params
    return out


class SidecarHandler(BaseHTTPRequestHandler):
    """One HTTP request. ``server.vllm`` is the local model server's base URL."""

    protocol_version = "HTTP/1.1"
    server: "SidecarServer"

    def log_message(self, format: str, *args: Any) -> None:
        pass

    def do_GET(self) -> None:  # noqa: N802 - http.server's naming
        self.proxy(None)

    def do_POST(self) -> None:  # noqa: N802
        length = int(self.headers.get("content-length") or 0)
        self.proxy(self.rfile.read(length))

    def proxy(self, raw: bytes | None) -> None:
        """Run P/D if this is a completion with a prefill header; else pass it through.

        P/D: POST ``prefill_body`` to ``http://<prefiller><path>``. A non-200
        answer goes straight back to the client. Otherwise send
        ``decode_body`` to the local server and stream its answer back. The
        prefill header is never forwarded.
        """
        prefiller = self.headers.get(PREFILL_ENDPOINT_HEADER)
        path = self.path.split("?", 1)[0]
        if raw is not None and prefiller and path in COMPLETION_PATHS:
            body = json.loads(raw)
            prefill = self.server.client.post(
                f"http://{prefiller}{self.path}", json=prefill_body(body)
            )
            if prefill.status_code != 200:
                self.send_buffered(prefill.status_code, prefill.headers, prefill.content)
                return
            raw = json.dumps(decode_body(body, prefill.json())).encode()
        self.forward(raw)

    def forward(self, raw: bytes | None) -> None:
        """Send the request to the local server and stream the response back chunked."""
        headers = {
            k: v
            for k, v in self.headers.items()
            if k.lower() not in HOP_BY_HOP and k.lower() != PREFILL_ENDPOINT_HEADER
        }
        method = "GET" if raw is None else "POST"
        url = f"{self.server.vllm}{self.path}"
        with self.server.client.stream(method, url, content=raw, headers=headers) as upstream:
            self.send_response(upstream.status_code)
            for k, v in upstream.headers.items():
                if k.lower() not in HOP_BY_HOP:
                    self.send_header(k, v)
            self.send_header("Transfer-Encoding", "chunked")
            self.end_headers()
            for chunk in upstream.iter_raw():
                if chunk:
                    self.wfile.write(f"{len(chunk):x}\r\n".encode() + chunk + b"\r\n")
                    self.wfile.flush()
            self.wfile.write(b"0\r\n\r\n")

    def send_buffered(self, status: int, headers: httpx.Headers, content: bytes) -> None:
        self.send_response(status)
        for k, v in headers.items():
            if k.lower() not in HOP_BY_HOP:
                self.send_header(k, v)
        self.send_header("Content-Length", str(len(content)))
        self.end_headers()
        self.wfile.write(content)


class SidecarServer(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, port: int, vllm: str) -> None:
        super().__init__(("0.0.0.0", port), SidecarHandler)
        self.vllm = vllm
        self.client = httpx.Client(timeout=300.0)


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="pd_sidecar", description=__doc__)
    p.add_argument("--port", type=int, default=8000)
    p.add_argument("--vllm-host", default="127.0.0.1")
    p.add_argument("--vllm-port", type=int, default=8200)
    a = p.parse_args(argv)
    server = SidecarServer(a.port, f"http://{a.vllm_host}:{a.vllm_port}")
    print(f"pd-sidecar: :{a.port} -> {server.vllm}", flush=True)
    server.serve_forever()
    return 0


if __name__ == "__main__":
    sys.exit(main())
