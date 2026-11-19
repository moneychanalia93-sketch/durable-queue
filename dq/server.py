"""TCP server: one JSON object per line in, one JSON object per line out.

Request : ``{"op": "send", "queue": "orders", "body": "hi"}``
Success : ``{"ok": true, "result": ...}``
Failure : ``{"ok": false, "error": {"code": "invalid_receipt", "message": "..."}}``

The server only binds to loopback. There is no authentication or TLS, so exposing
it on a real network interface would be unsafe; that is refused, not just advised.
"""

from __future__ import annotations

import json
import socketserver
import sys
import threading
from typing import Any, Callable

from .cluster import Cluster
from .errors import DQError, ValidationError

MAX_LINE_BYTES = 1024 * 1024
LOOPBACK_HOSTS = ("127.0.0.1", "localhost")


def _need(req: dict[str, Any], field: str) -> Any:
    """Fetch a required request field or raise ValidationError."""
    if field not in req:
        raise ValidationError(f"missing field {field!r}")
    return req[field]


def _build_ops(c: Cluster) -> dict[str, Callable[[dict[str, Any]], Any]]:
    """Map wire operation names to cluster calls."""
    return {
        "ping": lambda r: "pong",
        "create_queue": lambda r: c.create_queue(_need(r, "queue"), **r.get("config", {})),
        "send": lambda r: c.send(
            _need(r, "queue"), _need(r, "body"), group=r.get("group"), delay=r.get("delay"), dedup_id=r.get("dedup_id")
        ),
        "receive": lambda r: c.receive(_need(r, "queue"), r.get("max", 1), r.get("visibility_timeout")),
        "ack": lambda r: c.ack(_need(r, "queue"), _need(r, "handle")),
        "nack": lambda r: c.nack(_need(r, "queue"), _need(r, "handle"), r.get("delay")),
        "change_visibility": lambda r: c.change_visibility(_need(r, "queue"), _need(r, "handle"), _need(r, "seconds")),
        "stats": lambda r: c.stats(_need(r, "queue")),
        "peek": lambda r: c.peek(_need(r, "queue"), r.get("limit", 100)),
        "redrive": lambda r: c.redrive(_need(r, "queue"), r.get("max", 1000)),
        "snapshot": lambda r: c.snapshot(),
        "topology": lambda r: c.topology(),
        "add_node": lambda r: c.add_node(_need(r, "node")),
        "remove_node": lambda r: c.remove_node(_need(r, "node")),
    }


class _Handler(socketserver.StreamRequestHandler):
    disable_nagle_algorithm = True  # small request/response pairs: do not wait to batch them

    def handle(self) -> None:
        """Serve one connection: read request lines, write response lines."""
        server: DQServer = self.server  # type: ignore[assignment]
        while True:
            line = self.rfile.readline(MAX_LINE_BYTES + 1)
            if not line:
                return
            if len(line) > MAX_LINE_BYTES:
                peer_closed = False
                if not line.endswith(b"\n"):  # discard the rest of the line so the stream stays in sync
                    while True:
                        rest = self.rfile.readline(MAX_LINE_BYTES)
                        if not rest:
                            peer_closed = True
                        if not rest or rest.endswith(b"\n"):
                            break
                self._send(_error("validation", f"request exceeds {MAX_LINE_BYTES} bytes"))
                if peer_closed:
                    return
                continue
            self._send(server.dispatch(line))

    def _send(self, resp: dict[str, Any]) -> None:
        """Write one JSON response line."""
        self.wfile.write(json.dumps(resp, separators=(",", ":")).encode("utf-8") + b"\n")


def _error(code: str, message: str) -> dict[str, Any]:
    """Build an error response."""
    return {"ok": False, "error": {"code": code, "message": message}}


class DQServer(socketserver.ThreadingTCPServer):
    """A thread-per-connection server in front of a Cluster."""

    allow_reuse_address = True
    daemon_threads = True

    def __init__(self, cluster: Cluster, host: str = "127.0.0.1", port: int = 0) -> None:
        if host not in LOOPBACK_HOSTS:
            raise ValueError(f"refusing to bind {host!r}: this server has no auth/TLS and is loopback-only")
        super().__init__((host, port), _Handler)
        self.cluster = cluster
        self._ops = _build_ops(cluster)
        self._thread: threading.Thread | None = None

    @property
    def address(self) -> tuple[str, int]:
        """The (host, port) actually bound (useful when port 0 was requested)."""
        return self.server_address[0], self.server_address[1]  # type: ignore[return-value]

    def dispatch(self, line: bytes) -> dict[str, Any]:
        """Parse one request line and run it. Never raises."""
        try:
            req = json.loads(line)
            if not isinstance(req, dict):
                raise ValidationError("request must be a JSON object")
            fn = self._ops.get(req.get("op"))
            if fn is None:
                raise ValidationError(f"unknown op {req.get('op')!r}")
            return {"ok": True, "result": fn(req)}
        except DQError as exc:
            return _error(exc.code, str(exc))
        except (ValueError, TypeError) as exc:  # bad JSON or wrong argument types
            return _error("validation", f"bad request: {exc}")
        except Exception as exc:  # a server bug must not kill the connection thread
            print(f"internal error: {exc!r}", file=sys.stderr)
            return _error("internal", "internal error")

    def start(self) -> "DQServer":
        """Serve in a background thread."""
        self._thread = threading.Thread(target=self.serve_forever, name="dq-server", daemon=True)
        self._thread.start()
        return self

    def stop(self) -> None:
        """Stop serving and release the port."""
        self.shutdown()
        self.server_close()
        if self._thread is not None:
            self._thread.join(timeout=5)
