"""Python client for the dq TCP server. One connection per Client; thread-safe."""

from __future__ import annotations

import json
import socket
import threading
from typing import Any

from .errors import DQError, ERRORS_BY_CODE


class Client:
    """Blocking client. Server-side errors are raised as the matching ``DQError`` subclass."""

    def __init__(self, host: str = "127.0.0.1", port: int = 7878, timeout: float = 10.0) -> None:
        self._sock = socket.create_connection((host, port), timeout=timeout)
        self._sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self._file = self._sock.makefile("rb")
        self._lock = threading.Lock()

    def __enter__(self) -> "Client":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def close(self) -> None:
        """Close the connection."""
        self._file.close()
        self._sock.close()

    def _call(self, op: str, **fields: Any) -> Any:
        """Send one request line and return the result, or raise the matching DQError."""
        payload = json.dumps({"op": op, **fields}, separators=(",", ":")).encode("utf-8") + b"\n"
        with self._lock:
            self._sock.sendall(payload)
            line = self._file.readline()
        if not line:
            raise ConnectionError("server closed the connection")
        resp = json.loads(line)
        if resp["ok"]:
            return resp["result"]
        err = resp["error"]
        raise ERRORS_BY_CODE.get(err["code"], DQError)(err["message"])

    def ping(self) -> str:
        """Check the server is alive; returns 'pong'."""
        return self._call("ping")

    def create_queue(self, queue: str, **config: Any) -> bool:
        """Create a queue (idempotent for an identical configuration)."""
        return self._call("create_queue", queue=queue, config=config)

    def send(self, queue: str, body: str, *, group: str | None = None, delay: float | None = None, dedup_id: str | None = None) -> dict[str, Any]:
        """Send one message; returns its id and whether it was a duplicate."""
        return self._call("send", queue=queue, body=body, group=group, delay=delay, dedup_id=dedup_id)

    def receive(self, queue: str, max_messages: int = 1, visibility_timeout: float | None = None) -> list[dict[str, Any]]:
        """Receive up to ``max_messages``; each becomes invisible until acked or timed out."""
        return self._call("receive", queue=queue, max=max_messages, visibility_timeout=visibility_timeout)

    def ack(self, queue: str, handle: str) -> dict[str, Any]:
        """Delete a received message using its receipt handle."""
        return self._call("ack", queue=queue, handle=handle)

    def nack(self, queue: str, handle: str, delay: float | None = None) -> dict[str, Any]:
        """Return a message for retry, with backoff unless ``delay`` is given."""
        return self._call("nack", queue=queue, handle=handle, delay=delay)

    def change_visibility(self, queue: str, handle: str, seconds: float) -> dict[str, Any]:
        """Extend or shorten how long a received message stays hidden."""
        return self._call("change_visibility", queue=queue, handle=handle, seconds=seconds)

    def stats(self, queue: str) -> dict[str, Any]:
        """Queue counts, totalled and per node."""
        return self._call("stats", queue=queue)

    def peek(self, queue: str, limit: int = 100) -> list[dict[str, Any]]:
        """List messages without receiving them."""
        return self._call("peek", queue=queue, limit=limit)

    def redrive(self, queue: str, max_messages: int = 1000) -> int:
        """Move dead-lettered messages back to their source queue."""
        return self._call("redrive", queue=queue, max=max_messages)

    def snapshot(self) -> dict[str, int]:
        """Snapshot and compact every node's log."""
        return self._call("snapshot")

    def topology(self) -> dict[str, Any]:
        """Describe nodes, replicas and failover counts."""
        return self._call("topology")

    def add_node(self, node: str) -> dict[str, Any]:
        """Add a node and rebalance; returns how many messages moved."""
        return self._call("add_node", node=node)

    def remove_node(self, node: str) -> dict[str, Any]:
        """Drain a node into the others and remove it."""
        return self._call("remove_node", node=node)
