"""A durable single-node broker: engine + write-ahead log + snapshots + recovery.

Write path (all under one lock, so commands are applied in a single total order)::

    stamp time -> engine.execute(command) -> if state changed: append to WAL
                -> ship to replicas -> maybe snapshot -> return result

A command is applied in memory first and logged second. That is safe because the
caller only hears the result after the log write returns; if the process dies in
between, the caller never saw a success and will retry (at-least-once).

If the log write fails the broker becomes fail-stop: memory is then ahead of disk,
so it refuses all further work instead of pretending.
"""

from __future__ import annotations

import hashlib
import json
import os
import random
import threading
from dataclasses import asdict
from pathlib import Path
from typing import Any

from .clock import Clock, SystemClock
from .engine import Engine, QueueConfig
from .errors import (
    BrokerFailed,
    CorruptSnapshot,
    DQError,
    RecoveryError,
    ReplicationGap,
    ValidationError,
)
from .wal import ScanReport, WriteAheadLog, fsync_dir

SNAPSHOT_NAME = "snapshot.json"
WAL_NAME = "wal.log"


def write_snapshot_file(path: Path, snap: dict[str, Any]) -> None:
    """Atomically write ``snap``: temp file, fsync, rename. First line is a SHA-256."""
    payload = json.dumps(snap, separators=(",", ":")).encode("utf-8")
    tmp = path.with_suffix(".tmp")
    with open(tmp, "wb") as fh:
        fh.write(hashlib.sha256(payload).hexdigest().encode("ascii") + b"\n" + payload)
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp, path)
    fsync_dir(path.parent)


def read_snapshot_file(path: Path) -> dict[str, Any] | None:
    """Load and verify a snapshot. Returns None if absent; raises if it is damaged."""
    if not path.exists():
        return None
    digest, _, payload = path.read_bytes().partition(b"\n")
    if hashlib.sha256(payload).hexdigest().encode("ascii") != digest:
        raise CorruptSnapshot(f"{path} fails its checksum; refusing to guess at state")
    return json.loads(payload)


class Broker:
    """One node's queues, with optional on-disk durability (``path=None`` is memory only)."""

    def __init__(
        self,
        path: str | os.PathLike[str] | None = None,
        *,
        clock: Clock | None = None,
        fsync: str = "batch",
        batch_size: int = 64,
        batch_interval: float = 0.05,
        snapshot_every: int = 50_000,
        rng: random.Random | None = None,
    ) -> None:
        self._clock = clock or SystemClock()
        self._rng = rng or random.Random()
        self._lock = threading.RLock()
        self._snapshot_every = snapshot_every
        self._since_snapshot = 0
        self._failed = False
        self._replicas: list[Broker] = []
        self.replication_errors: list[str] = []
        self._dir = Path(path) if path is not None else None
        self._wal: WriteAheadLog | None = None
        self.recovery = ScanReport()
        self.replayed = 0
        self._engine = Engine()
        self._lsn = 0
        if self._dir is not None:
            self._dir.mkdir(parents=True, exist_ok=True)
            try:
                self._recover(fsync, batch_size, batch_interval)
            except Exception:
                if self._wal is not None:
                    self._wal.close(sync=False)  # do not leak the open file when recovery is refused
                raise

    # ---------------------------------------------------------------- recovery
    def _recover(self, fsync: str, batch_size: int, batch_interval: float) -> None:
        """Rebuild state: load the snapshot, then replay log records newer than it."""
        assert self._dir is not None
        snap = read_snapshot_file(self._dir / SNAPSHOT_NAME)
        if snap is not None:
            self._engine = Engine.from_snapshot(snap)
            self._lsn = snap["lsn"]
        self._wal, records, self.recovery = WriteAheadLog.open(self._dir / WAL_NAME, fsync, batch_size, batch_interval)
        for rec in records:
            try:
                lsn, op = rec["lsn"], rec["op"]
            except (KeyError, TypeError) as exc:
                raise RecoveryError(f"malformed log record: {rec!r}") from exc
            if lsn <= self._lsn:
                continue  # already covered by the snapshot (crash between snapshot and log reset)
            if lsn != self._lsn + 1:
                raise RecoveryError(f"log gap: expected lsn {self._lsn + 1}, found {lsn}")
            try:
                self._engine.execute(op)
            except DQError as exc:
                raise RecoveryError(f"cannot replay lsn {lsn}: {exc}") from exc
            self._lsn = lsn
            self.replayed += 1

    # --------------------------------------------------------------- internals
    @property
    def lsn(self) -> int:
        """Sequence number of the last applied command."""
        return self._lsn

    @property
    def failed(self) -> bool:
        """True once the broker has crashed, hit an I/O error or been closed."""
        return self._failed

    def _check_alive(self) -> None:
        """Raise BrokerFailed if the broker is fail-stop."""
        if self._failed:
            raise BrokerFailed("broker is fail-stop (crashed or hit an I/O error)")

    def _run(self, op: dict[str, Any]) -> Any:
        """Apply one command, log it if it changed state, replicate it."""
        with self._lock:
            self._check_alive()
            op["now"] = self._clock.now()  # stamped inside the lock so log order == time order
            try:
                result, mutated = self._engine.execute(op)
            except DQError:
                raise
            except Exception:
                self._failed = True  # a bug may have left state half-changed
                raise
            if mutated:
                self._append({"lsn": self._lsn + 1, "op": op})
            return result

    def _append(self, rec: dict[str, Any]) -> None:
        """Log a record, ship it to replicas, and snapshot when due. Caller holds the lock."""
        try:
            if self._wal is not None:
                self._wal.append(rec)
        except OSError as exc:
            self._failed = True
            raise BrokerFailed(f"write-ahead log append failed: {exc}") from exc
        self._lsn = rec["lsn"]
        for replica in list(self._replicas):
            try:
                replica.apply_replicated(rec)
            except Exception as exc:  # a broken follower must not take the leader down
                self._replicas.remove(replica)
                self.replication_errors.append(f"dropped replica: {exc}")
        self._since_snapshot += 1
        if self._wal is not None and self._snapshot_every and self._since_snapshot >= self._snapshot_every:
            self.snapshot()

    # -------------------------------------------------------------- public API
    def create_queue(self, name: str, **config: Any) -> bool:
        """Create a queue. Returns False if it already existed with the same config."""
        cfg = QueueConfig(name=name, **config)
        cfg.validate()
        return self._run({"op": "create", "cfg": asdict(cfg)})["created"]

    def list_queues(self) -> list[dict[str, Any]]:
        """Configuration of every queue, in creation order."""
        with self._lock:
            return [asdict(q.cfg) for q in self._engine.queues.values()]

    def send(
        self,
        queue: str,
        body: str,
        *,
        group: str | None = None,
        delay: float | None = None,
        dedup_id: str | None = None,
        message_id: str | None = None,
    ) -> dict[str, Any]:
        """Enqueue one message. Returns ``{"id", "duplicate"}``."""
        mid = message_id or f"{self._rng.getrandbits(96):024x}"
        return self._run({"op": "send", "q": queue, "body": body, "group": group, "delay": delay, "dedup": dedup_id, "id": mid})

    def receive(self, queue: str, max_messages: int = 1, visibility_timeout: float | None = None) -> list[dict[str, Any]]:
        """Receive up to ``max_messages``; each becomes invisible until acked or timed out."""
        return self._run({"op": "recv", "q": queue, "max": max_messages, "vt": visibility_timeout})

    def ack(self, queue: str, handle: str) -> dict[str, Any]:
        """Delete a message. Raises InvalidReceipt if the handle is stale."""
        return self._run({"op": "ack", "q": queue, "handle": handle})

    def nack(self, queue: str, handle: str, delay: float | None = None) -> dict[str, Any]:
        """Give a message back. Without ``delay`` the queue's backoff-with-jitter applies."""
        with self._lock:
            u = self._rng.random()
            return self._run({"op": "nack", "q": queue, "handle": handle, "delay": delay, "u": u})

    def change_visibility(self, queue: str, handle: str, seconds: float) -> dict[str, Any]:
        """Extend (or shorten) the time a received message stays hidden."""
        return self._run({"op": "extend", "q": queue, "handle": handle, "vt": seconds})

    def redrive(self, queue: str, max_messages: int = 1000) -> int:
        """Move messages from a dead-letter queue back to their source queue."""
        return self._run({"op": "redrive", "q": queue, "max": max_messages})["moved"]

    def stats(self, queue: str) -> dict[str, Any]:
        """Message counts for one queue (visible / in flight / delayed / blocked)."""
        with self._lock:
            self._check_alive()
            return self._engine.stats(queue, self._clock.now())

    def peek(self, queue: str, limit: int = 100) -> list[dict[str, Any]]:
        """List messages oldest-first without receiving them (used to inspect a DLQ)."""
        with self._lock:
            self._check_alive()
            if not 1 <= limit <= 10_000:
                raise ValidationError("limit must be in 1..10000")
            return self._engine.peek(queue, limit)

    # ------------------------------------------------------- rebalancing hooks
    def dump_messages(self, queue: str) -> list[dict[str, Any]]:
        """Full copies of every message in a queue (rebalancing, step 1)."""
        with self._lock:
            self._check_alive()
            return self._engine.dump(queue)

    def import_messages(self, queue: str, msgs: list[dict[str, Any]]) -> int:
        """Adopt messages handed over from another node (rebalancing, step 2)."""
        return self._run({"op": "import", "q": queue, "msgs": msgs})["imported"]

    def delete_ids(self, queue: str, ids: list[str]) -> int:
        """Remove messages by id after they were copied elsewhere (rebalancing, step 3)."""
        return self._run({"op": "delete_ids", "q": queue, "ids": ids})["removed"]

    # ------------------------------------------------- snapshots & replication
    def snapshot(self) -> int:
        """Write a snapshot and truncate the log (compaction). Returns the snapshot LSN."""
        with self._lock:
            self._check_alive()
            if self._dir is None or self._wal is None:
                return self._lsn
            write_snapshot_file(self._dir / SNAPSHOT_NAME, {"lsn": self._lsn, **self._engine.to_snapshot()})
            # Crash here is safe: recovery skips log records with lsn <= snapshot lsn.
            self._wal.reset()
            self._since_snapshot = 0
            return self._lsn

    def export_state(self) -> dict[str, Any]:
        """Canonical copy of all state plus the LSN (used to seed followers and in tests)."""
        with self._lock:
            return {"lsn": self._lsn, **self._engine.to_snapshot()}

    def install_state(self, state: dict[str, Any]) -> None:
        """Replace this broker's state with ``state`` (follower bootstrap)."""
        with self._lock:
            self._engine = Engine.from_snapshot(state)
            self._lsn = state["lsn"]
            if self._dir is not None and self._wal is not None:
                write_snapshot_file(self._dir / SNAPSHOT_NAME, state)
                self._wal.reset()

    def add_replica(self, replica: "Broker") -> None:
        """Seed ``replica`` from this broker and ship every later command to it (synchronously)."""
        with self._lock:
            self._check_alive()
            replica.install_state(self.export_state())
            self._replicas.append(replica)

    def apply_replicated(self, rec: dict[str, Any]) -> None:
        """Follower side: apply one leader command with the leader's LSN."""
        with self._lock:
            self._check_alive()
            if rec["lsn"] <= self._lsn:
                return  # duplicate delivery; already applied
            if rec["lsn"] != self._lsn + 1:
                raise ReplicationGap(f"expected lsn {self._lsn + 1}, got {rec['lsn']}")
            self._engine.execute(rec["op"])
            self._append(rec)

    # ---------------------------------------------------------------- lifecycle
    def crash(self) -> None:
        """Simulate a hard crash: stop serving, close files without a final fsync."""
        with self._lock:
            self._failed = True
            if self._wal is not None:
                self._wal.close(sync=False)

    def close(self) -> None:
        """Clean shutdown: fsync the log and close it."""
        with self._lock:
            if self._wal is not None:
                self._wal.close(sync=not self._failed)
            self._failed = True
