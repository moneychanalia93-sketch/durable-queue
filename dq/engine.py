"""The queue state machine: pure, deterministic, no I/O, no clock, no randomness.

Every state change is a *command* (a plain dict) that already carries everything
that could differ between runs: the current time ``now``, message ids, and the
random jitter fraction ``u``. Running the same commands in the same order on an
empty engine always yields the same state. That one property gives us:

  * crash recovery  = replay the write-ahead log,
  * replication     = ship the same commands to a follower,
  * model testing   = compare against a naive reference implementation.

Message lifecycle::

    send -> [waiting until avail_at] -> receive -> [in flight until avail_at]
         -> ack (deleted)  |  nack / timeout (back to waiting)
         -> received max_receive times -> dead-letter queue

Both "waiting" and "in flight" are the same thing to the scheduler: a message with
an ``avail_at`` time. A min-heap on ``(avail_at, seq)`` finds the next deliverable
message. Heap entries are never edited; a changed message gets a new ``ver`` and a
fresh entry, and stale entries are skipped when popped ("lazy deletion").
"""

from __future__ import annotations

import heapq
import re
from collections import deque
from dataclasses import asdict, dataclass, field
from typing import Any

from .errors import (
    InvalidReceipt,
    QueueConflict,
    QueueFull,
    QueueNotFound,
    ValidationError,
)

MAX_BODY_BYTES = 256 * 1024
MAX_TIMEOUT = 43_200.0  # 12 hours, same cap as SQS visibility timeout
MAX_RECEIVE_BATCH = 1000
_NAME_RE = re.compile(r"^[A-Za-z0-9_.-]{1,80}$")


@dataclass
class QueueConfig:
    """Per-queue settings. ``max_receive=0`` means never dead-letter."""

    name: str
    fifo: bool = False
    visibility_timeout: float = 30.0
    delay: float = 0.0
    max_receive: int = 0
    dlq: str | None = None
    dedup_window: float = 300.0
    backoff_base: float = 1.0
    backoff_cap: float = 60.0
    max_depth: int = 0  # 0 means unlimited; otherwise send is refused at this size

    def validate(self) -> None:
        """Raise ValidationError if any setting is out of range."""
        if not _NAME_RE.match(self.name):
            raise ValidationError("queue name must match [A-Za-z0-9_.-]{1,80}")
        for label, value in (("visibility_timeout", self.visibility_timeout), ("delay", self.delay), ("dedup_window", self.dedup_window)):
            if not 0 <= value <= MAX_TIMEOUT:
                raise ValidationError(f"{label} must be between 0 and {MAX_TIMEOUT}")
        if self.visibility_timeout <= 0:
            raise ValidationError("visibility_timeout must be > 0")
        if self.max_receive < 0 or self.max_depth < 0:
            raise ValidationError("max_receive and max_depth must be >= 0")
        if self.max_receive and not self.dlq:
            raise ValidationError("max_receive requires a dlq")
        if self.dlq == self.name:
            raise ValidationError("a queue cannot be its own dead-letter queue")
        if self.backoff_base <= 0 or self.backoff_cap < self.backoff_base:
            raise ValidationError("need 0 < backoff_base <= backoff_cap")


@dataclass
class Message:
    """One message and its delivery state."""
    id: str
    body: str
    group: str | None
    seq: int  # arrival order within this queue; also the heap tie-breaker
    sent_at: float
    avail_at: float  # when the message may next be delivered
    receive_count: int = 0
    handle: str | None = None  # current receipt handle; None when nobody holds it
    inflight: bool = False
    ver: int = 0
    source: str | None = None  # set while the message sits in a dead-letter queue


@dataclass
class _Queue:
    """A queue's configuration plus its scheduling structures."""
    cfg: QueueConfig
    msgs: dict[str, Message] = field(default_factory=dict)
    heap: list[tuple[float, int, str, int]] = field(default_factory=list)
    groups: dict[str, deque[str]] = field(default_factory=dict)  # FIFO queues only
    next_seq: int = 0
    delivery_ctr: int = 0
    dedup: dict[str, tuple[str, float]] = field(default_factory=dict)  # dedup id -> (msg id, expires)
    dedup_heap: list[tuple[float, str]] = field(default_factory=list)


def _msg_view(m: Message) -> dict[str, Any]:
    """The part of a message that is shown to consumers."""
    return {
        "id": m.id,
        "body": m.body,
        "group": m.group,
        "handle": m.handle,
        "receive_count": m.receive_count,
        "sent_at": m.sent_at,
    }


class Engine:
    """All queues of one broker. Not thread-safe; the broker serialises access."""

    def __init__(self) -> None:
        self.queues: dict[str, _Queue] = {}

    # ------------------------------------------------------------------ commands
    def execute(self, op: dict[str, Any]) -> tuple[Any, bool]:
        """Run one command. Returns ``(result, mutated)``.

        ``mutated`` tells the broker whether the command must be logged. A command
        that raises must not have changed any state (validation comes first).
        """
        handler = _HANDLERS.get(op.get("op"))
        if handler is None:
            raise ValidationError(f"unknown op {op.get('op')!r}")
        return handler(self, op)

    def _q(self, name: Any) -> _Queue:
        """Look up a queue or raise QueueNotFound."""
        q = self.queues.get(name) if isinstance(name, str) else None
        if q is None:
            raise QueueNotFound(f"no such queue: {name!r}")
        return q

    # -- scheduling helpers --------------------------------------------------
    @staticmethod
    def _push(q: _Queue, m: Message) -> None:
        """Add a heap entry for the message's current ``avail_at`` and version."""
        heapq.heappush(q.heap, (m.avail_at, m.seq, m.id, m.ver))

    def _enqueue(self, q: _Queue, m: Message) -> None:
        """Add a message to a queue, assigning its arrival number."""
        m.seq = q.next_seq
        q.next_seq += 1
        q.msgs[m.id] = m
        if q.cfg.fifo:
            g = q.groups.setdefault(m.group or "", deque())
            g.append(m.id)
            if len(g) > 1:
                return  # only a group's head is schedulable
        self._push(q, m)

    def _remove(self, q: _Queue, m: Message) -> None:
        """Delete a message; in FIFO queues, expose the next one in its group."""
        del q.msgs[m.id]
        if not q.cfg.fifo:
            return
        key = m.group or ""
        g = q.groups[key]
        was_head = g[0] == m.id
        if was_head:
            g.popleft()
        else:
            g.remove(m.id)
        if not g:
            del q.groups[key]
        elif was_head:
            self._push(q, q.msgs[g[0]])  # the next message in the group is now the head

    @staticmethod
    def _pop_ready(q: _Queue, now: float) -> Message | None:
        """Pop the next valid message whose time has come, skipping stale entries."""
        while q.heap and q.heap[0][0] <= now:
            _, seq, mid, ver = heapq.heappop(q.heap)
            m = q.msgs.get(mid)
            if m is None or m.ver != ver or m.seq != seq:
                continue  # stale entry
            return m
        return None

    def _dead_letter(self, q: _Queue, m: Message, now: float) -> None:
        """Move a message to the queue's dead-letter queue."""
        dlq = self._q(q.cfg.dlq)
        self._remove(q, m)
        self._enqueue(
            dlq,
            Message(m.id, m.body, m.group, 0, m.sent_at, now, m.receive_count, None, False, 0, source=q.cfg.name),
        )

    # -- handlers ------------------------------------------------------------
    def _create(self, op: dict[str, Any]) -> tuple[Any, bool]:
        """Command: create a queue (idempotent for an identical configuration)."""
        try:
            cfg = QueueConfig(**op["cfg"])
        except TypeError as exc:
            raise ValidationError(f"bad queue config: {exc}") from exc
        cfg.validate()
        existing = self.queues.get(cfg.name)
        if existing is not None:
            if existing.cfg != cfg:
                raise QueueConflict(f"queue {cfg.name!r} exists with a different configuration")
            return {"created": False}, False
        if cfg.dlq and cfg.dlq not in self.queues:
            raise QueueNotFound(f"dead-letter queue {cfg.dlq!r} must be created first")
        self.queues[cfg.name] = _Queue(cfg)
        return {"created": True}, True

    def _send(self, op: dict[str, Any]) -> tuple[Any, bool]:
        """Command: add a message (after validation, dedup and depth checks)."""
        q = self._q(op.get("q"))
        cfg, now = q.cfg, op["now"]
        body, group, mid, dedup = op.get("body"), op.get("group"), op["id"], op.get("dedup")
        if not isinstance(body, str):
            raise ValidationError("body must be a string")
        if len(body.encode("utf-8")) > MAX_BODY_BYTES:
            raise ValidationError(f"body exceeds {MAX_BODY_BYTES} bytes")
        if group is not None and (not isinstance(group, str) or not 0 < len(group) <= 128):
            raise ValidationError("group must be a non-empty string up to 128 chars")
        if cfg.fifo and group is None:
            raise ValidationError("FIFO queues require a group")
        if dedup is not None and (not isinstance(dedup, str) or not 0 < len(dedup) <= 128):
            raise ValidationError("dedup id must be a non-empty string up to 128 chars")
        delay = cfg.delay if op.get("delay") is None else op["delay"]
        if not isinstance(delay, (int, float)) or not 0 <= delay <= MAX_TIMEOUT:
            raise ValidationError(f"delay must be between 0 and {MAX_TIMEOUT}")
        if dedup is not None:
            hit = q.dedup.get(dedup)
            if hit is not None and hit[1] > now:
                return {"id": hit[0], "duplicate": True}, False
        if cfg.max_depth and len(q.msgs) >= cfg.max_depth:
            raise QueueFull(f"queue {cfg.name!r} is at max_depth={cfg.max_depth}")
        if mid in q.msgs:
            raise ValidationError(f"message id {mid!r} already exists")
        self._enqueue(q, Message(mid, body, group, 0, now, now + delay))
        if dedup is not None:
            q.dedup[dedup] = (mid, now + cfg.dedup_window)
            heapq.heappush(q.dedup_heap, (now + cfg.dedup_window, dedup))
            self._purge_dedup(q, now)
        return {"id": mid, "duplicate": False}, True

    @staticmethod
    def _purge_dedup(q: _Queue, now: float) -> None:
        """Drop expired dedup entries. Cleanup only: lookups already check expiry."""
        while q.dedup_heap and q.dedup_heap[0][0] <= now:
            expires, key = heapq.heappop(q.dedup_heap)
            entry = q.dedup.get(key)
            if entry is not None and entry[1] == expires:
                del q.dedup[key]

    def _recv(self, op: dict[str, Any]) -> tuple[Any, bool]:
        """Command: deliver up to ``max`` messages, dead-lettering any that used up their receives."""
        q = self._q(op.get("q"))
        now, limit = op["now"], op.get("max", 1)
        vt = q.cfg.visibility_timeout if op.get("vt") is None else op["vt"]
        if not isinstance(limit, int) or not 1 <= limit <= MAX_RECEIVE_BATCH:
            raise ValidationError(f"max must be an integer in 1..{MAX_RECEIVE_BATCH}")
        if not isinstance(vt, (int, float)) or not 0.001 <= vt <= MAX_TIMEOUT:
            raise ValidationError(f"visibility timeout must be between 0.001 and {MAX_TIMEOUT}")
        out: list[dict[str, Any]] = []
        mutated = False
        while len(out) < limit:
            m = self._pop_ready(q, now)
            if m is None:
                break
            mutated = True
            if q.cfg.max_receive and m.receive_count >= q.cfg.max_receive:
                self._dead_letter(q, m, now)
                continue
            q.delivery_ctr += 1
            m.receive_count += 1
            m.handle = f"{m.id}.{q.delivery_ctr}"  # unique per delivery, never reused
            m.inflight = True
            m.avail_at = now + vt
            m.ver += 1
            self._push(q, m)
            out.append(_msg_view(m))
        return out, mutated

    def _owned(self, q: _Queue, handle: Any) -> Message:
        """Find the message a receipt handle refers to, or raise InvalidReceipt."""
        if not isinstance(handle, str) or "." not in handle:
            raise InvalidReceipt("malformed receipt handle")
        m = q.msgs.get(handle.rsplit(".", 1)[0])
        if m is None or m.handle != handle:
            raise InvalidReceipt("receipt handle is stale: message was deleted or redelivered")
        return m

    def _ack(self, op: dict[str, Any]) -> tuple[Any, bool]:
        """Command: delete a message if the handle is its current one."""
        q = self._q(op.get("q"))
        m = self._owned(q, op.get("handle"))
        self._remove(q, m)
        return {"acked": m.id}, True

    def _nack(self, op: dict[str, Any]) -> tuple[Any, bool]:
        """Command: hide a message for a backoff delay and kill its handle."""
        q = self._q(op.get("q"))
        m = self._owned(q, op.get("handle"))
        delay = op.get("delay")
        if delay is None:
            # Exponential backoff with full jitter: uniform(0, min(cap, base * 2^(n-1))).
            ceiling = min(q.cfg.backoff_cap, q.cfg.backoff_base * 2 ** min(m.receive_count - 1, 62))
            delay = op["u"] * ceiling
        elif not isinstance(delay, (int, float)) or not 0 <= delay <= MAX_TIMEOUT:
            raise ValidationError(f"delay must be between 0 and {MAX_TIMEOUT}")
        m.avail_at = op["now"] + delay
        m.inflight = False
        m.handle = None  # a nacked handle cannot be used again
        m.ver += 1
        self._push(q, m)
        return {"delay": delay}, True

    def _extend(self, op: dict[str, Any]) -> tuple[Any, bool]:
        """Command: change how long a held message stays hidden."""
        q = self._q(op.get("q"))
        m = self._owned(q, op.get("handle"))
        vt = op.get("vt")
        if not isinstance(vt, (int, float)) or not 0 <= vt <= MAX_TIMEOUT:
            raise ValidationError(f"visibility timeout must be between 0 and {MAX_TIMEOUT}")
        m.avail_at = op["now"] + vt
        m.ver += 1
        self._push(q, m)
        return {"visible_at": m.avail_at}, True

    def _redrive(self, op: dict[str, Any]) -> tuple[Any, bool]:
        """Move dead-lettered messages back to the queue they came from."""
        q = self._q(op.get("q"))
        now, limit = op["now"], op.get("max", MAX_RECEIVE_BATCH)
        moved = 0
        for m in sorted(q.msgs.values(), key=lambda x: x.seq):
            if moved >= limit:
                break
            if m.source is None or m.source not in self.queues or (m.inflight and m.avail_at > now):
                continue
            target = self.queues[m.source]
            self._remove(q, m)
            self._enqueue(target, Message(m.id, m.body, m.group, 0, m.sent_at, now))
            moved += 1
        return {"moved": moved}, moved > 0

    def _import(self, op: dict[str, Any]) -> tuple[Any, bool]:
        """Add messages handed over from another node (rebalancing)."""
        q = self._q(op.get("q"))
        now, imported = op["now"], 0
        for d in sorted(op["msgs"], key=lambda x: x["seq"]):
            if d["id"] in q.msgs:
                continue
            # Anything that was in flight becomes visible; its old handle is dead.
            avail = now if d["inflight"] else d["avail_at"]
            self._enqueue(
                q,
                Message(d["id"], d["body"], d["group"], 0, d["sent_at"], avail, d["receive_count"], source=d["source"]),
            )
            imported += 1
        return {"imported": imported}, imported > 0

    def _delete_ids(self, op: dict[str, Any]) -> tuple[Any, bool]:
        """Remove messages by id (the source side of a rebalance)."""
        q = self._q(op.get("q"))
        removed = 0
        for mid in op["ids"]:
            m = q.msgs.get(mid)
            if m is not None:
                self._remove(q, m)
                removed += 1
        return {"removed": removed}, removed > 0

    # ------------------------------------------------------------------- reads
    def stats(self, name: str, now: float) -> dict[str, Any]:
        """Counts at time ``now``. Scans the queue, so it is O(messages)."""
        q = self._q(name)
        visible = in_flight = delayed = blocked = 0
        for m in q.msgs.values():
            if q.cfg.fifo and q.groups[m.group or ""][0] != m.id:
                blocked += 1
            elif m.avail_at <= now:
                visible += 1
            elif m.inflight:
                in_flight += 1
            else:
                delayed += 1
        return {
            "queue": name,
            "total": len(q.msgs),
            "visible": visible,
            "in_flight": in_flight,
            "delayed": delayed,
            "blocked_by_group": blocked,
        }

    def peek(self, name: str, limit: int) -> list[dict[str, Any]]:
        """List messages in arrival order without receiving them (no state change)."""
        q = self._q(name)
        picked = sorted(q.msgs.values(), key=lambda m: m.seq)[:limit]
        return [{**_msg_view(m), "handle": None, "source": m.source} for m in picked]

    def dump(self, name: str) -> list[dict[str, Any]]:
        """Full copies of every message (used to hand messages to another node)."""
        return [asdict(m) for m in sorted(self._q(name).msgs.values(), key=lambda m: m.seq)]

    # --------------------------------------------------------------- snapshots
    def to_snapshot(self) -> dict[str, Any]:
        """Canonical, JSON-able copy of all state (sorted so equal states compare equal)."""
        out: dict[str, Any] = {}
        for name in sorted(self.queues):
            q = self.queues[name]
            out[name] = {
                "cfg": asdict(q.cfg),
                "next_seq": q.next_seq,
                "delivery_ctr": q.delivery_ctr,
                "messages": [asdict(m) for m in sorted(q.msgs.values(), key=lambda m: m.seq)],
                "dedup": sorted([k, v[0], v[1]] for k, v in q.dedup.items()),
            }
        return {"queues": out}

    @classmethod
    def from_snapshot(cls, snap: dict[str, Any]) -> "Engine":
        """Rebuild an engine (including heaps and FIFO groups) from ``to_snapshot``."""
        eng = cls()
        for name, qd in snap["queues"].items():
            q = _Queue(QueueConfig(**qd["cfg"]), next_seq=qd["next_seq"], delivery_ctr=qd["delivery_ctr"])
            for md in qd["messages"]:  # already sorted by seq
                m = Message(**md)
                q.msgs[m.id] = m
                if q.cfg.fifo:
                    q.groups.setdefault(m.group or "", deque()).append(m.id)
                else:
                    eng._push(q, m)
            for g in q.groups.values():
                eng._push(q, q.msgs[g[0]])
            for key, mid, expires in qd["dedup"]:
                q.dedup[key] = (mid, expires)
                heapq.heappush(q.dedup_heap, (expires, key))
            eng.queues[name] = q
        return eng


_HANDLERS = {
    "create": Engine._create,
    "send": Engine._send,
    "recv": Engine._recv,
    "ack": Engine._ack,
    "nack": Engine._nack,
    "extend": Engine._extend,
    "redrive": Engine._redrive,
    "import": Engine._import,
    "delete_ids": Engine._delete_ids,
}
