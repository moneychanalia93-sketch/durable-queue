"""A deliberately naive reference queue used to check the real engine.

It keeps one plain list per queue and scans it on every operation. It is slow and
obviously correct, which is the point: random operation sequences must produce the
same observable results on the model and on the optimised engine.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class RefMsg:
    id: str
    body: str
    group: str | None
    sent_at: float
    avail: float
    rc: int = 0
    handle: str | None = None
    inflight: bool = False
    source: str | None = None


class RefQueue:
    def __init__(self, cfg: dict) -> None:
        self.cfg = cfg
        self.msgs: list[RefMsg] = []  # kept in arrival order
        self.deliveries = 0

    def is_head(self, m: RefMsg) -> bool:
        if not self.cfg.get("fifo"):
            return True
        return next(x for x in self.msgs if x.group == m.group) is m


class RefBroker:
    def __init__(self) -> None:
        self.q: dict[str, RefQueue] = {}

    def create(self, name: str, **cfg) -> None:
        cfg.setdefault("visibility_timeout", 30.0)
        cfg.setdefault("delay", 0.0)
        self.q[name] = RefQueue(cfg)

    def send(self, name: str, body: str, mid: str, now: float, group=None, delay=None) -> str:
        q = self.q[name]
        if q.cfg.get("max_depth") and len(q.msgs) >= q.cfg["max_depth"]:
            return "QueueFull"
        d = q.cfg["delay"] if delay is None else delay
        q.msgs.append(RefMsg(mid, body, group, now, now + d))
        return "ok"

    def receive(self, name: str, now: float, limit: int, vt: float | None = None) -> list[tuple]:
        q = self.q[name]
        vt = q.cfg["visibility_timeout"] if vt is None else vt
        out: list[tuple] = []
        while len(out) < limit:
            ready = [m for m in q.msgs if q.is_head(m) and m.avail <= now]
            if not ready:
                break
            m = min(ready, key=lambda x: (x.avail, q.msgs.index(x)))
            if q.cfg.get("max_receive") and m.rc >= q.cfg["max_receive"]:
                q.msgs.remove(m)
                m.source, m.handle, m.inflight, m.avail = name, None, False, now
                self.q[q.cfg["dlq"]].msgs.append(m)
                continue
            q.deliveries += 1
            m.rc += 1
            m.handle = f"{m.id}.{q.deliveries}"
            m.inflight = True
            m.avail = now + vt
            out.append((m.id, m.body, m.handle, m.rc))
        return out

    def _find(self, name: str, handle) -> RefMsg | None:
        return next((m for m in self.q[name].msgs if handle is not None and m.handle == handle), None)

    def ack(self, name: str, handle: str) -> str:
        m = self._find(name, handle)
        if m is None:
            return "InvalidReceipt"
        self.q[name].msgs.remove(m)
        return "ok"

    def nack(self, name: str, handle: str, now: float, delay: float) -> str:
        m = self._find(name, handle)
        if m is None:
            return "InvalidReceipt"
        m.avail, m.inflight, m.handle = now + delay, False, None
        return "ok"

    def extend(self, name: str, handle: str, now: float, vt: float) -> str:
        m = self._find(name, handle)
        if m is None:
            return "InvalidReceipt"
        m.avail = now + vt
        return "ok"

    def redrive(self, name: str, now: float) -> int:
        q, moved = self.q[name], 0
        for m in list(q.msgs):
            if m.source is None or (m.inflight and m.avail > now):
                continue
            q.msgs.remove(m)
            self.q[m.source].msgs.append(RefMsg(m.id, m.body, m.group, m.sent_at, now))
            moved += 1
        return moved

    def stats(self, name: str, now: float) -> dict:
        q = self.q[name]
        s = {"total": len(q.msgs), "visible": 0, "in_flight": 0, "delayed": 0, "blocked_by_group": 0}
        for m in q.msgs:
            if not q.is_head(m):
                s["blocked_by_group"] += 1
            elif m.avail <= now:
                s["visible"] += 1
            elif m.inflight:
                s["in_flight"] += 1
            else:
                s["delayed"] += 1
        return s

    def contents(self, name: str) -> list[tuple]:
        return [(m.id, m.body, m.rc) for m in self.q[name].msgs]
