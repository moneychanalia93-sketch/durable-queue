"""Partitioning, replication and failover across several in-process nodes.

* A ``Node`` is one partition: a leader ``Broker`` plus optional followers that get
  every command synchronously (so an acknowledged write is on both before the
  caller hears about it).
* A ``Cluster`` owns a consistent-hash ring. A message's *routing key* is its FIFO
  group (so a group's order lives on one node), else its dedup id, else its own id.
* Receipt handles are prefixed ``<node>|`` so an ack goes straight back to the node
  that issued it.

Honest limits: nodes are objects in one process (a simulation of distribution, not
a network protocol), failover is promote-the-follower with no election or fencing,
and only one follower per leader is exercised by the tests.
"""

from __future__ import annotations

import itertools
import json
import os
import random
import re
import threading
from pathlib import Path
from typing import Any

from .broker import Broker
from .clock import Clock, SystemClock
from .engine import MAX_RECEIVE_BATCH
from .errors import BrokerFailed, InvalidReceipt, ValidationError
from .ring import HashRing
from .rwlock import RWLock

_NODE_RE = re.compile(r"^[A-Za-z0-9_-]{1,40}$")
_SUM_FIELDS = ("total", "visible", "in_flight", "delayed", "blocked_by_group")


class Node:
    """A partition: one leader broker and (optionally) a synchronous follower."""

    def __init__(self, name: str, path: Path | None, replicas: int, **broker_kw: Any) -> None:
        self.name = name
        self._path = path
        self._broker_kw = broker_kw
        self._lock = threading.Lock()
        self.failovers = 0
        self.leader: Broker
        self.followers: list[Broker] = []
        count = replicas + 1
        existing = [p for p in path.glob("r*") if p.is_dir()] if path else []
        brokers = [self._open(f"r{i}") for i in range(max(count, len(existing)))]
        # After a restart the replica with the highest LSN holds everything that was acknowledged.
        self.leader = max(brokers, key=lambda b: b.lsn)
        for b in brokers:
            if b is not self.leader:
                self.leader.add_replica(b)  # resync from the leader's state
                self.followers.append(b)

    def _open(self, replica_dir: str) -> Broker:
        """Open (or recover) one replica's broker in its own directory."""
        return Broker(self._path / replica_dir if self._path else None, **self._broker_kw)

    def call(self, method: str, *args: Any, **kwargs: Any) -> Any:
        """Run a broker method on the leader; on a dead leader, fail over once and retry."""
        leader = self.leader
        try:
            return getattr(leader, method)(*args, **kwargs)
        except BrokerFailed:
            if not self._promote(leader):
                raise
            return getattr(self.leader, method)(*args, **kwargs)

    def _promote(self, failed: Broker) -> bool:
        """Make the most up-to-date follower the leader. Returns False if none is alive."""
        with self._lock:
            if self.leader is not failed:
                return True  # another thread already failed over
            live = [f for f in self.followers if not f.failed]
            if not live:
                return False
            new = max(live, key=lambda b: b.lsn)
            self.followers = [f for f in live if f is not new]
            for f in self.followers:
                new.add_replica(f)
            self.leader = new
            self.failovers += 1
            return True

    def fail_leader(self) -> None:
        """Simulate the leader process dying. The next call triggers failover."""
        self.leader.crash()

    def add_follower(self) -> None:
        """Attach a fresh follower, seeded from the leader's current state."""
        with self._lock:
            used = len(list(self._path.glob("r*"))) if self._path else 0
            follower = self._open(f"r{used}")
            self.leader.add_replica(follower)
            self.followers.append(follower)

    def close(self) -> None:
        """Close every broker (flushes and fsyncs the logs)."""
        for b in [self.leader, *self.followers]:
            b.close()


class Cluster:
    """The public entry point: same queue API as a Broker, spread over several nodes."""

    def __init__(
        self,
        path: str | os.PathLike[str] | None = None,
        nodes: list[str] | None = None,
        *,
        vnodes: int = 128,
        replicas: int = 0,
        clock: Clock | None = None,
        fsync: str = "batch",
        seed: int | None = None,  # for tests only: reusing a seed after a restart regenerates old message ids
        **broker_kw: Any,
    ) -> None:
        self._path = Path(path) if path is not None else None
        self._clock = clock or SystemClock()
        self._rng = random.Random(seed)
        self._id_lock = threading.Lock()
        self._topo = RWLock()
        self._rr = itertools.count()
        self._broker_kw = {"clock": self._clock, "fsync": fsync, **broker_kw}
        meta = self._load_meta()
        if meta is not None:
            nodes, vnodes, replicas = meta["nodes"], meta["vnodes"], meta["replicas"]
        if not nodes:
            raise ValidationError("a cluster needs at least one node name")
        for n in nodes:
            self._check_name(n)
        self._replicas = replicas
        self._ring = HashRing(nodes, vnodes)
        self._nodes: dict[str, Node] = {}
        for i, n in enumerate(nodes):
            self._nodes[n] = self._make_node(n, seed_offset=i)
        self._save_meta()

    # ----------------------------------------------------------------- helpers
    @staticmethod
    def _check_name(name: str) -> None:
        """Reject node names that would break routing or file paths."""
        if not isinstance(name, str) or not _NODE_RE.match(name):
            raise ValidationError("node name must match [A-Za-z0-9_-]{1,40}")

    def _make_node(self, name: str, seed_offset: int = 0) -> Node:
        """Build a Node with its own seeded random generator."""
        kw = dict(self._broker_kw)
        kw["rng"] = random.Random(self._rng.getrandbits(64) + seed_offset)
        return Node(name, self._path / name if self._path else None, self._replicas, **kw)

    def _meta_path(self) -> Path | None:
        """Where the cluster topology is persisted (None for in-memory clusters)."""
        return self._path / "cluster.json" if self._path else None

    def _load_meta(self) -> dict[str, Any] | None:
        """Read the saved topology, if any."""
        p = self._meta_path()
        return json.loads(p.read_text()) if p is not None and p.exists() else None

    def _save_meta(self) -> None:
        """Atomically save the current topology."""
        p = self._meta_path()
        if p is None:
            return
        p.parent.mkdir(parents=True, exist_ok=True)
        tmp = p.with_suffix(".tmp")
        tmp.write_text(json.dumps({"nodes": self._ring.nodes, "vnodes": self._ring.vnodes, "replicas": self._replicas}))
        os.replace(tmp, p)

    def _new_id(self) -> str:
        """A fresh 96-bit random message id."""
        with self._id_lock:
            return f"{self._rng.getrandbits(96):024x}"

    def _node_for_handle(self, handle: str) -> tuple[Node, str]:
        """Split ``node|handle`` and find the node that issued it."""
        name, sep, inner = handle.partition("|") if isinstance(handle, str) else ("", "", "")
        node = self._nodes.get(name)
        if not sep or node is None:
            raise InvalidReceipt("receipt handle does not belong to a live node")
        return node, inner

    @property
    def ring(self) -> HashRing:
        """The current hash ring (read-only use)."""
        return self._ring

    def node(self, name: str) -> Node:
        """Look up a node by name (used by tests and the demo to inject failures)."""
        return self._nodes[name]

    # -------------------------------------------------------------- queue API
    def create_queue(self, name: str, **config: Any) -> bool:
        """Create the queue on every node. Returns True if it was newly created anywhere."""
        with self._topo.read():
            return any([n.call("create_queue", name, **config) for n in self._nodes.values()])

    def send(
        self,
        queue: str,
        body: str,
        *,
        group: str | None = None,
        delay: float | None = None,
        dedup_id: str | None = None,
    ) -> dict[str, Any]:
        """Send a message to the node that owns its routing key."""
        mid = self._new_id()
        key = group or dedup_id or mid
        with self._topo.read():
            node = self._nodes[self._ring.get(key)]
            res = node.call("send", queue, body, group=group, delay=delay, dedup_id=dedup_id, message_id=mid)
        return {**res, "node": node.name}

    def receive(self, queue: str, max_messages: int = 1, visibility_timeout: float | None = None) -> list[dict[str, Any]]:
        """Receive from nodes in rotating order so no node is starved."""
        if not isinstance(max_messages, int) or not 1 <= max_messages <= MAX_RECEIVE_BATCH:
            raise ValidationError(f"max must be an integer in 1..{MAX_RECEIVE_BATCH}")
        out: list[dict[str, Any]] = []
        with self._topo.read():
            names = sorted(self._nodes)
            start = next(self._rr) % len(names)
            for name in names[start:] + names[:start]:
                want = max_messages - len(out)
                if want <= 0:
                    break
                for m in self._nodes[name].call("receive", queue, want, visibility_timeout):
                    m["handle"] = f"{name}|{m['handle']}"
                    out.append(m)
        return out

    def ack(self, queue: str, handle: str) -> dict[str, Any]:
        """Delete a message on the node that issued the handle."""
        with self._topo.read():
            node, inner = self._node_for_handle(handle)
            return node.call("ack", queue, inner)

    def nack(self, queue: str, handle: str, delay: float | None = None) -> dict[str, Any]:
        """Return a message for retry on the node that issued the handle."""
        with self._topo.read():
            node, inner = self._node_for_handle(handle)
            return node.call("nack", queue, inner, delay)

    def change_visibility(self, queue: str, handle: str, seconds: float) -> dict[str, Any]:
        """Extend or shorten a message's hold on the node that issued the handle."""
        with self._topo.read():
            node, inner = self._node_for_handle(handle)
            return node.call("change_visibility", queue, inner, seconds)

    def stats(self, queue: str) -> dict[str, Any]:
        """Totals across nodes plus a per-node breakdown."""
        with self._topo.read():
            per_node = {name: n.call("stats", queue) for name, n in sorted(self._nodes.items())}
        total: dict[str, Any] = {k: sum(s[k] for s in per_node.values()) for k in _SUM_FIELDS}
        return {"queue": queue, **total, "nodes": per_node}

    def peek(self, queue: str, limit: int = 100) -> list[dict[str, Any]]:
        """Messages across nodes (oldest first, by send time), without receiving them."""
        with self._topo.read():
            merged = [{**m, "node": name} for name, n in self._nodes.items() for m in n.call("peek", queue, limit)]
        return sorted(merged, key=lambda m: (m["sent_at"], m["id"]))[:limit]

    def redrive(self, queue: str, max_messages: int = 1000) -> int:
        """Redrive every node's share of a dead-letter queue."""
        with self._topo.read():
            return sum(n.call("redrive", queue, max_messages) for n in self._nodes.values())

    def list_queues(self) -> list[dict[str, Any]]:
        """Queue definitions (identical on every node)."""
        with self._topo.read():
            return next(iter(self._nodes.values())).call("list_queues")

    def snapshot(self) -> dict[str, int]:
        """Snapshot + compact every node's log. Returns node -> snapshot LSN."""
        with self._topo.read():
            return {name: n.call("snapshot") for name, n in self._nodes.items()}

    def topology(self) -> dict[str, Any]:
        """Nodes with their LSN, follower count and failover count."""
        with self._topo.read():
            return {
                "vnodes": self._ring.vnodes,
                "replicas": self._replicas,
                "nodes": {
                    name: {"lsn": n.leader.lsn, "followers": len(n.followers), "failovers": n.failovers}
                    for name, n in sorted(self._nodes.items())
                },
            }

    # ------------------------------------------------------ topology changes
    def _create_queues_on(self, node: Node) -> None:
        """Copy queue definitions to a new node, dead-letter queues before their users."""
        pending = {c["name"]: c for c in next(iter(self._nodes.values())).call("list_queues")}
        while pending:
            ready = [c for c in pending.values() if not c["dlq"] or c["dlq"] not in pending]
            for cfg in ready:
                name = cfg.pop("name")
                node.call("create_queue", name, **cfg)
                del pending[name]

    def _rebalance(self, new_ring: HashRing, sources: list[Node]) -> dict[str, int]:
        """Move every message whose owner changed. Must run under the write lock.

        Order matters for safety: copy to the new owner first, delete from the old
        owner second. A crash in between duplicates messages (allowed by
        at-least-once) instead of losing them.
        """
        moved = total = 0
        queues = [c["name"] for c in next(iter(self._nodes.values())).call("list_queues")]
        for src in sources:
            for q in queues:
                msgs = src.call("dump_messages", q)
                total += len(msgs)
                by_target: dict[str, list[dict[str, Any]]] = {}
                for m in msgs:
                    owner = new_ring.get(m["group"] or m["id"])
                    if owner != src.name:
                        by_target.setdefault(owner, []).append(m)
                for owner, batch in by_target.items():
                    self._nodes[owner].call("import_messages", q, batch)
                    src.call("delete_ids", q, [m["id"] for m in batch])
                    moved += len(batch)
        return {"moved": moved, "total": total}

    def add_node(self, name: str) -> dict[str, Any]:
        """Add a node and migrate the messages it now owns. In-flight messages that move
        become visible again (their old handles stop working)."""
        self._check_name(name)
        with self._topo.write():
            if name in self._nodes:
                raise ValidationError(f"node {name!r} already exists")
            node = self._make_node(name, seed_offset=len(self._nodes))
            self._create_queues_on(node)
            new_ring = self._ring.copy()
            new_ring.add(name)
            before = dict(self._nodes)
            self._nodes[name] = node
            report = self._rebalance(new_ring, list(before.values()))
            self._ring = new_ring
            self._save_meta()
        return {**report, "fraction": report["moved"] / report["total"] if report["total"] else 0.0}

    def remove_node(self, name: str) -> dict[str, Any]:
        """Drain a node into the others and drop it. Its files stay on disk, unused."""
        with self._topo.write():
            if name not in self._nodes:
                raise ValidationError(f"no such node {name!r}")
            if len(self._nodes) == 1:
                raise ValidationError("cannot remove the last node")
            new_ring = self._ring.copy()
            new_ring.remove(name)
            leaving = self._nodes[name]
            report = self._rebalance(new_ring, [leaving])
            del self._nodes[name]
            self._ring = new_ring
            self._save_meta()
            leaving.close()
        return {**report, "fraction": report["moved"] / report["total"] if report["total"] else 0.0}

    def close(self) -> None:
        """Close every broker (flushes and fsyncs the logs)."""
        with self._topo.write():
            for n in self._nodes.values():
                n.close()
