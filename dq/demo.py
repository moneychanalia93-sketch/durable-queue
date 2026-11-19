"""A narrated, deterministic tour of the guarantees. Run with ``python -m dq demo``.

It uses a manual clock, so "30 seconds later" costs nothing and the output is the
same every run. Everything here is synthetic data.
"""

from __future__ import annotations

import shutil
import tempfile
from collections import Counter, defaultdict

from .clock import ManualClock
from .cluster import Cluster
from .errors import InvalidReceipt


def _title(text: str) -> None:
    """Print a section heading."""
    print(f"\n=== {text} ===")


def run_demo(quick: bool = False) -> None:
    """Run the whole narrated demo."""
    n_orders = 20 if quick else 60
    clock = ManualClock()
    tmp = tempfile.mkdtemp(prefix="dq-demo-")
    cluster = Cluster(tmp, nodes=["n1", "n2", "n3"], replicas=1, clock=clock, fsync="batch", seed=42)
    try:
        # ------------------------------------------------------------------ 1
        _title("1. At-least-once delivery + an idempotent consumer")
        cluster.create_queue("orders-dlq")
        cluster.create_queue("orders", visibility_timeout=30, max_receive=3, dlq="orders-dlq", backoff_base=2, backoff_cap=20)
        poison_id = "order-7"
        for i in range(n_orders):
            cluster.send("orders", f"order-{i}")
        print(f"sent {n_orders} orders across 3 nodes (replicated, logged to a temporary directory)")

        deliveries: Counter[str] = Counter()
        effects: set[str] = set()  # the "database": applying an order twice would be a bug
        crashed_once: set[str] = set()
        poison_fixed = False

        def work(batch_size: int = 10) -> int:
            """One worker pass: receive a batch, process each message, ack or fail it."""
            handled = 0
            for m in cluster.receive("orders", batch_size):
                handled += 1
                deliveries[m["body"]] += 1
                if m["body"] == poison_id and not poison_fixed:
                    cluster.nack("orders", m["handle"])  # a bug: this message can never succeed
                    continue
                if m["receive_count"] == 1 and hash_bucket(m["body"]) == 0 and m["body"] not in crashed_once:
                    crashed_once.add(m["body"])  # the worker dies before acking
                    continue
                if m["body"] not in effects:  # idempotency check: skip work already done
                    effects.add(m["body"])
                cluster.ack("orders", m["handle"])
            return handled

        for _ in range(40):
            work()
            clock.advance(31)  # let every visibility timeout and backoff expire
        dup = sum(c - 1 for c in deliveries.values() if c > 1)
        print(f"worker crashes simulated   : {len(crashed_once)}")
        print(f"messages delivered > once  : {dup} extra deliveries (crashed workers + retries of the poison message)")
        print(f"unique effects applied     : {len(effects)} of {n_orders} (the missing one is the poison message; every other order applied exactly once)")

        # ------------------------------------------------------------------ 2
        _title("2. Poison message -> dead-letter queue -> inspect -> redrive")
        dead = cluster.peek("orders-dlq")
        print(f"DLQ holds {len(dead)} message(s): " + ", ".join(f"{m['body']} (received {m['receive_count']}x, from {m['source']})" for m in dead))
        poison_fixed = True
        print("(we 'deploy a fix', then redrive)")
        print(f"redriven: {cluster.redrive('orders-dlq')} message(s)")
        for _ in range(3):
            work()
            clock.advance(31)
        print(f"orders applied now: {len(effects)} of {n_orders}; DLQ size: {cluster.stats('orders-dlq')['total']}")

        # ------------------------------------------------------------------ 3
        _title("3. Receipt handles stop a stale consumer")
        cluster.create_queue("jobs", visibility_timeout=10)
        cluster.send("jobs", "slow-job")
        a = cluster.receive("jobs")[0]
        clock.advance(11)  # consumer A stalls past the visibility timeout
        b = cluster.receive("jobs")[0]
        print(f"A and B hold the same message ({a['id'][:8]}), receive_count A={a['receive_count']} B={b['receive_count']}")
        try:
            cluster.ack("jobs", a["handle"])
        except InvalidReceipt as exc:
            print(f"A's late ack is rejected: {exc}")
        cluster.ack("jobs", b["handle"])
        print("B's ack succeeds; message deleted exactly once")

        # ------------------------------------------------------------------ 4
        _title("4. FIFO groups: order inside a group, parallel across groups")
        cluster.create_queue("ledger", fifo=True, visibility_timeout=30)
        for step in range(4):
            for acct in ("alice", "bob", "carol"):
                cluster.send("ledger", f"{acct}:{step}", group=acct)
        seen: dict[str, list[int]] = defaultdict(list)
        rounds = 0
        while True:
            got = cluster.receive("ledger", 10)
            if not got:
                break
            rounds += 1
            if rounds == 1:
                print(f"first receive returned {len(got)} messages, one per group: {sorted(m['body'] for m in got)}")
            for m in got:
                acct, step = m["body"].split(":")
                seen[acct].append(int(step))
                cluster.ack("ledger", m["handle"])
        print("per-group order: " + "; ".join(f"{k}={v}" for k, v in sorted(seen.items())))

        # ------------------------------------------------------------------ 5
        _title("5. Add a node: only ~1/N of messages move")
        cluster.create_queue("events")
        for i in range(2000):
            cluster.send("events", f"evt-{i}")
        report = cluster.add_node("n4")
        print(f"added n4: moved {report['moved']} of {report['total']} messages ({report['fraction']:.1%}); ideal 1/4 = 25.0%")
        print("per-node totals:", {n: s["total"] for n, s in cluster.stats("events")["nodes"].items()})

        # ------------------------------------------------------------------ 6
        _title("6. Leader failure and failover")
        before = cluster.stats("events")["total"]
        cluster.node("n2").fail_leader()
        after = cluster.stats("events")["total"]
        print(f"killed n2's leader; follower promoted (failovers={cluster.node('n2').failovers}); events before={before} after={after}")

        # ------------------------------------------------------------------ 7
        _title("7. Crash recovery from the write-ahead log")
        state_before = {n: cluster.node(n).leader.export_state() for n in ("n1", "n3", "n4")}
        cluster.close()
        reopened = Cluster(tmp, clock=clock, fsync="batch")
        try:
            ok = all(reopened.node(n).leader.export_state()["queues"] == s["queues"] for n, s in state_before.items())
            print(f"reopened {len(reopened.topology()['nodes'])} nodes from disk; state identical to before shutdown: {ok}")
            rec = reopened.node("n1").leader
            print(f"n1 replayed {rec.replayed} log record(s) on top of its snapshot/log; torn bytes discarded: {rec.recovery.discarded_bytes}")
            print(f"events still queued: {reopened.stats('events')['total']}")
        finally:
            reopened.close()
        print("\nDone. Guarantee shown: at-least-once delivery. Duplicates happened; idempotent handling made the effects exactly-once.")
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def hash_bucket(text: str) -> int:
    """Deterministic 'which worker crashes' picker (Python's hash() is salted per run)."""
    return sum(text.encode()) % 5
