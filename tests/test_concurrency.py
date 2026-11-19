"""Concurrency and chaos tests.

The guarantee under test is AT-LEAST-ONCE delivery:
  * no message is ever lost,
  * a message is acknowledged successfully at most once (stale receipts are refused),
  * duplicates DO happen (a consumer that dies after working but before acking),
    so an idempotent consumer is what makes the *effect* happen once.
"""

import random
import shutil
import tempfile
import threading
import time
import unittest
from collections import Counter
from pathlib import Path

from dq import Broker, Cluster, ManualClock
from dq.errors import BrokerFailed, InvalidReceipt


class ThreadedStressTests(unittest.TestCase):
    def _run(self, queue_api, n_producers: int, per_producer: int, n_consumers: int, chaos=None) -> dict:
        """Producers and crashing consumers hammer ``queue_api`` until everything is acked."""
        total = n_producers * per_producer
        sent: set[str] = set()
        sent_lock = threading.Lock()
        acked: Counter[str] = Counter()  # successful acks per message
        processed: Counter[str] = Counter()  # times a consumer did the work
        stale = [0]
        lock = threading.Lock()
        producers_done = threading.Event()
        stop = threading.Event()
        failures: list[BaseException] = []

        def producer(n: int) -> None:
            try:
                for i in range(per_producer):
                    mid = f"p{n}-{i}"
                    queue_api.send("q", mid)
                    with sent_lock:
                        sent.add(mid)
            except BaseException as exc:  # noqa: BLE001
                failures.append(exc)

        def consumer(n: int) -> None:
            rnd = random.Random(1000 + n)
            try:
                while not stop.is_set():
                    got = queue_api.receive("q", 5)
                    if not got:
                        time.sleep(0.002)
                        continue
                    for m in got:
                        roll = rnd.random()
                        if roll < 0.15:
                            continue  # crash mid-processing: never works, never acks
                        with lock:
                            processed[m["body"]] += 1
                        if roll < 0.20:
                            time.sleep(0.08)  # stall past the visibility timeout, then ack late
                        try:
                            queue_api.ack("q", m["handle"])
                            with lock:
                                acked[m["body"]] += 1
                        except InvalidReceipt:
                            with lock:
                                stale[0] += 1
            except BaseException as exc:  # noqa: BLE001
                failures.append(exc)

        prods = [threading.Thread(target=producer, args=(n,)) for n in range(n_producers)]
        cons = [threading.Thread(target=consumer, args=(n,)) for n in range(n_consumers)]
        chaos_thread = threading.Thread(target=chaos) if chaos else None
        for t in cons + prods + ([chaos_thread] if chaos_thread else []):
            t.start()
        for t in prods:
            t.join()
        producers_done.set()
        deadline = time.time() + 40
        while time.time() < deadline:
            with lock:
                if len(acked) == total:
                    break
            time.sleep(0.02)
        stop.set()
        for t in cons + ([chaos_thread] if chaos_thread else []):
            t.join()
        self.assertEqual(failures, [])
        return {"sent": sent, "acked": acked, "processed": processed, "stale": stale[0], "total": total}

    def _assert_invariants(self, r: dict) -> None:
        self.assertEqual(len(r["sent"]), r["total"])
        self.assertEqual(set(r["acked"]), r["sent"], "a message was lost")
        self.assertTrue(all(c == 1 for c in r["acked"].values()), "a message was acknowledged twice")
        self.assertTrue(set(r["processed"]) >= r["sent"])  # every message was worked on at least once

    def test_broker_many_producers_and_consumers_with_crashing_workers(self) -> None:
        b = Broker(None)
        b.create_queue("q", visibility_timeout=0.05)
        r = self._run(b, n_producers=4, per_producer=250, n_consumers=8)
        self._assert_invariants(r)
        extra = sum(c - 1 for c in r["processed"].values())
        print(f"\n  [stress] broker: {r['total']} msgs, {extra} duplicate deliveries processed, {r['stale']} stale acks refused", end="")
        self.assertGreater(extra + r["stale"], 0, "chaos should have caused redeliveries")

    def test_cluster_with_replication_survives_a_leader_crash_under_load(self) -> None:
        c = Cluster(None, nodes=["a", "b", "c"], replicas=1, seed=3)
        self.addCleanup(c.close)
        c.create_queue("q", visibility_timeout=0.05)

        def chaos() -> None:
            time.sleep(0.05)
            c.node("b").fail_leader()

        r = self._run(c, n_producers=4, per_producer=150, n_consumers=6, chaos=chaos)
        self._assert_invariants(r)
        self.assertEqual(c.node("b").failovers, 1)

    def test_idempotent_consumer_turns_at_least_once_into_exactly_once_effects(self) -> None:
        b = Broker(None)
        b.create_queue("q", visibility_timeout=0.03)
        for i in range(200):
            b.send("q", f"pay-{i}")
        ledger: Counter[str] = Counter()  # the side effect we must not double-apply
        applied: set[str] = set()
        lock = threading.Lock()

        def worker(n: int) -> None:
            rnd = random.Random(n)
            idle = 0
            while idle < 100:
                got = b.receive("q", 4)
                idle = 0 if got else idle + 1
                for m in got:
                    with lock:
                        if m["body"] not in applied:  # the idempotency check
                            applied.add(m["body"])
                            ledger[m["body"]] += 1
                    if rnd.random() < 0.3:
                        continue  # crash AFTER the effect, BEFORE the ack -> will be redelivered
                    try:
                        b.ack("q", m["handle"])
                    except InvalidReceipt:
                        pass
                if not got:
                    time.sleep(0.01)

        threads = [threading.Thread(target=worker, args=(n,)) for n in range(6)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(b.stats("q")["total"], 0)
        self.assertEqual(set(ledger), {f"pay-{i}" for i in range(200)})
        self.assertTrue(all(v == 1 for v in ledger.values()))


class SeededChaosTests(unittest.TestCase):
    """Single-threaded, fully deterministic fault injection (same seed = same run)."""

    def _run_seed(self, seed: int) -> dict:
        rnd = random.Random(seed)
        clock = ManualClock()
        tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, tmp, True)
        path = Path(tmp)

        def open_broker() -> Broker:
            b = Broker(path, clock=clock, fsync="none", snapshot_every=50, rng=random.Random(seed))
            self.addCleanup(b.close)
            return b

        b = open_broker()
        b.create_queue("dlq")
        b.create_queue("q", visibility_timeout=20, max_receive=12, dlq="dlq", backoff_base=1, backoff_cap=10)
        sent: list[str] = []
        holdings: list[tuple[str, str]] = []  # (message id, handle) held by "workers"
        acked: Counter[str] = Counter()
        deliveries: Counter[str] = Counter()
        stats = {"crashes": 0, "restarts": 0, "stale_acks": 0, "failovers": 0}
        poison = {f"m{i}" for i in range(0, 120, 17)}

        def try_ack(mid: str, handle: str) -> None:
            try:
                b.ack("q", handle)
                acked[mid] += 1
            except InvalidReceipt:
                stats["stale_acks"] += 1

        for step in range(1500):
            action = rnd.random()
            if action < 0.12 and len(sent) < 120:
                mid = f"m{len(sent)}"
                b.send("q", mid, message_id=mid)
                sent.append(mid)
            elif action < 0.30:
                for m in b.receive("q", rnd.randint(1, 3)):
                    deliveries[m["id"]] += 1
                    holdings.append((m["id"], m["handle"]))
            elif action < 0.75 and holdings:
                mid, handle = holdings.pop(rnd.randrange(len(holdings)))
                outcome = rnd.random()
                if mid in poison:
                    if outcome < 0.5:  # a poison message can only fail (nack) or crash its worker; it never acks
                        try:
                            b.nack("q", handle)
                        except InvalidReceipt:
                            stats["stale_acks"] += 1
                elif outcome < 0.65:
                    try_ack(mid, handle)
                elif outcome < 0.80:
                    stats["crashes"] += 1  # worker dies; handle is simply forgotten
                elif outcome < 0.90:
                    try:
                        b.nack("q", handle)
                    except InvalidReceipt:
                        stats["stale_acks"] += 1
                else:
                    holdings.append((mid, handle))  # a slow worker: tries again later (maybe stale)
            elif action < 0.93:
                clock.advance(rnd.choice([1, 3, 8, 25]))
            elif action < 0.95:
                b.crash()  # the whole broker process dies; recover from disk
                b = open_broker()
                stats["restarts"] += 1
        # Drain: keep working until nothing is left in the queue.
        for _ in range(300):
            clock.advance(30)
            for m in b.receive("q", 50):
                deliveries[m["id"]] += 1
                if m["id"] in poison:
                    continue
                try_ack(m["id"], m["handle"])
            for mid, handle in holdings:
                if mid not in poison and rnd.random() < 0.5:
                    try_ack(mid, handle)
            holdings.clear()
            if b.stats("q")["total"] == 0:
                break
        dead = {m["id"] for m in b.peek("dlq", 1000)}
        return {"poison": poison, "sent": set(sent), "acked": acked, "dead": dead, "left": b.stats("q")["total"], "deliveries": deliveries, "stats": stats}

    def test_no_loss_and_no_double_ack_across_many_seeds(self) -> None:
        total_dups = total_restarts = total_dead = 0
        for seed in range(25):
            r = self._run_seed(seed)
            self.assertEqual(r["left"], 0, f"seed {seed}: queue not drained")
            self.assertEqual(set(r["acked"]) | r["dead"], r["sent"], f"seed {seed}: message lost")
            self.assertEqual(set(r["acked"]) & r["dead"], set(), f"seed {seed}: acked and dead-lettered")
            self.assertTrue(all(c == 1 for c in r["acked"].values()), f"seed {seed}: double ack")
            self.assertTrue(r["poison"] & r["sent"] <= r["dead"], f"seed {seed}: a poison message escaped the DLQ")
            total_dups += sum(c - 1 for c in r["deliveries"].values() if c > 1)
            total_restarts += r["stats"]["restarts"]
            total_dead += len(r["dead"])
        print(f"\n  [chaos] 25 seeds: {total_dups} redeliveries, {total_restarts} broker crashes, {total_dead} messages dead-lettered (poison), 0 lost", end="")
        self.assertGreater(total_dups, 0)
        self.assertGreater(total_restarts, 0)
        self.assertGreater(total_dead, 0)

    def test_same_seed_gives_the_same_run(self) -> None:
        a, b = self._run_seed(7), self._run_seed(7)
        self.assertEqual(a["deliveries"], b["deliveries"])
        self.assertEqual(a["dead"], b["dead"])
        self.assertEqual(a["stats"], b["stats"])

    def test_broker_that_is_down_refuses_work_instead_of_losing_it(self) -> None:
        b = Broker(None)
        b.create_queue("q")
        b.crash()
        with self.assertRaises(BrokerFailed):
            b.send("q", "x")


if __name__ == "__main__":
    unittest.main()
