"""Queue semantics, tested through an in-memory Broker with a manual clock."""

import random
import unittest

from dq import Broker, ManualClock
from dq.errors import InvalidReceipt, QueueConflict, QueueFull, QueueNotFound, ValidationError


class QueueTests(unittest.TestCase):
    def setUp(self) -> None:
        self.clock = ManualClock()
        self.b = Broker(None, clock=self.clock, rng=random.Random(1))

    def test_send_receive_ack(self) -> None:
        self.b.create_queue("q")
        sent = self.b.send("q", "hello")
        got = self.b.receive("q")
        self.assertEqual([m["body"] for m in got], ["hello"])
        self.assertEqual(got[0]["id"], sent["id"])
        self.b.ack("q", got[0]["handle"])
        self.assertEqual(self.b.stats("q")["total"], 0)

    def test_received_message_is_hidden_then_reappears_after_visibility_timeout(self) -> None:
        self.b.create_queue("q", visibility_timeout=30)
        self.b.send("q", "x")
        first = self.b.receive("q")
        self.assertEqual(len(first), 1)
        self.clock.advance(29.9)
        self.assertEqual(self.b.receive("q"), [])
        self.clock.advance(0.2)
        again = self.b.receive("q")
        self.assertEqual(again[0]["id"], first[0]["id"])
        self.assertEqual(again[0]["receive_count"], 2)

    def test_stale_receipt_handle_cannot_ack_after_redelivery(self) -> None:
        self.b.create_queue("q", visibility_timeout=10)
        self.b.send("q", "x")
        old = self.b.receive("q")[0]
        self.clock.advance(11)
        new = self.b.receive("q")[0]
        with self.assertRaises(InvalidReceipt):
            self.b.ack("q", old["handle"])
        self.assertEqual(self.b.stats("q")["total"], 1)  # still there
        self.b.ack("q", new["handle"])

    def test_double_ack_is_rejected(self) -> None:
        self.b.create_queue("q")
        self.b.send("q", "x")
        h = self.b.receive("q")[0]["handle"]
        self.b.ack("q", h)
        with self.assertRaises(InvalidReceipt):
            self.b.ack("q", h)

    def test_malformed_and_unknown_handles_are_rejected(self) -> None:
        self.b.create_queue("q")
        for bad in ("nodot", "abc.1", "", None, 5):
            with self.assertRaises(InvalidReceipt):
                self.b.ack("q", bad)  # type: ignore[arg-type]

    def test_batch_receive_returns_oldest_first_up_to_max(self) -> None:
        self.b.create_queue("q")
        for i in range(5):
            self.b.send("q", str(i))
        self.assertEqual([m["body"] for m in self.b.receive("q", 3)], ["0", "1", "2"])
        self.assertEqual([m["body"] for m in self.b.receive("q", 10)], ["3", "4"])

    def test_delayed_message_is_invisible_until_delay_passes(self) -> None:
        self.b.create_queue("q")
        self.b.send("q", "later", delay=60)
        self.assertEqual(self.b.receive("q"), [])
        self.assertEqual(self.b.stats("q")["delayed"], 1)
        self.clock.advance(60)
        self.assertEqual(len(self.b.receive("q")), 1)

    def test_queue_default_delay_applies(self) -> None:
        self.b.create_queue("q", delay=5)
        self.b.send("q", "x")
        self.assertEqual(self.b.receive("q"), [])
        self.clock.advance(5)
        self.assertEqual(len(self.b.receive("q")), 1)

    def test_nack_backoff_grows_exponentially_and_stays_under_the_cap(self) -> None:
        self.b.create_queue("q", backoff_base=2.0, backoff_cap=20.0, visibility_timeout=1000)
        self.b.send("q", "x")
        ceilings = [2.0, 4.0, 8.0, 16.0, 20.0, 20.0]  # min(cap, base * 2^(n-1))
        for ceiling in ceilings:
            m = self.b.receive("q")[0]
            delay = self.b.nack("q", m["handle"])["delay"]
            self.assertTrue(0 <= delay <= ceiling, (delay, ceiling))
            self.clock.advance(ceiling)  # always long enough to become visible again

    def test_nack_with_explicit_delay_and_dead_handle_after_nack(self) -> None:
        self.b.create_queue("q")
        self.b.send("q", "x")
        m = self.b.receive("q")[0]
        self.b.nack("q", m["handle"], delay=7)
        with self.assertRaises(InvalidReceipt):
            self.b.nack("q", m["handle"])
        self.clock.advance(6.9)
        self.assertEqual(self.b.receive("q"), [])
        self.clock.advance(0.2)
        self.assertEqual(len(self.b.receive("q")), 1)

    def test_change_visibility_extends_the_hold(self) -> None:
        self.b.create_queue("q", visibility_timeout=10)
        self.b.send("q", "x")
        m = self.b.receive("q")[0]
        self.clock.advance(9)
        self.b.change_visibility("q", m["handle"], 100)
        self.clock.advance(50)
        self.assertEqual(self.b.receive("q"), [])
        self.clock.advance(51)
        self.assertEqual(len(self.b.receive("q")), 1)

    def test_poison_message_moves_to_dlq_after_max_receive(self) -> None:
        self.b.create_queue("dlq")
        self.b.create_queue("q", max_receive=3, dlq="dlq", visibility_timeout=5)
        self.b.send("q", "poison")
        for _ in range(3):
            self.assertEqual(len(self.b.receive("q")), 1)
            self.clock.advance(6)
        self.assertEqual(self.b.receive("q"), [])  # 4th attempt dead-letters it
        self.assertEqual(self.b.stats("q")["total"], 0)
        dead = self.b.peek("dlq")
        self.assertEqual([(m["body"], m["source"], m["receive_count"]) for m in dead], [("poison", "q", 3)])

    def test_redrive_returns_dlq_messages_with_fresh_receive_count(self) -> None:
        self.b.create_queue("dlq")
        self.b.create_queue("q", max_receive=1, dlq="dlq", visibility_timeout=5)
        self.b.send("q", "poison")
        self.b.receive("q")
        self.clock.advance(6)
        self.b.receive("q")  # dead-letters
        self.assertEqual(self.b.redrive("dlq"), 1)
        self.assertEqual(self.b.stats("dlq")["total"], 0)
        again = self.b.receive("q")
        self.assertEqual((again[0]["body"], again[0]["receive_count"]), ("poison", 1))

    def test_handle_from_before_dead_lettering_stays_dead_after_redrive(self) -> None:
        self.b.create_queue("dlq")
        self.b.create_queue("q", max_receive=1, dlq="dlq", visibility_timeout=5)
        self.b.send("q", "x")
        stale = self.b.receive("q")[0]["handle"]
        self.clock.advance(6)
        self.b.receive("q")
        self.b.redrive("dlq")
        fresh = self.b.receive("q")[0]["handle"]
        self.assertNotEqual(stale, fresh)
        with self.assertRaises(InvalidReceipt):
            self.b.ack("q", stale)

    def test_dedup_id_suppresses_duplicates_inside_the_window_only(self) -> None:
        self.b.create_queue("q", dedup_window=300)
        a = self.b.send("q", "x", dedup_id="d1")
        b = self.b.send("q", "x", dedup_id="d1")
        self.assertEqual((a["duplicate"], b["duplicate"], a["id"] == b["id"]), (False, True, True))
        self.assertEqual(self.b.stats("q")["total"], 1)
        self.clock.advance(301)
        c = self.b.send("q", "x", dedup_id="d1")
        self.assertFalse(c["duplicate"])
        self.assertEqual(self.b.stats("q")["total"], 2)

    def test_max_depth_applies_backpressure(self) -> None:
        self.b.create_queue("q", max_depth=2)
        self.b.send("q", "1")
        self.b.send("q", "2")
        with self.assertRaises(QueueFull):
            self.b.send("q", "3")
        h = self.b.receive("q")[0]["handle"]
        self.b.ack("q", h)
        self.b.send("q", "3")  # room again

    def test_validation_errors(self) -> None:
        with self.assertRaises(ValidationError):
            self.b.create_queue("bad name!")
        with self.assertRaises(ValidationError):
            self.b.create_queue("q", max_receive=3)  # needs a dlq
        with self.assertRaises(QueueNotFound):
            self.b.create_queue("q", max_receive=3, dlq="nope")
        self.b.create_queue("q")
        with self.assertRaises(ValidationError):
            self.b.send("q", 123)  # type: ignore[arg-type]
        with self.assertRaises(ValidationError):
            self.b.send("q", "x" * (256 * 1024 + 1))
        with self.assertRaises(ValidationError):
            self.b.receive("q", 0)
        with self.assertRaises(ValidationError):
            self.b.receive("q", 1, visibility_timeout=0)
        with self.assertRaises(QueueNotFound):
            self.b.send("missing", "x")

    def test_create_queue_is_idempotent_but_rejects_conflicting_config(self) -> None:
        self.assertTrue(self.b.create_queue("q", delay=1))
        self.assertFalse(self.b.create_queue("q", delay=1))
        with self.assertRaises(QueueConflict):
            self.b.create_queue("q", delay=2)

    def test_stats_buckets(self) -> None:
        self.b.create_queue("q", visibility_timeout=10)
        for i in range(3):
            self.b.send("q", str(i))
        self.b.send("q", "d", delay=100)
        self.b.receive("q", 1)
        s = self.b.stats("q")
        self.assertEqual((s["total"], s["visible"], s["in_flight"], s["delayed"]), (4, 2, 1, 1))


class FifoTests(unittest.TestCase):
    def setUp(self) -> None:
        self.clock = ManualClock()
        self.b = Broker(None, clock=self.clock, rng=random.Random(2))
        self.b.create_queue("f", fifo=True, visibility_timeout=10)

    def test_fifo_requires_a_group(self) -> None:
        with self.assertRaises(ValidationError):
            self.b.send("f", "x")

    def test_order_is_preserved_within_a_group(self) -> None:
        for i in range(4):
            self.b.send("f", f"g1-{i}", group="g1")
        seen = []
        for _ in range(4):
            m = self.b.receive("f", 10)
            self.assertEqual(len(m), 1)  # only the head of the group is deliverable
            seen.append(m[0]["body"])
            self.b.ack("f", m[0]["handle"])
        self.assertEqual(seen, [f"g1-{i}" for i in range(4)])

    def test_group_is_blocked_while_its_head_is_in_flight(self) -> None:
        self.b.send("f", "a1", group="a")
        self.b.send("f", "a2", group="a")
        first = self.b.receive("f")[0]
        self.assertEqual(self.b.receive("f"), [])
        self.assertEqual(self.b.stats("f")["blocked_by_group"], 1)
        self.clock.advance(11)  # head times out and comes back before a2
        self.assertEqual(self.b.receive("f")[0]["id"], first["id"])

    def test_groups_are_delivered_in_parallel(self) -> None:
        for g in ("a", "b", "c"):
            self.b.send("f", f"{g}1", group=g)
            self.b.send("f", f"{g}2", group=g)
        got = self.b.receive("f", 10)
        self.assertEqual(sorted(m["body"] for m in got), ["a1", "b1", "c1"])

    def test_dead_lettering_a_head_unblocks_the_group(self) -> None:
        self.b.create_queue("fd")
        self.b.create_queue("f2", fifo=True, max_receive=1, dlq="fd", visibility_timeout=5)
        self.b.send("f2", "poison", group="g")
        self.b.send("f2", "next", group="g")
        self.b.receive("f2")
        self.clock.advance(6)
        got = self.b.receive("f2")  # poison is dead-lettered, then "next" is delivered
        self.assertEqual([m["body"] for m in got], ["next"])
        self.assertEqual([m["body"] for m in self.b.peek("fd")], ["poison"])

    def test_fifo_dedup(self) -> None:
        self.b.send("f", "x", group="g", dedup_id="same")
        self.assertTrue(self.b.send("f", "x", group="g", dedup_id="same")["duplicate"])
        self.assertEqual(self.b.stats("f")["total"], 1)


class SnapshotStateTests(unittest.TestCase):
    def test_state_survives_snapshot_roundtrip_and_behaves_identically(self) -> None:
        from dq.engine import Engine

        clock = ManualClock()
        b = Broker(None, clock=clock, rng=random.Random(3))
        b.create_queue("dlq")
        b.create_queue("q", max_receive=2, dlq="dlq", dedup_window=100)
        b.create_queue("f", fifo=True)
        for i in range(5):
            b.send("q", str(i), dedup_id=f"d{i}")
            b.send("f", str(i), group=f"g{i % 2}")
        b.receive("q", 2)
        b.receive("f", 2)
        state = b.export_state()
        clone = Engine.from_snapshot(state)
        self.assertEqual(clone.to_snapshot(), b._engine.to_snapshot())
        b2 = Broker(None, clock=clock)
        b2.install_state(state)
        clock.advance(1000)
        self.assertEqual([m["body"] for m in b2.receive("q", 10)], [m["body"] for m in b.receive("q", 10)])
        self.assertEqual([m["body"] for m in b2.receive("f", 10)], [m["body"] for m in b.receive("f", 10)])


if __name__ == "__main__":
    unittest.main()
