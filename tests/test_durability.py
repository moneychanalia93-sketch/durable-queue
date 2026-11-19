"""Crash recovery: replay, snapshots, torn tails, corruption."""

import random
import tempfile
import unittest
from pathlib import Path

from dq import Broker, ManualClock
from dq.broker import SNAPSHOT_NAME, WAL_NAME, write_snapshot_file
from dq.errors import BrokerFailed, CorruptSnapshot, InvalidReceipt, RecoveryError
from dq.wal import MAGIC, encode_record


class DurabilityTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.dir = Path(self._tmp.name)
        self.clock = ManualClock()

    def open(self, **kw) -> Broker:
        kw.setdefault("fsync", "none")
        b = Broker(self.dir, clock=self.clock, rng=random.Random(7), **kw)
        self.addCleanup(b.close)
        return b

    def _populate(self, b: Broker) -> None:
        b.create_queue("dlq")
        b.create_queue("q", max_receive=2, dlq="dlq", visibility_timeout=30)
        b.create_queue("f", fifo=True)
        for i in range(10):
            b.send("q", f"m{i}", dedup_id=f"d{i}")
            b.send("f", f"f{i}", group=f"g{i % 3}")
        got = b.receive("q", 4)
        b.ack("q", got[0]["handle"])
        b.nack("q", got[1]["handle"], delay=20)
        b.change_visibility("q", got[2]["handle"], 90)

    def test_reopen_restores_exact_state_including_in_flight_messages(self) -> None:
        b = self.open()
        self._populate(b)
        before = b.export_state()
        b.close()
        b2 = self.open()
        self.assertEqual(b2.export_state(), before)
        self.assertGreater(b2.replayed, 0)

    def test_receipt_handle_issued_before_a_crash_still_works_after_recovery(self) -> None:
        b = self.open()
        b.create_queue("q")
        b.send("q", "x")
        handle = b.receive("q")[0]["handle"]
        b.crash()
        b2 = self.open()
        b2.ack("q", handle)
        self.assertEqual(b2.stats("q")["total"], 0)

    def test_visibility_timeout_keeps_counting_across_a_restart(self) -> None:
        b = self.open()
        b.create_queue("q", visibility_timeout=30)
        b.send("q", "x")
        b.receive("q")
        b.close()
        self.clock.advance(31)
        b2 = self.open()
        self.assertEqual(len(b2.receive("q")), 1)

    def test_snapshot_compacts_the_log_and_recovery_matches(self) -> None:
        b = self.open()
        self._populate(b)
        size_before = (self.dir / WAL_NAME).stat().st_size
        lsn = b.snapshot()
        self.assertEqual((self.dir / WAL_NAME).read_bytes(), MAGIC)
        self.assertGreater(size_before, len(MAGIC))
        b.send("q", "after-snapshot")
        before = b.export_state()
        b.close()
        b2 = self.open()
        self.assertEqual(b2.export_state(), before)
        self.assertEqual(b2.replayed, 1)  # only the post-snapshot record is replayed
        self.assertEqual(b2.lsn, lsn + 1)

    def test_crash_between_snapshot_and_log_reset_does_not_double_apply(self) -> None:
        b = self.open()
        self._populate(b)
        before = b.export_state()
        write_snapshot_file(self.dir / SNAPSHOT_NAME, before)  # snapshot written, log NOT reset
        b.crash()
        b2 = self.open()
        self.assertEqual(b2.export_state(), before)
        self.assertEqual(b2.replayed, 0)

    def test_auto_snapshot_after_n_records(self) -> None:
        b = self.open(snapshot_every=10)
        b.create_queue("q")
        for i in range(25):
            b.send("q", str(i))
        self.assertTrue((self.dir / SNAPSHOT_NAME).exists())
        before = b.export_state()
        b.close()
        self.assertEqual(self.open().export_state(), before)

    def test_every_torn_tail_recovers_to_a_state_that_really_existed(self) -> None:
        """Truncate the log at every record boundary and mid-record offset."""
        b = self.open()
        states = [b.export_state()["queues"]]
        b.create_queue("q", visibility_timeout=30)
        states.append(b.export_state()["queues"])
        for i in range(6):
            b.send("q", f"m{i}")
            states.append(b.export_state()["queues"])
            if i % 2:
                got = b.receive("q", 1)
                states.append(b.export_state()["queues"])
                b.ack("q", got[0]["handle"])
                states.append(b.export_state()["queues"])
        b.close()
        wal_path = self.dir / WAL_NAME
        full = wal_path.read_bytes()
        for cut in range(len(MAGIC), len(full) + 1, 7):
            wal_path.write_bytes(full[:cut])
            r = Broker(self.dir, clock=self.clock, fsync="none")
            self.assertIn(r.export_state()["queues"], states, f"cut={cut}")
            r.close()
            wal_path.write_bytes(full)

    def test_corrupt_record_in_the_middle_stops_replay_there(self) -> None:
        b = self.open()
        b.create_queue("q")
        for i in range(5):
            b.send("q", f"m{i}")
        b.close()
        wal_path = self.dir / WAL_NAME
        data = bytearray(wal_path.read_bytes())
        # walk the frames to the start of the 4th record (lsn 4)
        pos = len(MAGIC)
        for _ in range(3):
            length = int.from_bytes(data[pos : pos + 4], "little")
            pos += 8 + length
        data[pos + 10] ^= 0x55
        wal_path.write_bytes(bytes(data))
        r = self.open()
        self.assertEqual(r.recovery.reason, "checksum mismatch")
        self.assertEqual(r.lsn, 3)  # create + 2 sends survive
        self.assertEqual(r.stats("q")["total"], 2)
        r.send("q", "new", message_id="fresh")  # and the log is usable again
        self.assertEqual(r.stats("q")["total"], 3)

    def test_garbage_appended_after_a_good_log_is_discarded(self) -> None:
        b = self.open()
        b.create_queue("q")
        b.send("q", "keep")
        b.close()
        with open(self.dir / WAL_NAME, "ab") as fh:
            fh.write(b"\x00garbage-after-power-loss" * 3)
        r = self.open()
        self.assertEqual(r.stats("q")["total"], 1)
        self.assertGreater(r.recovery.discarded_bytes, 0)

    def test_corrupt_snapshot_is_refused_not_ignored(self) -> None:
        b = self.open()
        self._populate(b)
        b.snapshot()
        b.close()
        p = self.dir / SNAPSHOT_NAME
        data = bytearray(p.read_bytes())
        data[-5] ^= 0x01
        p.write_bytes(bytes(data))
        with self.assertRaises(CorruptSnapshot):
            Broker(self.dir, clock=self.clock)

    def test_a_gap_in_lsns_is_a_recovery_error(self) -> None:
        b = self.open()
        b.create_queue("q")
        b.close()
        with open(self.dir / WAL_NAME, "ab") as fh:
            fh.write(encode_record({"lsn": 99, "op": {"op": "create", "cfg": {"name": "z"}, "now": 0}}))
        with self.assertRaises(RecoveryError):
            Broker(self.dir, clock=self.clock)

    def test_failed_or_crashed_broker_is_fail_stop(self) -> None:
        b = self.open()
        b.create_queue("q")
        b.crash()
        with self.assertRaises(BrokerFailed):
            b.send("q", "x")
        with self.assertRaises(BrokerFailed):
            b.stats("q")

    def test_fsync_policy_changes_fsync_count_not_results(self) -> None:
        counts = {}
        for policy in ("always", "batch", "none"):
            d = self.dir / policy
            b = Broker(d, clock=self.clock, fsync=policy, batch_size=10, batch_interval=3600)
            b.create_queue("q")
            for i in range(30):
                b.send("q", str(i))
            counts[policy] = b._wal.fsync_count
            b.close()
            again = Broker(d, clock=self.clock)
            self.assertEqual(again.stats("q")["total"], 30)
            again.close()
        self.assertEqual(counts["always"], 31)
        self.assertEqual(counts["batch"], 3)  # 31 records, one fsync per 10
        self.assertEqual(counts["none"], 0)

    def test_failed_commands_are_not_logged(self) -> None:
        b = self.open()
        b.create_queue("q")
        lsn = b.lsn
        with self.assertRaises(InvalidReceipt):
            b.ack("q", "nope.1")
        self.assertEqual(b.receive("q"), [])  # empty poll changes nothing
        self.assertEqual(b.lsn, lsn)
        self.assertEqual(len(b.export_state()["queues"]), 1)


if __name__ == "__main__":
    unittest.main()
