"""Write-ahead log: framing, checksums, torn tails, fsync policies."""

import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from dq.errors import CorruptLog
from dq.wal import MAGIC, WriteAheadLog, encode_record, scan_bytes


def _records(n: int) -> list[dict]:
    return [{"lsn": i + 1, "op": {"op": "send", "body": f"message-{i}"}} for i in range(n)]


class WalTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.path = Path(self._tmp.name) / "wal.log"

    def _write(self, recs: list[dict], policy: str = "none") -> None:
        wal, _, _ = WriteAheadLog.open(self.path, policy)
        for r in recs:
            wal.append(r)
        wal.close()

    def test_roundtrip(self) -> None:
        recs = _records(20)
        self._write(recs)
        wal, got, report = WriteAheadLog.open(self.path)
        wal.close()
        self.assertEqual(got, recs)
        self.assertIsNone(report.reason)
        self.assertEqual(report.discarded_bytes, 0)

    def test_every_truncation_point_recovers_a_clean_prefix(self) -> None:
        """Cut the file at EVERY byte offset: recovery must return a prefix, never garbage."""
        recs = _records(6)
        self._write(recs)
        data = self.path.read_bytes()
        boundaries = [len(MAGIC)]
        for r in recs:
            boundaries.append(boundaries[-1] + len(encode_record(r)))
        for cut in range(len(data) + 1):
            got, report = scan_bytes(data[:cut])
            complete = sum(1 for b in boundaries[1:] if b <= cut)
            self.assertEqual(got, recs[:complete], f"cut={cut}")
            self.assertEqual(report.good_bytes + report.discarded_bytes, cut)

    def test_flipped_byte_cuts_log_at_that_record(self) -> None:
        recs = _records(5)
        self._write(recs)
        data = bytearray(self.path.read_bytes())
        start_of_third = len(MAGIC) + sum(len(encode_record(r)) for r in recs[:2])
        data[start_of_third + 12] ^= 0xFF  # inside record 3's payload
        got, report = scan_bytes(bytes(data))
        self.assertEqual(got, recs[:2])  # records 4 and 5 are valid but unreachable, so dropped
        self.assertEqual(report.reason, "checksum mismatch")

    def test_open_truncates_torn_tail_and_keeps_appending(self) -> None:
        self._write(_records(4))
        with open(self.path, "ab") as fh:
            fh.write(b"\x10\x00\x00\x00\x99")  # half a header: a torn write
        wal, got, report = WriteAheadLog.open(self.path)
        self.assertEqual(len(got), 4)
        self.assertEqual(report.reason, "short header")
        self.assertTrue(self.path.with_suffix(".log.corrupt").exists())
        wal.append({"lsn": 5, "op": {}})
        wal.close()
        wal2, again, report2 = WriteAheadLog.open(self.path)
        wal2.close()
        self.assertEqual(len(again), 5)
        self.assertIsNone(report2.reason)

    def test_foreign_file_is_rejected(self) -> None:
        self.path.write_bytes(b"NOTAWAL!" + b"x" * 40)
        with self.assertRaises(CorruptLog):
            WriteAheadLog.open(self.path)

    def test_implausible_length_header_is_treated_as_corruption(self) -> None:
        self._write(_records(2))
        with open(self.path, "ab") as fh:
            fh.write(b"\xff\xff\xff\xff\x00\x00\x00\x00")
        got, report = scan_bytes(self.path.read_bytes())
        self.assertEqual(len(got), 2)
        self.assertEqual(report.reason, "implausible length")

    def test_empty_and_short_files_open_as_empty_logs(self) -> None:
        for content in (b"", b"DQW"):
            self.path.write_bytes(content)
            wal, got, _ = WriteAheadLog.open(self.path)
            wal.close()
            self.assertEqual(got, [])
            self.assertEqual(self.path.read_bytes(), MAGIC)

    def test_fsync_policies_issue_the_expected_number_of_fsyncs(self) -> None:
        for policy, expected in (("always", 10), ("none", 0)):
            self.path.unlink(missing_ok=True)
            wal, _, _ = WriteAheadLog.open(self.path, policy)
            with mock.patch("dq.wal.os.fsync") as fake:
                for r in _records(10):
                    wal.append(r)
            self.assertEqual(fake.call_count, expected, policy)
            wal.close(sync=False)

    def test_batch_policy_syncs_every_batch_size_records(self) -> None:
        wal, _, _ = WriteAheadLog.open(self.path, "batch", batch_size=5, batch_interval=3600)
        with mock.patch("dq.wal.os.fsync") as fake:
            for r in _records(12):
                wal.append(r)
        self.assertEqual(fake.call_count, 2)  # after records 5 and 10
        wal.close(sync=False)

    def test_reset_leaves_an_empty_valid_log(self) -> None:
        wal, _, _ = WriteAheadLog.open(self.path, "none")
        for r in _records(3):
            wal.append(r)
        wal.reset()
        wal.append({"lsn": 4, "op": {}})
        wal.close()
        wal2, got, _ = WriteAheadLog.open(self.path)
        wal2.close()
        self.assertEqual(got, [{"lsn": 4, "op": {}}])
        self.assertFalse(Path(str(self.path) + ".new").exists())


if __name__ == "__main__":
    unittest.main()
