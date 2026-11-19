"""Append-only write-ahead log with per-record checksums and a torn-tail rule.

File layout::

    MAGIC (8 bytes) | record | record | ...
    record = length (u32 LE) | crc32 of payload (u32 LE) | payload (JSON, UTF-8)

Recovery reads records in order and stops at the first one that is short or whose
checksum fails. Everything from that point on is cut off (and saved to a
``.corrupt`` side file). Cutting at the first bad record is deliberate: later
records were written assuming the earlier ones exist, so applying them with a hole
in front would build state that never existed.

fsync policies:
  * ``always`` - fsync after every record. Survives power loss; slowest.
  * ``batch``  - fsync every ``batch_size`` records or ``batch_interval`` seconds,
                 checked when a record is appended. Up to one batch can be lost on
                 power loss, and an idle tail stays unsynced until the next append
                 or close (there is no background flusher).
  * ``none``   - never fsync (data still reaches the OS on every append, so it
                 survives a process crash but not an OS crash or power loss).
"""

from __future__ import annotations

import json
import os
import struct
import time
import zlib
from dataclasses import dataclass
from pathlib import Path
from typing import Any, BinaryIO

from .errors import CorruptLog

MAGIC = b"DQWAL001"
HEADER = struct.Struct("<II")
MAX_RECORD_BYTES = 16 * 1024 * 1024
FSYNC_POLICIES = ("always", "batch", "none")


@dataclass
class ScanReport:
    """What recovery found in a log file."""

    records: int = 0
    good_bytes: int = 0
    discarded_bytes: int = 0
    reason: str | None = None  # why the tail was cut; None means the file was clean


def encode_record(record: dict[str, Any]) -> bytes:
    """Frame one record: length, checksum, payload."""
    payload = json.dumps(record, separators=(",", ":")).encode("utf-8")
    return HEADER.pack(len(payload), zlib.crc32(payload)) + payload


def scan_bytes(data: bytes) -> tuple[list[dict[str, Any]], ScanReport]:
    """Parse a whole log image. Returns the valid prefix of records and a report."""
    if len(data) < len(MAGIC):
        # A crash while the file was being created leaves a short or empty header.
        return [], ScanReport(good_bytes=0, discarded_bytes=len(data), reason="short magic" if data else None)
    if data[: len(MAGIC)] != MAGIC:
        raise CorruptLog("bad magic: not a durable-queue write-ahead log")
    records: list[dict[str, Any]] = []
    pos = len(MAGIC)
    reason: str | None = None
    while pos < len(data):
        if pos + HEADER.size > len(data):
            reason = "short header"
            break
        length, crc = HEADER.unpack_from(data, pos)
        if length > MAX_RECORD_BYTES:
            reason = "implausible length"
            break
        end = pos + HEADER.size + length
        if end > len(data):
            reason = "short payload"
            break
        payload = data[pos + HEADER.size : end]
        if zlib.crc32(payload) != crc:
            reason = "checksum mismatch"
            break
        try:
            records.append(json.loads(payload))
        except ValueError:
            reason = "invalid json"
            break
        pos = end
    return records, ScanReport(len(records), pos, len(data) - pos, reason)


def fsync_dir(directory: Path) -> None:
    """Make a rename durable. Windows cannot open directories, so skip there."""
    if os.name == "nt":
        return
    fd = os.open(directory, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


class WriteAheadLog:
    """An open, append-only log file."""

    def __init__(self, path: Path, fh: BinaryIO, policy: str, batch_size: int, batch_interval: float) -> None:
        self.path = path
        self._fh = fh
        self.policy = policy
        self.batch_size = batch_size
        self.batch_interval = batch_interval
        self._unsynced = 0
        self._last_sync = time.monotonic()
        self.fsync_count = 0

    @classmethod
    def open(
        cls,
        path: str | os.PathLike[str],
        policy: str = "batch",
        batch_size: int = 64,
        batch_interval: float = 0.05,
    ) -> tuple["WriteAheadLog", list[dict[str, Any]], ScanReport]:
        """Open (or create) a log, repairing a torn tail. Returns (log, records, report)."""
        if policy not in FSYNC_POLICIES:
            raise ValueError(f"fsync policy must be one of {FSYNC_POLICIES}")
        path = Path(path)
        records: list[dict[str, Any]] = []
        report = ScanReport(good_bytes=len(MAGIC))
        if path.exists():
            data = path.read_bytes()
            records, report = scan_bytes(data)
            if report.discarded_bytes:
                path.with_suffix(path.suffix + ".corrupt").write_bytes(data[report.good_bytes : report.good_bytes + (1 << 20)])
                with open(path, "r+b") as fh:
                    fh.truncate(report.good_bytes)
                    fh.flush()
                    os.fsync(fh.fileno())
        if not path.exists() or path.stat().st_size < len(MAGIC):
            path.write_bytes(MAGIC)
        fh = open(path, "ab")
        return cls(path, fh, policy, batch_size, batch_interval), records, report

    def append(self, record: dict[str, Any]) -> None:
        """Write one record, then apply the fsync policy."""
        self._fh.write(encode_record(record))
        self._fh.flush()  # to the OS: survives a process crash
        if self.policy == "always":
            self._fsync()
        elif self.policy == "batch":
            self._unsynced += 1
            if self._unsynced >= self.batch_size or time.monotonic() - self._last_sync >= self.batch_interval:
                self._fsync()

    def _fsync(self) -> None:
        """Force the file to stable storage and reset the batch counters."""
        os.fsync(self._fh.fileno())
        self.fsync_count += 1
        self._unsynced = 0
        self._last_sync = time.monotonic()

    def sync(self) -> None:
        """Force everything written so far to stable storage."""
        self._fh.flush()
        self._fsync()

    def reset(self) -> None:
        """Atomically replace the log with an empty one (used after a snapshot)."""
        self._fh.close()
        tmp = self.path.with_suffix(self.path.suffix + ".new")
        with open(tmp, "wb") as fh:
            fh.write(MAGIC)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, self.path)
        fsync_dir(self.path.parent)
        self._fh = open(self.path, "ab")
        self._unsynced = 0

    def close(self, sync: bool = True) -> None:
        """Close the file. ``sync=False`` simulates a crash (no final fsync)."""
        if self._fh.closed:
            return
        if sync:
            self.sync()
        self._fh.close()
