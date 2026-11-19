"""Exception types shared by every layer. Each carries a short wire ``code``."""

from __future__ import annotations


class DQError(Exception):
    """Base class for all durable-queue errors."""

    code = "internal"


class ValidationError(DQError):
    """The request is malformed (bad argument, missing field, too large)."""

    code = "validation"


class QueueNotFound(DQError):
    """The named queue does not exist."""

    code = "queue_not_found"


class QueueConflict(DQError):
    """A queue with this name exists but with a different configuration."""

    code = "queue_conflict"


class InvalidReceipt(DQError):
    """The receipt handle is unknown or stale (message was redelivered or deleted)."""

    code = "invalid_receipt"


class QueueFull(DQError):
    """Backpressure: the queue is at ``max_depth`` and refuses new messages."""

    code = "queue_full"


class CorruptLog(DQError):
    """The write-ahead log is not a durable-queue log at all (bad magic)."""

    code = "corrupt_log"


class CorruptSnapshot(DQError):
    """The snapshot file fails its checksum; refusing to guess at state."""

    code = "corrupt_snapshot"


class RecoveryError(DQError):
    """Replaying the log failed (gap in LSNs, record that cannot be applied)."""

    code = "recovery_failed"


class BrokerFailed(DQError):
    """The broker hit an I/O error or was crashed; it is fail-stop afterwards."""

    code = "broker_failed"


class ReplicationGap(DQError):
    """A follower received a record that does not follow its last LSN."""

    code = "replication_gap"


ERRORS_BY_CODE: dict[str, type[DQError]] = {
    cls.code: cls
    for cls in (
        DQError,
        ValidationError,
        QueueNotFound,
        QueueConflict,
        InvalidReceipt,
        QueueFull,
        CorruptLog,
        CorruptSnapshot,
        RecoveryError,
        BrokerFailed,
        ReplicationGap,
    )
}
