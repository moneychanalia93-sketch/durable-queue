"""Clock abstraction so tests can control time instead of sleeping."""

from __future__ import annotations

import time
from typing import Protocol


class Clock(Protocol):
    """Anything with a ``now()`` that returns seconds as a float."""

    def now(self) -> float:
        """Current time in seconds."""
        ...


class SystemClock:
    """Wall-clock time. Wall time (not monotonic) because timestamps are persisted."""

    def now(self) -> float:
        """Current time in seconds."""
        return time.time()


class ManualClock:
    """A clock that only moves when told to. Used by deterministic tests."""

    def __init__(self, start: float = 1_000_000.0) -> None:
        self._t = float(start)

    def now(self) -> float:
        """Current time in seconds."""
        return self._t

    def advance(self, seconds: float) -> float:
        """Move time forward (never backward) and return the new time."""
        if seconds < 0:
            raise ValueError("time cannot go backward")
        self._t += seconds
        return self._t
