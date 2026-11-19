"""A small readers-writer lock (many readers OR one writer, writers not starved)."""

from __future__ import annotations

import threading
from contextlib import contextmanager
from typing import Iterator


class RWLock:
    """Normal queue operations take ``read()``; topology changes take ``write()``."""

    def __init__(self) -> None:
        self._cond = threading.Condition()
        self._readers = 0
        self._writer = False
        self._writers_waiting = 0

    @contextmanager
    def read(self) -> Iterator[None]:
        """Shared access: many readers at once, blocked while a writer waits or runs."""
        with self._cond:
            while self._writer or self._writers_waiting:
                self._cond.wait()
            self._readers += 1
        try:
            yield
        finally:
            with self._cond:
                self._readers -= 1
                if self._readers == 0:
                    self._cond.notify_all()

    @contextmanager
    def write(self) -> Iterator[None]:
        """Exclusive access: waits for readers to leave, blocks new ones."""
        with self._cond:
            self._writers_waiting += 1
            while self._writer or self._readers:
                self._cond.wait()
            self._writers_waiting -= 1
            self._writer = True
        try:
            yield
        finally:
            with self._cond:
                self._writer = False
                self._cond.notify_all()
