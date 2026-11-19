"""Consistent-hash ring with virtual nodes.

Each physical node is placed on a 64-bit ring at ``vnodes`` pseudo-random points.
A key belongs to the first point clockwise from its own hash. Adding a node only
steals the arcs just before its new points, so about 1/N of keys move; removing a
node hands only its own arcs to its neighbours.
"""

from __future__ import annotations

import hashlib
import math
from bisect import bisect_right
from typing import Iterable, Sequence


def hash64(text: str) -> int:
    """Stable 64-bit hash (BLAKE2b). Python's built-in ``hash`` is salted per process."""
    return int.from_bytes(hashlib.blake2b(text.encode("utf-8"), digest_size=8).digest(), "big")


class HashRing:
    """Maps string keys to node names."""

    def __init__(self, nodes: Iterable[str] = (), vnodes: int = 128) -> None:
        if vnodes < 1:
            raise ValueError("vnodes must be >= 1")
        self.vnodes = vnodes
        self._nodes: set[str] = set()
        self._points: list[tuple[int, str]] = []
        self._hashes: list[int] = []
        for node in nodes:
            self.add(node)

    @property
    def nodes(self) -> list[str]:
        """Node names on the ring, sorted."""
        return sorted(self._nodes)

    def __len__(self) -> int:
        return len(self._nodes)

    def add(self, node: str) -> None:
        """Place ``node`` on the ring. Re-adding an existing node is an error."""
        if node in self._nodes:
            raise ValueError(f"node {node!r} already on ring")
        self._nodes.add(node)
        self._points.extend((hash64(f"{node}#{i}"), node) for i in range(self.vnodes))
        self._rebuild()

    def remove(self, node: str) -> None:
        """Take ``node`` off the ring."""
        if node not in self._nodes:
            raise ValueError(f"node {node!r} not on ring")
        self._nodes.discard(node)
        self._points = [p for p in self._points if p[1] != node]
        self._rebuild()

    def _rebuild(self) -> None:
        """Re-sort the ring points after a change."""
        self._points.sort()
        self._hashes = [h for h, _ in self._points]

    def get(self, key: str) -> str:
        """Return the node that owns ``key``."""
        if not self._points:
            raise ValueError("ring is empty")
        idx = bisect_right(self._hashes, hash64(key)) % len(self._points)
        return self._points[idx][1]

    def copy(self) -> "HashRing":
        """An independent copy (used to compute a ring before committing a change)."""
        other = HashRing(vnodes=self.vnodes)
        other._nodes = set(self._nodes)
        other._points = list(self._points)
        other._hashes = list(self._hashes)
        return other


def load_distribution(ring: HashRing, keys: Sequence[str]) -> dict[str, int]:
    """Count how many of ``keys`` each node owns."""
    counts = {n: 0 for n in ring.nodes}
    for k in keys:
        counts[ring.get(k)] += 1
    return counts


def std_dev_fraction(counts: dict[str, int]) -> float:
    """Population std dev of per-node load, as a fraction of the mean load."""
    values = list(counts.values())
    mean = sum(values) / len(values)
    var = sum((v - mean) ** 2 for v in values) / len(values)
    return math.sqrt(var) / mean


def moved_fraction(before: HashRing, after: HashRing, keys: Sequence[str]) -> float:
    """Fraction of ``keys`` whose owner differs between two rings."""
    moved = sum(1 for k in keys if before.get(k) != after.get(k))
    return moved / len(keys)
