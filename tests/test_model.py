"""Property-style tests: random operation sequences must match the naive reference model."""

import random
import shutil
import tempfile
import unittest
from pathlib import Path

from dq import Broker, ManualClock
from dq.errors import InvalidReceipt, QueueFull

from .reference_model import RefBroker

QUEUES = ("std", "fifo", "capped", "dlq", "dlq2")


def _setup(real: Broker, ref: RefBroker) -> None:
    specs = [
        ("dlq", {}),
        ("dlq2", {}),
        ("std", {"max_receive": 3, "dlq": "dlq", "visibility_timeout": 10.0}),
        ("fifo", {"fifo": True, "max_receive": 2, "dlq": "dlq2", "visibility_timeout": 7.0}),
        ("capped", {"max_depth": 25, "delay": 2.0, "visibility_timeout": 5.0}),
    ]
    for name, cfg in specs:
        real.create_queue(name, **cfg)
        ref.create(name, **cfg)


def _wrap(fn):
    try:
        fn()
        return "ok"
    except InvalidReceipt:
        return "InvalidReceipt"
    except QueueFull:
        return "QueueFull"


class ModelRunner:
    """Drives the real broker and the model with the same random operations."""

    def __init__(self, seed: int, clock: ManualClock, make_broker) -> None:
        self.rnd = random.Random(seed)
        self.clock = clock
        self.make_broker = make_broker
        self.real = make_broker()
        self.ref = RefBroker()
        _setup(self.real, self.ref)
        self.handles: dict[str, list[str]] = {q: [] for q in QUEUES}  # includes stale ones on purpose
        self.n = 0

    def step(self, restart_prob: float = 0.0) -> None:
        r, rnd, now = self.rnd.random(), self.rnd, self.clock.now()
        q = rnd.choice(QUEUES)
        if r < 0.40:
            q = rnd.choice(["std", "fifo", "capped", "dlq"])
            self.n += 1
            mid, body = f"id{self.n}", f"b{self.n}"
            group = f"g{rnd.randrange(4)}" if q == "fifo" else None
            delay = rnd.choice([None, None, 0, 3, 20])
            a = _wrap(lambda: self.real.send(q, body, group=group, delay=delay, message_id=mid))
            b = self.ref.send(q, body, mid, now, group, delay)
            assert a == b, (a, b)
        elif r < 0.65:
            limit, vt = rnd.choice([1, 2, 5, 10]), rnd.choice([None, None, 1.0, 12.0])
            got = self.real.receive(q, limit, vt)
            want = self.ref.receive(q, now, limit, vt)
            assert [(m["id"], m["body"], m["handle"], m["receive_count"]) for m in got] == want
            self.handles[q].extend(m["handle"] for m in got)
        elif r < 0.78:
            h = self._pick(q)
            assert _wrap(lambda: self.real.ack(q, h)) == self.ref.ack(q, h)
        elif r < 0.84:
            h, d = self._pick(q), rnd.choice([0.0, 1.0, 9.5])
            assert _wrap(lambda: self.real.nack(q, h, delay=d)) == self.ref.nack(q, h, now, d)
        elif r < 0.89:
            h, vt = self._pick(q), rnd.choice([0.0, 4.0, 30.0])
            assert _wrap(lambda: self.real.change_visibility(q, h, vt)) == self.ref.extend(q, h, now, vt)
        elif r < 0.97:
            self.clock.advance(rnd.choice([0.5, 1.0, 3.0, 8.0, 15.0]))
        elif r < 0.99:
            d = rnd.choice(["dlq", "dlq2"])
            assert self.real.redrive(d) == self.ref.redrive(d, now)
        if restart_prob and rnd.random() < restart_prob:
            self.real.close()
            self.real = self.make_broker()
        self.check()

    def _pick(self, q: str) -> str:
        pool = self.handles[q]
        return self.rnd.choice(pool) if pool and self.rnd.random() < 0.9 else "nothere.1"

    def check(self) -> None:
        now = self.clock.now()
        for q in QUEUES:
            s = self.real.stats(q)
            want = self.ref.stats(q, now)
            assert {k: s[k] for k in want} == want, (q, s, want)

    def check_contents(self) -> None:
        for q in QUEUES:
            got = [(m["id"], m["body"], m["receive_count"]) for m in self.real.peek(q, 10_000)]
            assert got == self.ref.contents(q), q


class ModelTests(unittest.TestCase):
    def test_in_memory_broker_matches_reference_model_over_many_seeds(self) -> None:
        for seed in range(60):
            clock = ManualClock()
            run = ModelRunner(seed, clock, lambda c=clock: Broker(None, clock=c, rng=random.Random(0)))
            for i in range(400):
                run.step()
                if i % 50 == 0:
                    run.check_contents()
            run.check_contents()

    def test_durable_broker_matches_model_even_with_random_restarts(self) -> None:
        for seed in range(100, 108):
            tmp = tempfile.mkdtemp()
            self.addCleanup(shutil.rmtree, tmp, True)
            clock = ManualClock()
            made: list[Broker] = []

            def make(c=clock, d=Path(tmp)) -> Broker:
                b = Broker(d, clock=c, fsync="none", snapshot_every=40, rng=random.Random(0))
                made.append(b)
                return b

            run = ModelRunner(seed, clock, make)
            for i in range(300):
                run.step(restart_prob=0.03)
                if i % 60 == 0:
                    run.check_contents()
            run.check_contents()
            for b in made:
                b.close()

    def test_runner_actually_exercises_dead_lettering_and_redrive(self) -> None:
        """Guard against a vacuous model test: the random runs must reach the interesting states."""
        seen_dlq = seen_stale = 0
        for seed in range(10):
            clock = ManualClock()
            run = ModelRunner(seed, clock, lambda c=clock: Broker(None, clock=c, rng=random.Random(0)))
            for _ in range(400):
                run.step()
            seen_dlq += run.real.stats("dlq")["total"] + run.real.stats("dlq2")["total"]
            seen_stale += sum(len(v) for v in run.handles.values())
        self.assertGreater(seen_dlq, 0)
        self.assertGreater(seen_stale, 100)


if __name__ == "__main__":
    unittest.main()
