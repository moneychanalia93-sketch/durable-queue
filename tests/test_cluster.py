"""Partitioned cluster: routing, rebalancing, replication, failover."""

import shutil
import tempfile
import unittest
from collections import defaultdict
from pathlib import Path

from dq import Cluster, ManualClock
from dq.errors import BrokerFailed, InvalidReceipt, ValidationError


def drain(cluster: Cluster, queue: str, batch: int = 50) -> list[dict]:
    """Receive and ack everything currently visible; return the messages in receive order."""
    out: list[dict] = []
    while True:
        got = cluster.receive(queue, batch)
        if not got:
            return out
        for m in got:
            cluster.ack(queue, m["handle"])
        out.extend(got)


class ClusterTests(unittest.TestCase):
    def setUp(self) -> None:
        self.clock = ManualClock()

    def make(self, nodes=("a", "b", "c"), **kw) -> Cluster:
        c = Cluster(nodes=list(nodes), clock=self.clock, seed=11, **kw)
        self.addCleanup(c.close)
        return c

    def test_a_group_always_lands_on_one_node(self) -> None:
        c = self.make()
        c.create_queue("f", fifo=True)
        nodes = {c.send("f", str(i), group="order-42")["node"] for i in range(30)}
        self.assertEqual(len(nodes), 1)

    def test_ungrouped_messages_spread_over_all_nodes_and_are_all_delivered(self) -> None:
        c = self.make()
        c.create_queue("q")
        for i in range(600):
            c.send("q", str(i))
        per_node = {n: s["total"] for n, s in c.stats("q")["nodes"].items()}
        self.assertTrue(all(v > 100 for v in per_node.values()), per_node)
        self.assertEqual(sorted(int(m["body"]) for m in drain(c, "q")), list(range(600)))

    def test_add_node_moves_about_one_over_n_and_loses_nothing(self) -> None:
        c = self.make(("a", "b", "c", "d"))
        c.create_queue("q")
        for i in range(4000):
            c.send("q", str(i))
        report = c.add_node("e")
        print(f"\n  [cluster] add 5th node: moved {report['moved']}/{report['total']} messages = {report['fraction']:.4f} (ideal 0.2000)", end="")
        self.assertTrue(0.12 <= report["fraction"] <= 0.30, report)
        self.assertEqual(c.stats("q")["total"], 4000)
        self.assertGreater(c.stats("q")["nodes"]["e"]["total"], 0)
        self.assertEqual(sorted(int(m["body"]) for m in drain(c, "q")), list(range(4000)))

    def test_remove_node_drains_it_and_loses_nothing(self) -> None:
        c = self.make()
        c.create_queue("q")
        for i in range(900):
            c.send("q", str(i))
        report = c.remove_node("b")
        self.assertGreater(report["moved"], 0)
        self.assertEqual(list(c.stats("q")["nodes"]), ["a", "c"])
        self.assertEqual(len(drain(c, "q")), 900)
        with self.assertRaises(ValidationError):
            c.remove_node("zzz")

    def test_cannot_remove_last_node_or_add_duplicate(self) -> None:
        c = self.make(("a",))
        with self.assertRaises(ValidationError):
            c.remove_node("a")
        with self.assertRaises(ValidationError):
            c.add_node("a")
        with self.assertRaises(ValidationError):
            c.add_node("bad|name")

    def test_fifo_order_per_group_survives_rebalancing(self) -> None:
        c = self.make()
        c.create_queue("f", fifo=True)
        for i in range(40):
            for g in range(25):
                c.send("f", f"{g}:{i}", group=f"g{g}")
        c.add_node("d")
        c.remove_node("a")
        seen: dict[str, list[int]] = defaultdict(list)
        for m in drain(c, "f", 10):
            g, i = m["body"].split(":")
            seen[g].append(int(i))
        self.assertEqual(len(seen), 25)
        for g, order in seen.items():
            self.assertEqual(order, list(range(40)), g)

    def test_in_flight_messages_become_visible_on_move_and_old_handles_die(self) -> None:
        c = self.make(("a", "b"))
        c.create_queue("q", visibility_timeout=100)
        for i in range(200):
            c.send("q", str(i))
        held = c.receive("q", 200)
        self.assertEqual(len(held), 200)
        report = c.add_node("c")
        self.assertGreater(report["moved"], 0)
        redelivered = c.receive("q", 200)
        self.assertEqual(len(redelivered), report["moved"])  # exactly the moved ones came back
        moved_ids = {m["id"] for m in redelivered}
        for m in held:
            if m["id"] in moved_ids:
                with self.assertRaises(InvalidReceipt):
                    c.ack("q", m["handle"])
            else:
                c.ack("q", m["handle"])  # untouched messages keep their handles

    def test_handle_of_removed_node_is_invalid(self) -> None:
        c = self.make(("a", "b"))
        c.create_queue("q")
        for i in range(50):
            c.send("q", str(i))
        handles = [m["handle"] for m in c.receive("q", 50)]
        c.remove_node("b")
        outcomes = set()
        for h in handles:
            try:
                c.ack("q", h)
                outcomes.add("ok")
            except InvalidReceipt:
                outcomes.add("invalid")
        self.assertEqual(outcomes, {"ok", "invalid"})

    def test_dead_letter_queue_works_across_nodes(self) -> None:
        c = self.make()
        c.create_queue("dlq")
        c.create_queue("q", max_receive=1, dlq="dlq", visibility_timeout=5)
        for i in range(60):
            c.send("q", str(i))
        c.receive("q", 60)
        self.clock.advance(6)
        self.assertEqual(c.receive("q", 60), [])
        self.assertEqual(c.stats("dlq")["total"], 60)
        self.assertEqual(len(c.peek("dlq", 100)), 60)
        self.assertEqual(c.redrive("dlq"), 60)
        self.assertEqual(len(drain(c, "q", 100)), 60)

    def test_added_node_gets_queue_definitions_including_dlq_dependencies(self) -> None:
        c = self.make(("a",))
        c.create_queue("dlq")
        c.create_queue("q", max_receive=2, dlq="dlq")
        c.add_node("b")
        self.assertEqual({q["name"] for q in c.list_queues()}, {"dlq", "q"})
        c.send("q", "x", group="g")
        self.assertEqual(c.stats("q")["total"], 1)


class ReplicationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.clock = ManualClock()

    def _tmpdir(self) -> Path:
        d = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, d, True)  # registered first, so it runs after the closes
        return Path(d)

    def make(self, path=None, **kw) -> Cluster:
        c = Cluster(path, nodes=["a", "b"], replicas=1, clock=self.clock, seed=5, fsync="none", **kw)
        self.addCleanup(c.close)
        return c

    def _workload(self, c: Cluster) -> None:
        c.create_queue("dlq")
        c.create_queue("q", max_receive=3, dlq="dlq", visibility_timeout=20)
        for i in range(100):
            c.send("q", str(i))
        got = c.receive("q", 60)
        for m in got[:30]:
            c.ack("q", m["handle"])
        for m in got[30:40]:
            c.nack("q", m["handle"])
        self.clock.advance(25)
        c.receive("q", 20)

    def test_follower_state_is_identical_to_leader_state(self) -> None:
        c = self.make()
        self._workload(c)
        for name in ("a", "b"):
            node = c.node(name)
            self.assertEqual(node.leader.export_state(), node.followers[0].export_state())

    def test_failover_loses_no_acknowledged_work_and_keeps_in_flight_handles(self) -> None:
        c = self.make()
        c.create_queue("q", visibility_timeout=100)
        for i in range(300):
            c.send("q", str(i))
        held = c.receive("q", 100)
        acked = {m["body"] for m in held[:50]}
        for m in held[:50]:
            c.ack("q", m["handle"])
        for name in ("a", "b"):
            c.node(name).fail_leader()
        # The very next calls hit dead leaders, fail over, and carry on.
        for m in held[50:]:
            c.ack("q", m["handle"])  # handles issued by the old leaders still work
        acked |= {m["body"] for m in held[50:]}
        rest = drain(c, "q", 100)
        self.assertEqual(acked | {m["body"] for m in rest}, {str(i) for i in range(300)})
        self.assertEqual(len(rest) + len(acked), 300)  # nothing duplicated or lost
        self.assertEqual(c.node("a").failovers + c.node("b").failovers, 2)

    def test_without_a_follower_a_dead_leader_stays_dead(self) -> None:
        c = Cluster(None, nodes=["solo"], replicas=0, clock=self.clock)
        c.create_queue("q")
        c.node("solo").fail_leader()
        with self.assertRaises(BrokerFailed):
            c.send("q", "x")

    def test_a_fresh_follower_catches_up_from_the_leader(self) -> None:
        c = self.make()
        self._workload(c)
        c.node("a").fail_leader()
        c.stats("q")  # triggers failover; node a now has no follower
        self.assertEqual(len(c.node("a").followers), 0)
        c.node("a").add_follower()
        node = c.node("a")
        self.assertEqual(node.leader.export_state(), node.followers[0].export_state())
        node.leader.send("q", "later")  # new commands now reach the new follower too
        self.assertEqual(node.leader.export_state(), node.followers[0].export_state())

    def test_second_failure_after_failover_with_new_follower(self) -> None:
        c = self.make()
        c.create_queue("q")
        for i in range(100):
            c.send("q", str(i))
        node = c.node("a")
        node.fail_leader()
        c.stats("q")
        node.add_follower()
        node.fail_leader()  # the promoted leader dies too
        self.assertEqual(c.stats("q")["total"], 100)
        self.assertEqual(node.failovers, 2)

    def test_cluster_restarts_from_disk_with_topology_and_data(self) -> None:
        tmp = self._tmpdir()
        c = Cluster(tmp, nodes=["a", "b"], replicas=1, clock=self.clock, fsync="none")
        c.create_queue("q")
        for i in range(200):
            c.send("q", str(i))
        c.add_node("c")
        c.close()
        c2 = Cluster(tmp, clock=self.clock, fsync="none")  # topology comes from cluster.json
        self.addCleanup(c2.close)
        self.assertEqual(sorted(c2.topology()["nodes"]), ["a", "b", "c"])
        self.assertEqual(c2.stats("q")["total"], 200)
        self.assertEqual(len(drain(c2, "q")), 200)

    def test_restart_after_failover_picks_the_replica_with_the_most_data(self) -> None:
        tmp = self._tmpdir()
        c = Cluster(tmp, nodes=["a"], replicas=1, clock=self.clock, fsync="none")
        c.create_queue("q")
        for i in range(50):
            c.send("q", str(i))
        c.node("a").fail_leader()
        for i in range(50, 80):
            c.send("q", str(i))  # served by the promoted follower; old leader's disk is stale
        c.close()
        c2 = Cluster(tmp, clock=self.clock, fsync="none")
        self.addCleanup(c2.close)
        self.assertEqual(c2.stats("q")["total"], 80)
        node = c2.node("a")
        self.assertEqual(node.leader.export_state(), node.followers[0].export_state())


if __name__ == "__main__":
    unittest.main()
