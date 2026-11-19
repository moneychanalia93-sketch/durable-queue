"""Consistent hash ring: stability, minimal movement, evenness."""

import unittest

from dq.ring import HashRing, load_distribution, moved_fraction, std_dev_fraction

KEYS = [f"key-{i}" for i in range(50_000)]


class RingTests(unittest.TestCase):
    def test_same_key_always_maps_to_same_node(self) -> None:
        a, b = HashRing(["n1", "n2", "n3"]), HashRing(["n3", "n1", "n2"])
        for k in KEYS[:2000]:
            self.assertEqual(a.get(k), b.get(k))  # independent of insertion order

    def test_empty_ring_raises(self) -> None:
        with self.assertRaises(ValueError):
            HashRing().get("x")

    def test_duplicate_add_and_unknown_remove_raise(self) -> None:
        r = HashRing(["a"])
        with self.assertRaises(ValueError):
            r.add("a")
        with self.assertRaises(ValueError):
            r.remove("zzz")

    def test_adding_a_node_moves_about_one_over_n_of_keys(self) -> None:
        for n in (2, 4, 8):
            before = HashRing([f"n{i}" for i in range(n)], vnodes=128)
            after = before.copy()
            after.add("new")
            frac = moved_fraction(before, after, KEYS)
            ideal = 1 / (n + 1)
            print(f"\n  [ring] {n}->{n + 1} nodes: moved {frac:.4f} of keys (ideal 1/(N+1) = {ideal:.4f})", end="")
            self.assertTrue(0.6 * ideal <= frac <= 1.5 * ideal, (n, frac, ideal))

    def test_keys_only_move_to_the_new_node_never_between_old_nodes(self) -> None:
        before = HashRing(["a", "b", "c", "d"])
        after = before.copy()
        after.add("e")
        for k in KEYS:
            if before.get(k) != after.get(k):
                self.assertEqual(after.get(k), "e")

    def test_removing_a_node_moves_only_that_nodes_keys(self) -> None:
        before = HashRing(["a", "b", "c", "d"])
        after = before.copy()
        after.remove("c")
        for k in KEYS:
            if before.get(k) != "c":
                self.assertEqual(before.get(k), after.get(k))
        frac = moved_fraction(before, after, KEYS)
        print(f"\n  [ring] 4->3 nodes: moved {frac:.4f} of keys (ideal 0.2500)", end="")
        self.assertTrue(0.15 <= frac <= 0.35)

    def test_add_then_remove_restores_the_original_mapping(self) -> None:
        r = HashRing(["a", "b", "c"])
        snapshot = {k: r.get(k) for k in KEYS[:3000]}
        r.add("x")
        r.remove("x")
        self.assertEqual(snapshot, {k: r.get(k) for k in KEYS[:3000]})

    def test_virtual_nodes_even_out_the_load(self) -> None:
        nodes = [f"n{i}" for i in range(8)]
        sd = {v: std_dev_fraction(load_distribution(HashRing(nodes, vnodes=v), KEYS)) for v in (1, 16, 128, 512)}
        print("\n  [ring] load std dev / mean by vnodes, 8 nodes, 50k keys: " + ", ".join(f"{v}: {s:.3f}" for v, s in sd.items()), end="")
        self.assertLess(sd[128], sd[1])
        self.assertLess(sd[512], sd[16])
        self.assertLess(sd[128], 0.15)


if __name__ == "__main__":
    unittest.main()
