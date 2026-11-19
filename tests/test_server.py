"""TCP server, Python client and CLI, exercised over real loopback sockets."""

import contextlib
import io
import json
import socket
import threading
import unittest

from dq import Client, Cluster
from dq.cli import main as cli_main
from dq.errors import InvalidReceipt, QueueFull, QueueNotFound, ValidationError
from dq.server import DQServer


class ServerTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.cluster = Cluster(None, nodes=["a", "b"], replicas=1)
        self.server = DQServer(self.cluster, "127.0.0.1", 0).start()
        self.host, self.port = self.server.address
        self.addCleanup(self.cluster.close)
        self.addCleanup(self.server.stop)

    def client(self) -> Client:
        c = Client(self.host, self.port)
        self.addCleanup(c.close)
        return c


class ServerTests(ServerTestCase):
    def test_binds_loopback_only_and_refuses_other_hosts(self) -> None:
        self.assertEqual(self.host, "127.0.0.1")
        for bad in ("0.0.0.0", "192.168.1.5", ""):
            with self.assertRaises(ValueError):
                DQServer(self.cluster, bad, 0)

    def test_full_message_lifecycle_over_the_network(self) -> None:
        c = self.client()
        self.assertEqual(c.ping(), "pong")
        c.create_queue("q", visibility_timeout=30)
        sent = c.send("q", "hello", dedup_id="once")
        self.assertFalse(sent["duplicate"])
        self.assertTrue(c.send("q", "hello", dedup_id="once")["duplicate"])
        msgs = c.receive("q", 5)
        self.assertEqual([m["body"] for m in msgs], ["hello"])
        self.assertEqual(c.stats("q")["in_flight"], 1)
        c.ack("q", msgs[0]["handle"])
        self.assertEqual(c.stats("q")["total"], 0)

    def test_server_errors_arrive_as_the_matching_exception_types(self) -> None:
        c = self.client()
        c.create_queue("q", max_depth=1)
        with self.assertRaises(QueueNotFound):
            c.send("nope", "x")
        with self.assertRaises(InvalidReceipt):
            c.ack("q", "a|zzz.1")
        with self.assertRaises(InvalidReceipt):
            c.ack("q", "garbage")
        with self.assertRaises(QueueFull):
            for i in range(20):  # max_depth is per node, and the 2 nodes each take 1 message
                c.send("q", str(i))
        with self.assertRaises(ValidationError):
            c.receive("q", 0)

    def test_malformed_requests_get_error_responses_and_keep_the_connection(self) -> None:
        with socket.create_connection((self.host, self.port)) as s:
            f = s.makefile("rwb")
            for raw, code in (
                (b"this is not json\n", "validation"),
                (b'[1,2,3]\n', "validation"),
                (b'{"op":"nope"}\n', "validation"),
                (b'{"op":"send","queue":"q"}\n', "validation"),
                (b'{"op":"receive","queue":"q","max":"lots"}\n', "validation"),
            ):
                f.write(raw)
                f.flush()
                resp = json.loads(f.readline())
                self.assertFalse(resp["ok"])
                self.assertEqual(resp["error"]["code"], code, raw)
            f.write(b'{"op":"ping"}\n')
            f.flush()
            self.assertEqual(json.loads(f.readline()), {"ok": True, "result": "pong"})

    def test_oversized_request_is_rejected_and_the_connection_stays_usable(self) -> None:
        with socket.create_connection((self.host, self.port)) as s:
            f = s.makefile("rwb")
            f.write(b"x" * (1024 * 1024 + 10) + b"\n" + b'{"op":"ping"}\n')
            f.flush()
            self.assertEqual(json.loads(f.readline())["error"]["code"], "validation")
            self.assertEqual(json.loads(f.readline())["result"], "pong")

    def test_many_concurrent_clients_lose_nothing(self) -> None:
        self.client().create_queue("q", visibility_timeout=60)
        errors: list[BaseException] = []

        def producer(n: int) -> None:
            try:
                c = self.client()
                for i in range(100):
                    c.send("q", f"{n}-{i}")
            except BaseException as exc:  # noqa: BLE001 - surface any failure in the main thread
                errors.append(exc)

        threads = [threading.Thread(target=producer, args=(n,)) for n in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(errors, [])
        c, got = self.client(), set()
        while True:
            batch = c.receive("q", 50)
            if not batch:
                break
            for m in batch:
                c.ack("q", m["handle"])
                got.add(m["body"])
        self.assertEqual(len(got), 800)

    def test_topology_and_node_management_over_the_wire(self) -> None:
        c = self.client()
        c.create_queue("q")
        for i in range(200):
            c.send("q", str(i))
        self.assertEqual(sorted(c.topology()["nodes"]), ["a", "b"])
        report = c.add_node("c")
        self.assertEqual(report["total"], 200)
        self.assertEqual(c.stats("q")["total"], 200)
        c.remove_node("a")
        self.assertEqual(c.stats("q")["total"], 200)


class CliTests(ServerTestCase):
    def run_cli(self, *args: str) -> tuple[int, str, str]:
        out, err = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            code = cli_main([args[0], "--port", str(self.port), *args[1:]])
        return code, out.getvalue(), err.getvalue()

    def test_send_receive_stats_and_ack_through_the_cli(self) -> None:
        self.assertEqual(self.run_cli("create", "jobs")[0], 0)
        code, out, _ = self.run_cli("send", "jobs", "hello world")
        self.assertEqual(code, 0)
        self.assertFalse(json.loads(out)["duplicate"])
        code, out, _ = self.run_cli("stats", "jobs")
        self.assertEqual(json.loads(out)["total"], 1)
        code, out, _ = self.run_cli("receive", "jobs", "--max", "3")
        msg = json.loads(out)
        self.assertEqual(msg["body"], "hello world")
        self.assertEqual(self.run_cli("ack", "jobs", msg["handle"])[0], 0)
        self.assertEqual(json.loads(self.run_cli("stats", "jobs")[1])["total"], 0)

    def test_cli_dlq_inspect_and_redrive(self) -> None:
        clock_free = self.client()
        self.run_cli("create", "work", "--max-receive", "1", "--visibility-timeout", "0.05")
        clock_free.send("work", "poison")
        self.run_cli("receive", "work")
        threading.Event().wait(0.1)  # let the 50 ms visibility timeout lapse
        self.run_cli("receive", "work")  # dead-letters it
        code, out, _ = self.run_cli("dlq", "work-dlq")
        self.assertEqual(json.loads(out)[0]["body"], "poison")
        self.assertEqual(json.loads(self.run_cli("dlq", "work-dlq", "--redrive")[1]), {"redriven": 1})

    def test_cli_reports_errors_with_nonzero_exit(self) -> None:
        code, _, err = self.run_cli("send", "missing-queue", "x")
        self.assertEqual(code, 1)
        self.assertIn("queue_not_found", err)

    def test_cli_reports_unreachable_server(self) -> None:
        with socket.socket() as s:
            s.bind(("127.0.0.1", 0))
            free_port = s.getsockname()[1]
        err = io.StringIO()
        with contextlib.redirect_stderr(err):
            code = cli_main(["stats", "q", "--port", str(free_port)])
        self.assertEqual(code, 2)
        self.assertIn("cannot reach server", err.getvalue())


class DemoTests(unittest.TestCase):
    def test_demo_runs_and_shows_the_guarantees(self) -> None:
        from dq.demo import run_demo

        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            run_demo(quick=True)
        text = out.getvalue()
        self.assertIn("extra deliveries", text)
        self.assertIn("late ack is rejected", text)
        self.assertIn("state identical to before shutdown: True", text)


if __name__ == "__main__":
    unittest.main()
