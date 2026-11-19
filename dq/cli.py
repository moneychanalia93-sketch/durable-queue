"""Command line: ``python -m dq serve | create | send | receive | ack | stats | dlq | bench | demo``."""

from __future__ import annotations

import argparse
import json
import sys
from typing import Any, Sequence

from .client import Client
from .errors import DQError

DEFAULT_PORT = 7878


def _print(obj: Any) -> None:
    """Print a result as JSON (pretty for stats, one line otherwise)."""
    print(json.dumps(obj, indent=2 if isinstance(obj, dict) and "queue" in obj else None))


def _connection_args() -> argparse.ArgumentParser:
    """Options shared by every command that talks to a server."""
    p = argparse.ArgumentParser(add_help=False)
    p.add_argument("--host", default="127.0.0.1", help="server host (default 127.0.0.1)")
    p.add_argument("--port", type=int, default=DEFAULT_PORT, help=f"server port (default {DEFAULT_PORT})")
    return p


def build_parser() -> argparse.ArgumentParser:
    """Define every sub-command and its options."""
    conn = _connection_args()
    parser = argparse.ArgumentParser(prog="dq", description="durable-queue: an SQS-style message queue")
    sub = parser.add_subparsers(dest="cmd", required=True)

    s = sub.add_parser("serve", help="run the server (loopback only)")
    s.add_argument("--dir", default="dq-data", help="data directory (default ./dq-data)")
    s.add_argument("--nodes", type=int, default=3, help="number of in-process nodes for a NEW data dir (default 3)")
    s.add_argument("--replicas", type=int, default=0, choices=(0, 1), help="followers per node for a NEW data dir")
    s.add_argument("--fsync", default="batch", choices=("always", "batch", "none"))
    s.add_argument("--host", default="127.0.0.1")
    s.add_argument("--port", type=int, default=DEFAULT_PORT)

    s = sub.add_parser("create", parents=[conn], help="create a queue")
    s.add_argument("queue")
    s.add_argument("--fifo", action="store_true")
    s.add_argument("--visibility-timeout", type=float, default=30.0)
    s.add_argument("--delay", type=float, default=0.0)
    s.add_argument("--max-receive", type=int, default=0, help="dead-letter after this many receives (auto-creates <queue>-dlq)")
    s.add_argument("--max-depth", type=int, default=0, help="refuse sends beyond this many messages (backpressure)")

    s = sub.add_parser("send", parents=[conn], help="send one message")
    s.add_argument("queue")
    s.add_argument("body")
    s.add_argument("--group")
    s.add_argument("--delay", type=float)
    s.add_argument("--dedup-id")

    s = sub.add_parser("receive", parents=[conn], help="receive messages")
    s.add_argument("queue")
    s.add_argument("--max", type=int, default=1)
    s.add_argument("--visibility-timeout", type=float)
    s.add_argument("--ack", action="store_true", help="delete each message after printing it")

    s = sub.add_parser("ack", parents=[conn], help="delete a received message")
    s.add_argument("queue")
    s.add_argument("handle")

    s = sub.add_parser("stats", parents=[conn], help="queue counts per node")
    s.add_argument("queue")

    s = sub.add_parser("dlq", parents=[conn], help="inspect or redrive a dead-letter queue")
    s.add_argument("queue", help="the dead-letter queue name")
    s.add_argument("--redrive", action="store_true", help="move its messages back to their source queue")

    s = sub.add_parser("bench", help="run benchmarks and print measured numbers")
    s.add_argument("--quick", action="store_true", help="smaller runs (numbers are noisier)")
    s.add_argument("--json", metavar="PATH", help="also write raw results as JSON")

    s = sub.add_parser("demo", help="narrated tour of the guarantees")
    s.add_argument("--quick", action="store_true")
    return parser


def _serve(args: argparse.Namespace) -> int:
    """Run the TCP server until Ctrl+C, then close the cluster cleanly."""
    from .cluster import Cluster
    from .server import DQServer

    cluster = Cluster(args.dir, nodes=[f"n{i + 1}" for i in range(args.nodes)], replicas=args.replicas, fsync=args.fsync)
    server = DQServer(cluster, args.host, args.port)
    host, port = server.address
    print(f"dq serving on {host}:{port}  data={args.dir}  nodes={cluster.ring.nodes}  fsync={args.fsync}  (Ctrl+C to stop)", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nshutting down")
    finally:
        server.server_close()
        cluster.close()
    return 0


def _client_command(args: argparse.Namespace) -> int:
    """Run one command against a running server."""
    with Client(args.host, args.port) as c:
        if args.cmd == "create":
            cfg: dict[str, Any] = {"fifo": args.fifo, "visibility_timeout": args.visibility_timeout, "delay": args.delay, "max_depth": args.max_depth}
            if args.max_receive:
                dlq = f"{args.queue}-dlq"
                c.create_queue(dlq)
                cfg.update(max_receive=args.max_receive, dlq=dlq)
            _print({"created": c.create_queue(args.queue, **cfg)})
        elif args.cmd == "send":
            _print(c.send(args.queue, args.body, group=args.group, delay=args.delay, dedup_id=args.dedup_id))
        elif args.cmd == "receive":
            for m in c.receive(args.queue, args.max, args.visibility_timeout):
                _print(m)
                if args.ack:
                    c.ack(args.queue, m["handle"])
        elif args.cmd == "ack":
            _print(c.ack(args.queue, args.handle))
        elif args.cmd == "stats":
            _print(c.stats(args.queue))
        elif args.cmd == "dlq":
            _print({"redriven": c.redrive(args.queue)} if args.redrive else c.peek(args.queue))
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    """Entry point. Returns the process exit code (0 ok, 1 server error, 2 unreachable)."""
    args = build_parser().parse_args(argv)
    try:
        if args.cmd == "serve":
            return _serve(args)
        if args.cmd == "bench":
            from .bench import main as bench_main

            return bench_main(args.quick, args.json)
        if args.cmd == "demo":
            from .demo import run_demo

            run_demo(args.quick)
            return 0
        return _client_command(args)
    except DQError as exc:
        print(f"error [{exc.code}]: {exc}", file=sys.stderr)
        return 1
    except (ConnectionError, OSError) as exc:
        print(f"error: cannot reach server ({exc}). Is `python -m dq serve` running?", file=sys.stderr)
        return 2
