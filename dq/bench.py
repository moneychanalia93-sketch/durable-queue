"""Benchmarks. Every number printed here is measured on the machine running it.

What is measured:
  * thread scaling      - in-process broker, fsync=batch, 1/4/16 threads
  * fsync policy cost   - 1 thread, memory-only vs none vs batch vs always
  * over the network    - TCP server + client, 1 and 4 connections
Latency percentiles are per operation, measured with ``perf_counter`` around the
call. "recv+ack" is one receive of 1 message followed by its ack.

Python threads share one interpreter lock, so more threads do not mean more CPU for
this pure-Python code; the thread rows show contention behaviour, not scaling.
"""

from __future__ import annotations

import json
import os
import platform
import shutil
import sys
import tempfile
import threading
import time
from typing import Any, Callable

from .broker import Broker
from .cluster import Cluster
from .client import Client
from .server import DQServer

BODY = "x" * 100  # 100-byte synthetic payload


def percentile(sorted_values: list[float], p: float) -> float:
    """Nearest-rank percentile of an already sorted list."""
    if not sorted_values:
        return 0.0
    rank = max(1, -(-len(sorted_values) * p // 100))  # ceil
    return sorted_values[int(rank) - 1]


def summarize(latencies: list[float], seconds: float, count: int) -> dict[str, float]:
    """Throughput and latency percentiles (milliseconds) for one measured phase."""
    lat = sorted(latencies)
    return {
        "ops": count,
        "seconds": round(seconds, 3),
        "per_sec": round(count / seconds, 1),
        "p50_ms": round(percentile(lat, 50) * 1000, 4),
        "p95_ms": round(percentile(lat, 95) * 1000, 4),
        "p99_ms": round(percentile(lat, 99) * 1000, 4),
    }


def _run_threads(n_threads: int, work: Callable[[int, list[float]], None]) -> tuple[list[float], float]:
    """Run ``work(thread_index, latency_list)`` on n threads; return all latencies and wall time."""
    lists: list[list[float]] = [[] for _ in range(n_threads)]
    errors: list[BaseException] = []
    barrier = threading.Barrier(n_threads + 1)

    def runner(i: int) -> None:
        """Wait for the start barrier, then run this thread's share of the work."""
        barrier.wait()
        try:
            work(i, lists[i])
        except BaseException as exc:  # noqa: BLE001 - a failed worker must fail the benchmark, not vanish
            errors.append(exc)

    threads = [threading.Thread(target=runner, args=(i,)) for i in range(n_threads)]
    for t in threads:
        t.start()
    barrier.wait()
    start = time.perf_counter()
    for t in threads:
        t.join()
    elapsed = time.perf_counter() - start
    if errors:
        raise errors[0]
    return [x for lst in lists for x in lst], elapsed


def _measure(
    send: Callable[[int], Any], recv: Callable[[int], list[Any]], ack: Callable[[int, Any], Any], threads: int, n: int
) -> dict[str, Any]:
    """Send ``n`` messages from ``threads`` threads, then receive+ack them all.

    The callables take the worker index so each worker can use its own connection.
    """
    per = n // threads
    total = per * threads

    def sender(i: int, lat: list[float]) -> None:
        """Send this thread's share of messages, timing each call."""
        for _ in range(per):
            t0 = time.perf_counter()
            send(i)
            lat.append(time.perf_counter() - t0)

    lat_s, sec_s = _run_threads(threads, sender)
    done = [0]
    lock = threading.Lock()

    def consumer(i: int, lat: list[float]) -> None:
        """Receive and ack until every message has been handled, timing each cycle."""
        while True:
            with lock:
                if done[0] >= total:
                    return
            t0 = time.perf_counter()
            got = recv(i)
            if not got:
                continue
            ack(i, got[0])
            lat.append(time.perf_counter() - t0)
            with lock:
                done[0] += 1

    lat_r, sec_r = _run_threads(threads, consumer)
    return {"threads": threads, "send": summarize(lat_s, sec_s, total), "recv_ack": summarize(lat_r, sec_r, len(lat_r))}


def _broker_run(fsync: str | None, threads: int, n: int) -> dict[str, Any]:
    """Benchmark an in-process broker; ``fsync=None`` means memory only (no log)."""
    tmp = tempfile.mkdtemp(prefix="dqbench-") if fsync else None
    try:
        b = Broker(tmp, fsync=fsync or "none", snapshot_every=0)
        b.create_queue("bench", visibility_timeout=600)
        out = _measure(
            lambda i: b.send("bench", BODY),
            lambda i: b.receive("bench", 1),
            lambda i, m: b.ack("bench", m["handle"]),
            threads,
            n,
        )
        b.close()
        return out
    finally:
        if tmp:
            shutil.rmtree(tmp, ignore_errors=True)


def _network_run(threads: int, n: int) -> dict[str, Any]:
    """Benchmark through the TCP server with one client connection per thread."""
    tmp = tempfile.mkdtemp(prefix="dqbench-")
    cluster = Cluster(tmp, nodes=["n1"], fsync="batch", snapshot_every=0)
    server = DQServer(cluster, "127.0.0.1", 0).start()
    clients = [Client(*server.address) for _ in range(threads)]
    try:
        clients[0].create_queue("bench", visibility_timeout=600)
        return _measure(
            lambda i: clients[i].send("bench", BODY),
            lambda i: clients[i].receive("bench", 1),
            lambda i, m: clients[i].ack("bench", m["handle"]),
            threads,
            n,
        )
    finally:
        for c in clients:
            c.close()
        server.stop()
        cluster.close()
        shutil.rmtree(tmp, ignore_errors=True)


def machine_info() -> dict[str, str]:
    """Describe the machine and interpreter the numbers came from."""
    return {
        "python": sys.version.split()[0],
        "implementation": platform.python_implementation(),
        "platform": platform.platform(),
        "processor": platform.processor(),
        "logical_cpus": str(os.cpu_count()),
    }


def run_bench(quick: bool = False) -> dict[str, Any]:
    """Run every benchmark section and return the results as a dict."""
    n = 2_000 if quick else 20_000
    n_always = 200 if quick else 1_000
    n_net = 500 if quick else 3_000
    result: dict[str, Any] = {"machine": machine_info(), "messages": {"threads": n, "fsync_always": n_always, "network": n_net}}
    result["thread_scaling_fsync_batch"] = [_broker_run("batch", t, n) for t in (1, 4, 16)]
    fsync_rows = []
    for label, policy, count in (("memory (no log)", None, n), ("none", "none", n), ("batch", "batch", n), ("always", "always", n_always)):
        row = _broker_run(policy, 1, count)
        row["policy"] = label
        fsync_rows.append(row)
    result["fsync_policy_1_thread"] = fsync_rows
    result["network_fsync_batch"] = [_network_run(t, n_net) for t in (1, 4)]
    return result


def format_report(r: dict[str, Any]) -> str:
    """Render results as Markdown tables."""
    m = r["machine"]
    lines = [
        f"Machine: {m['processor']} | {m['logical_cpus']} logical CPUs | {m['platform']} | {m['implementation']} {m['python']}",
        f"Messages per run: {r['messages']}",
        "",
    ]

    def table(title: str, label: str, rows: list[dict[str, Any]], key: Callable[[dict[str, Any]], str]) -> None:
        """Append one Markdown table to the report."""
        lines.append(f"### {title}")
        lines.append(f"| {label} | send msg/s | send p50/p95/p99 ms | recv+ack msg/s | recv+ack p50/p95/p99 ms |")
        lines.append("|---|---:|---|---:|---|")
        for row in rows:
            s, a = row["send"], row["recv_ack"]
            lines.append(
                f"| {key(row)} | {s['per_sec']:,.0f} | {s['p50_ms']:.3f} / {s['p95_ms']:.3f} / {s['p99_ms']:.3f} "
                f"| {a['per_sec']:,.0f} | {a['p50_ms']:.3f} / {a['p95_ms']:.3f} / {a['p99_ms']:.3f} |"
            )
        lines.append("")

    table("Thread scaling (in-process, fsync=batch)", "threads", r["thread_scaling_fsync_batch"], lambda x: str(x["threads"]))
    table("fsync policy cost (in-process, 1 thread)", "policy", r["fsync_policy_1_thread"], lambda x: x["policy"])
    table("Over TCP on loopback (fsync=batch)", "connections", r["network_fsync_batch"], lambda x: str(x["threads"]))
    return "\n".join(lines)


def main(quick: bool = False, json_path: str | None = None) -> int:
    """Run, print and optionally save the benchmark. Returns a process exit code."""
    print("running benchmarks (this takes a little while)...", flush=True)
    result = run_bench(quick)
    print(format_report(result))
    if json_path:
        with open(json_path, "w", encoding="utf-8") as fh:
            json.dump(result, fh, indent=2)
        print(f"raw results written to {json_path}")
    return 0
