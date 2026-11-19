# Code map

One line per file.

## Package `dq/`

| File | What it does |
|---|---|
| `dq/__init__.py` | Public imports (`Broker`, `Cluster`, `Client`, ...) and the version |
| `dq/__main__.py` | Lets you run `python -m dq` |
| `dq/clock.py` | `SystemClock` and `ManualClock` so tests control time instead of sleeping |
| `dq/errors.py` | Exception types, each with a short wire `code` |
| `dq/engine.py` | The queue state machine: send, receive, ack, nack, DLQ, FIFO groups, dedup (pure, no I/O) |
| `dq/wal.py` | Write-ahead log: checksummed records, fsync policies, torn-tail repair |
| `dq/broker.py` | One durable node: engine + log + snapshots + crash recovery + follower shipping |
| `dq/ring.py` | Consistent-hash ring with virtual nodes and helpers to measure movement and evenness |
| `dq/rwlock.py` | Readers-writer lock: normal operations share it, topology changes take it exclusively |
| `dq/cluster.py` | Partitioned, replicated cluster: routing, add/remove node, failover |
| `dq/server.py` | TCP server (JSON lines, loopback only) in front of a cluster |
| `dq/client.py` | Python client for that server |
| `dq/cli.py` | `python -m dq serve / create / send / receive / ack / stats / dlq / bench / demo` |
| `dq/bench.py` | Benchmarks: thread scaling, fsync policy cost, over TCP, with latency percentiles |
| `dq/demo.py` | Narrated, deterministic tour of the guarantees |

## Tests `tests/` (run with `python -m unittest`)

| File | What it proves |
|---|---|
| `tests/reference_model.py` | A deliberately naive queue used as the oracle for random tests (not a test itself) |
| `tests/test_wal.py` | Framing, checksums, truncation at every byte offset, fsync policy behaviour |
| `tests/test_engine.py` | Visibility timeout, stale handles, delay, backoff, DLQ, redrive, FIFO, dedup, backpressure |
| `tests/test_durability.py` | Recovery, snapshots, crash between snapshot and log reset, corruption, fail-stop |
| `tests/test_ring.py` | Stability, ~1/N key movement (measured), evenness vs virtual nodes |
| `tests/test_cluster.py` | Routing, rebalancing without loss, FIFO order across moves, replication, failover, restart |
| `tests/test_model.py` | Random operation sequences must match the reference model (with and without restarts) |
| `tests/test_concurrency.py` | Threaded stress with crashing consumers, seeded chaos, idempotent consumer |
| `tests/test_server.py` | Real TCP sockets, malformed input, concurrent clients, the CLI, the demo |

## Repository files

| File | What it is |
|---|---|
| `README.md` | What it is, quickstart, architecture, guarantees, measured benchmarks, limitations |
| `DESIGN.md` | Decisions and trade-offs, CAP reasoning, what breaks at 10x/1000x, AWS mapping (note only) |
| `CODE_MAP.md` | This file |
| `LICENSE` | MIT, Money Chanalia |
| `pyproject.toml` | Package metadata and the `dq` console script |
| `.gitignore` | Ignores caches, build output and local data directories |
| `.github/workflows/ci.yml` | Runs the tests on Python 3.11, 3.12 and 3.13 |
| `docs/benchmark_run1.json` .. `benchmark_run3.json` | Raw benchmark output from three full runs |
| `docs/bench_output_run1.txt` .. `bench_output_run3.txt` | The same runs as printed tables |
| `docs/test_output.txt` | Verbose output of the full test run |
| `docs/demo_output.txt` | Captured output of `python -m dq demo` |
| `docs/network_session.txt` | Captured output of exercising a running server with the CLI |
