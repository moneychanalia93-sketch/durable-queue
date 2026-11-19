# Design notes

Decisions, the alternatives I rejected, and what breaks as load grows. File names refer to the `dq/` package.

## 1. The core idea: a deterministic command log

`engine.py` is a state machine with no I/O, no clock and no randomness. Every change is a **command** (a dict) that already contains everything that could vary between runs: the time `now`, the message id, and the jitter number `u` used for retry backoff.

```
state(t+1) = apply(state(t), command)        # same commands in, same state out
```

Three features fall out of this one property:

| Feature | How |
|---|---|
| Crash recovery | Load the last snapshot, replay log commands after it |
| Replication | Send the same commands, in the same order, to a follower |
| Testing | Run a naive reference model next to the engine and compare (`tests/reference_model.py`) |

**Rejected: log the *results* (e.g. "message X delivered with handle H").** Bigger log, and replay would not exercise the real logic. **Rejected: log inside the engine.** It would make the engine untestable without disks.

**Apply first, log second.** `Broker._run` executes the command in memory, then appends it to the log, then replies. The caller only hears "success" after the log write returns, so a crash in between just looks like a failed request that the client retries. If the log write itself fails, memory is ahead of disk, so the broker becomes **fail-stop** and refuses all further work rather than serve state it cannot make durable. Commands that change nothing (an empty poll, a rejected ack, a duplicate send) are not logged, which keeps idle consumers from growing the log.

## 2. Queue semantics (`engine.py`)

### Scheduling: one heap, lazy deletion
A message is either *waiting* or *in flight*, and both are the same thing to the scheduler: a message with an `avail_at` time. One min-heap ordered by `(avail_at, seq)` yields the next deliverable message in O(log n).

Heap entries are never edited. When a message changes (received, nacked, extended) it gets a new version number `ver` and a fresh heap entry; old entries are skipped when popped because their `(ver, seq)` no longer match. This is simpler and faster than a heap with decrease-key, at the cost of some stale entries sitting in memory until popped.

### Visibility timeout and receipt handles
Receive sets `avail_at = now + visibility_timeout` and issues a handle `"<message id>.<delivery counter>"`. The counter is per queue and never reused, so a handle identifies one *delivery*, not just one message.

- Ack/nack/extend must present the message's *current* handle. A consumer that stalled past the timeout holds an old handle; once the message is redelivered that handle is dead and its ack raises `InvalidReceipt`.
- A late ack **before** anyone re-received the message still succeeds (the same leniency SQS shows). The message was done; deleting it is correct.
- Why a global counter and not `receive_count` in the handle? After a DLQ move and redrive, `receive_count` resets to 0, so a handle from the first life of the message could collide with a new one. The counter cannot collide. A test covers exactly this case.

### Retries
`nack` hides the message for `uniform(0, min(cap, base * 2^(receive_count-1)))` seconds: exponential backoff with **full jitter**. Jitter matters because without it, every message that failed together retries together and re-overloads the dependency that just failed ("thundering herd"). Full jitter (rather than "equal" jitter) gives the best spread; the cost is that some retries come back almost immediately. The random draw happens in the broker and is stored in the command, so replay is exact.

### Dead-letter queue
When a message is about to be delivered and `receive_count >= max_receive`, it is moved to the configured DLQ instead (SQS does the same check at receive time). Consequences, stated plainly: a queue nobody polls never dead-letters its poison messages, and the DLQ keeps `source` so `redrive` knows where each message came from. Redrive resets `receive_count` and, for FIFO sources, appends at the tail of the group (original position is not restored).

### FIFO groups
Each group is a deque. Only the **head** of a group is in the heap, so while the head is in flight the rest of the group is invisible: order within a group, parallelism across groups. Removing the head (ack or dead-letter) pushes the next one. Cost: one slow or poison message blocks its whole group until it succeeds or is dead-lettered. That head-of-line blocking is inherent to ordering.

### Deduplication
`dedup_id` is remembered for `dedup_window` seconds. A hit returns the original message id and logs nothing. Expired entries are purged lazily (lookup also checks the expiry, so purging is housekeeping only).

### Backpressure
`max_depth` makes `send` raise `QueueFull` instead of accepting unbounded work. Rejecting early pushes the problem back to producers, who can slow down, shed load or alert. Unbounded queues turn an overload into out-of-memory plus huge recovery times. In the cluster the limit applies per node.

## 3. Durability (`wal.py`, `broker.py`)

### Log format
```
"DQWAL001" | [ length u32 | crc32(payload) u32 | payload JSON ] ...
```
JSON because it is readable by a person and debuggable with a text editor; a binary format would be smaller and faster, which matters at scale (section 7).

### Torn tail rule
On open, records are read in order and the scan **stops at the first record that is short or fails its CRC**; everything after is cut off (and saved to `wal.log.corrupt`). We never skip over a bad record to use later ones: later commands assumed the earlier one happened (an ack for a message whose send was lost, for example), so applying them would build a state that never existed. The price: real damage in the *middle* of the file loses the valid records after it. The test truncates the file at every byte offset and checks that recovery always yields a state that really existed.

### fsync policies

| Policy | Survives process crash | Survives power loss | Cost (measured, README) |
|---|---|---|---|
| `always` | yes | yes | ~26x slower sends than `batch` |
| `batch` (64 records or 50 ms) | yes | up to one batch lost | baseline |
| `none` | yes | no | about 1.7x faster sends than `batch` (medians; ranges do not overlap) |

Every append is flushed to the OS, so a *process* crash never loses acknowledged data under any policy; fsync only matters for OS crash and power loss. `batch` has no background thread: the time check runs when a record is appended.

### Snapshots and compaction
The log grows forever without compaction. `snapshot()` does: (1) write `snapshot.json.tmp`, fsync, atomically rename over `snapshot.json`; (2) replace the log with an empty one. Each snapshot starts with its SHA-256. Crash points:

| Crash after | State on disk | Recovery |
|---|---|---|
| before step 1 completes | old snapshot + full log | normal replay |
| step 1, before step 2 | new snapshot + full old log | log records with `lsn <=` snapshot LSN are skipped, so nothing is applied twice |
| step 2 | new snapshot + empty log | load snapshot |

A corrupt snapshot **refuses to start**. The log before it was already compacted away, so silently starting empty would lose data while looking healthy.

## 4. Partitioning (`ring.py`, `cluster.py`)

**Routing key.** FIFO group, else the dedup id, else the message id. Same group means same node, which is what makes per-group ordering possible. Messages with no group spread by id.

**Why consistent hashing, not `hash(key) % N`.** With modulo, changing N remaps almost every key (about `1 - 1/N` of them). On a ring, a new node takes over only the arcs just before its own points, so about `1/N` of keys move. Measured: 18.9% moved going from 4 to 5 nodes (ideal 20%), 10.2% going from 8 to 9 (ideal 11.1%).

**Virtual nodes.** With one point per node, arcs are uneven (std dev of 120% of the mean load with 8 nodes). With 128 points per node it falls to 13%; 512 gives 3.6%. More vnodes means a larger ring to build and search, so it is a dial, not a free lunch.

**Rebalancing protocol** (under an exclusive lock; normal operations hold the shared side of a readers-writer lock, so no send races with a topology change):
1. Dump each queue on every source node, find messages whose owner changed.
2. **Import** them on the new owner (logged there).
3. Only then **delete** them from the old owner.

Copy-then-delete means a crash between 2 and 3 *duplicates* messages. Duplicates are legal under at-least-once; lost messages would not be. In-flight messages that move become visible at the new owner (their old handles stop working), because carrying handles across nodes would need a handle directory. Dedup tables do not move.

## 5. Replication and failover

Each node has a leader and optionally a follower. After the leader logs a command it applies the same record (with the same LSN) to the follower, **before replying**. So an acknowledged write exists on two logs.

- **Failover:** a call that finds the leader dead promotes the follower with the highest LSN and retries once. Because commands are deterministic and the follower saw every acknowledged one, receipt handles issued by the old leader still work on the new one (tested).
- **Restart:** the replica with the highest LSN becomes leader; the other is re-seeded from it.
- **Re-protecting:** `add_follower` seeds a fresh follower from a state snapshot taken under the leader's lock.

What this is not: there is no election, no failure detector, no fencing token, and nothing stops two nodes both believing they lead. In a real network that is the split-brain problem, solved with consensus (Raft/Paxos) or an external lease. Synchronous replication of one follower also means the leader stalls if the follower stalls (a follower that raises is dropped instead).

## 6. CAP and delivery-guarantee reasoning

**CAP.** In a network partition you must choose consistency or availability. This design, with synchronous replication and a single promotable follower, would choose **consistency over availability** for a partitioned follower (writes stop or the follower is dropped) and has no answer for a partitioned *leader* (that needs consensus). Since nodes here share a process, partitions are not simulated; claiming a CAP position beyond this reasoning would be overselling.

**Why exactly-once delivery is a myth.** The consumer processes a message, then sends an ack. If the ack is lost or the consumer dies between processing and acking, the broker cannot tell "never processed" from "processed, ack lost". It has two options: redeliver (at-least-once, duplicates possible) or give up (at-most-once, loss possible). No protocol removes that ambiguity. What systems sell as exactly-once is **at-least-once delivery plus idempotent processing**: the effect (a row insert, a payment) is keyed by message id so a second delivery is a no-op. `demo.py` and `test_idempotent_consumer_*` show this: duplicates are delivered, effects are applied once.

**Ordering.** Standard queues are unordered: a redelivered message arrives after newer ones. FIFO groups give order within a group at the cost of head-of-line blocking.

## 7. What breaks at 10x and 1000x

| Scale | What breaks | What I would do |
|---|---|---|
| **10x** (about 10^5 msgs/s) | One broker lock plus GIL caps throughput (measured: 1 thread is as fast as 16). JSON plus per-record flush dominates (log path ~6-10x slower than memory). `fsync=always` is ~660 msg/s | Group commit (one fsync serves many waiting writers); binary log format; batch API for send/receive; run one process per node instead of threads; a language without a global lock |
| **100x** | Snapshot is O(state) and blocks writes; recovery replays a long log; `stats` scans every message; one node's memory holds all its messages | Incremental/copy-on-write snapshots; log segments; counters maintained incrementally; spill cold messages to disk |
| **1000x** | Real network partitions, correlated failures, hot partitions (one FIFO group or one big customer maps to one node), rebalancing moves data while traffic runs, clock skew between machines breaks wall-clock visibility timeouts | Consensus-replicated partitions (Raft); partition count fixed and large, many partitions per node so rebalancing moves whole partitions; hot-key splitting; leases instead of absolute timestamps; autoscaling and tenant quotas |

## 8. Mapping to AWS (design note only)

This project does **not** run on AWS or use any AWS service. The table maps each idea here to the managed service where the same idea lives, to show the concepts transfer.

| Here | AWS equivalent | Note |
|---|---|---|
| Queue with visibility timeout, receipt handle, `max_receive` + DLQ, FIFO groups, dedup window | **SQS** (standard and FIFO queues) | SQS FIFO calls the group `MessageGroupId` and the dedup id `MessageDeduplicationId` (5-minute window); redrive exists as a console/API feature |
| Fan-out to many consumers | **SNS** topics delivering to several queues | Not built here: a topic would copy each message into N queues |
| Partition by key with a hash ring and virtual nodes; adding a node moves ~1/N of keys | **DynamoDB** partitioning (hash of the partition key to a partition; splits move data) | DynamoDB's internals are not public in detail; the shared idea is hash-based partition placement |
| Ordered, partitioned log with shard-level ordering and consumer checkpoints | **Kinesis Data Streams** | Differs from a queue: records are not deleted on read; consumers track their position |
| WAL + snapshot + replication of commands | The storage layers under all of the above (replicated across Availability Zones) | SQS and DynamoDB replicate across zones; the exact mechanisms are not described here |
| Idempotent consumer | Recommended by AWS for SQS standard queues, which are at-least-once | Same reasoning as section 6 |

## 9. Testing strategy

- **Unit tests** per module, including a truncation test at every byte offset of the log.
- **Property-style tests**: thousands of random operation sequences run against both the engine and a naive reference model; results, handles and stats must match exactly. A guard test checks the random runs actually reach the DLQ and stale-handle paths. A second variant restarts the broker from disk at random moments.
- **Concurrency**: many producer and consumer threads, consumers that crash or stall mid-message, a leader killed under load; asserts no loss and no double acknowledgement.
- **Deterministic chaos**: single-threaded, seeded, manual clock; injects worker crashes, stalls, nacks, poison messages and whole-broker crashes with recovery. Same seed gives the same run (tested).
