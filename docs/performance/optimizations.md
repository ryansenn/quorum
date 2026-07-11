# Performance optimizations

Changelog of write-path optimizations applied to Quorum, tracked against the roadmap in [performance.md](../performance.md).

The sections below put architecturally interesting changes first. Low-hanging fruit (removing bad sleep patterns and small CPU wins) is at the end.

Per-experiment tables use the `--quick` harness (2 s measurement window). Headline numbers in the [benchmark report](../benchmarks/REPORT.md) and README use the full 5 s suite (about 28k write ops/s at conc=64 versus about 20k here).

Benchmark command (consistent across per-opt runs):

```bash
go run ./benchmarks --quick --concurrency=1,16,64 --out=benchmarks/results/<label>
```

Environment: Cursor Cloud VM, 4 vCPUs, 16 GB RAM, Go 1.24.0, 3-node cluster, 2 s measurement window.

---

## Baseline (pre-optimization)

Per-entry `fsync` on every log append. A 10 ms sleep after every AppendEntries round (including successful replication).

| Metric | conc=1 | conc=16 | conc=64 |
|---|---:|---:|---:|
| Write throughput (ops/s) | 102 | 815 | 2,444 |
| Write p50 (ms) | 11.51 | 19.91 | 21.67 |
| Write p99 (ms) | 12.63 | 32.94 | 60.19 |
| Read throughput (ops/s) | 19,061 | 65,644 | 67,500 |
| Read p99 (ms) | 0.11 | 1.38 | 4.43 |

---

## Final cumulative results

All kept hot-path optimizations combined (group commit, single fsync, wake on append, sleep removal, serialized cache, failure backoff). Later items such as off-lock compaction change steady-state behavior under active compaction rather than this peak table.

| Metric | conc=1 | conc=16 | conc=64 | vs baseline (throughput) |
|---|---:|---:|---:|---|
| Write throughput (ops/s) | 517 | 7,473 | 20,249 | 5.1× / 9.2× / 8.3× |
| Write p50 (ms) | 1.84 | 2.11 | 3.00 | 6.3× / 9.4× / 7.2× faster |
| Write p99 (ms) | 3.54 | 3.90 | 6.09 | 3.6× / 8.4× / 9.9× faster |
| Read throughput (ops/s) | 15,806 | 66,740 | 69,161 | ~unchanged |
| Read p99 (ms) | 0.12 | 1.44 | 4.65 | ~unchanged |

Write latency at concurrency 1 dropped from about 11 ms (fsync-dominated floor) to about 2 ms. Write throughput at 64 clients rose from about 2.4k to about 20k ops/s.

---

## Architectural optimizations

### Leader-side write batching (opt-in, not a default win)

#### Problem

Each client `put` becomes its own Raft log entry. Under concurrency, group commit already shares one `fsync` and one AppendEntries round across many entries. Packing many puts into one compound entry would further cut log growth, JSON work, and apply steps.

#### Implementation

- `Command` gains `Op: "batch"` with a `Puts []KV` payload. Single-put commands stay unchanged on the wire and on disk.
- The leader runs a `WriteBatcher` collector. Concurrent puts are drained from a submit channel up to `--batch-max-ops` and flushed immediately. A timer wait was tried and rejected for closed-loop load (it only added latency at conc=1).
- `--batch-max-ops=1` (the default) disables batching so one put remains one log entry.
- Apply expands a batch entry into multiple `Storage.Put` calls. Step-down rejects queued puts with `Error: election`.

Files: `core/batch.go`, `core/log.go`, `core/leader.go`, `core/node.go`, `main.go`, `benchmarks/main.go`

#### How numbers were chosen

Grid on the quick harness, then three repeats of finalists (same bar as pipelined commits):

```bash
for ops in 1 32 64; do
  go run ./benchmarks --quick --concurrency=1,16,64 \
    --batch-max-ops=$ops --out=benchmarks/results/final-ops${ops}-rN
done
```

Drain-and-flush policy (no timer). Selection rule: keep as a default only if the conc=64 throughput range clears baseline with no overlap and conc=1 p99 stays within about 2× of baseline.

#### Result (3 runs each, min to max)

| Config | conc=1 thr | conc=16 thr | conc=64 thr | conc=64 p99 (ms) |
|---|---:|---:|---:|---:|
| ops=1 (disabled) | 1,267 to 1,470 | 10,108 to 11,558 | 22,444 to 25,424 | 5.92 to 6.79 |
| ops=32 | 1,266 to 1,880 | 9,186 to 13,549 | 21,377 to 30,570 | 4.50 to 6.52 |
| ops=64 | 1,279 to 1,727 | 8,633 to 10,548 | 19,564 to 28,817 | 4.31 to 6.22 |

Ranges overlap heavily. Median conc=64 for ops=32 is actually slightly below the disabled baseline. On this loopback closed-loop harness, group commit already amortizes fsync and replication across in-flight entries, so fewer log entries do not reliably raise peak ops/s.

**Verdict:** kept as an **opt-in** knob (`--batch-max-ops`, default 1), not counted as a default performance win. Same treatment as `--no-events`. Useful for experiments and for workloads that care about log entry count rather than peak throughput.

---

### Off-lock snapshot compaction (kept)

#### Problem

`maybeCompact` used to write the snapshot, rewrite the log, and `fsync` while holding `ApplyMu` and `LogMu`. Under sustained writes at an aggressive threshold (2,000), that pause cost throughput and inflated tail latency (historical measurement about 27.4k to 17.7k ops/s and p99 about 5 ms to 46 ms).

#### Implementation

1. Short lock: capture applied index, term, state-machine copy, and retained log tail. Release locks.
2. Off-lock disk work: write `.snap.tmp` and `.rlog.tmp`, fsync both.
3. Brief cutover under `LogMu`: if `SnapshotIndex` moved (race with InstallSnapshot), delete temps and abort. Otherwise append any entries that arrived during the off-lock window, swap the pre-synced log file in, rename the snapshot, and update memory.

Files: `core/snapshot.go`, `core/log.go`

#### Result (3-node, conc=64, `--quick`, batching disabled)

| Arm | Write thr (ops/s, min to max) | Write p99 (ms) |
|---|---:|---:|
| Compaction off (threshold `1<<60`) | 28,810 to 31,737 | 4.43 to 4.75 |
| Compaction on (threshold 2,000), off-lock | 23,854 to 27,405 | 10.56 to 12.04 |

Compared with the old on-lock cliff (about 17.7k ops/s and p99 about 46 ms at the same threshold), off-lock compaction recovers most of the throughput and cuts the p99 spike roughly 4×. Compaction is still not free. The pause is much smaller.

Recovery and size (unchanged purpose of compaction), via `TestSnapshotRecoveryBench`:

| Metric | compaction off | compaction on |
|---|---:|---:|
| On-disk log (`.rlog`) | 5.5 MB | 162 KB |
| Snapshot file (`.snap`) | none | 8.7 KB |
| Restart recovery time | 201 ms | 101 ms |

**Kept.** Correctness covered by `TestSnapshotCompaction`, `TestCompactionKeepsConcurrentAppends`, and `TestSnapshotCatchUp`.

---

### Snapshots and log compaction (kept)

#### Problem

The log grew without bound in memory and on disk. A long-running cluster (especially one that rewrites a small key space) accumulates unbounded log entries even though the state machine stays small. A restarting or far-behind node must replay or refetch the entire history.

#### Implementation

- State-machine snapshot (`Engine.Snapshot`/`Restore`) plus log-prefix truncation once applied entries exceed `SnapshotThreshold` (`core/snapshot.go`).
- Log indices are tracked in absolute terms and remapped to the retained tail via `SnapshotIndex`.
- Atomic snapshot persistence plus log rewrite. Recovery loads the snapshot and replays only the tail (`RecoverState`).
- `InstallSnapshot` RPC: the leader ships a snapshot to any follower whose `nextIndex` has fallen below the compacted prefix.

Files: `node.proto`, `core/snapshot.go`, `core/log.go`, `core/node.go`, `core/leader.go`, `core/rpc.go`, `core/storage.go`, `main.go`

See the off-lock section above for current throughput-under-compaction numbers. The size and recovery table is in that section as well.

Reproduce with:

```bash
QUORUM_SNAPSHOT_BENCH=1 go test -run TestSnapshotRecoveryBench -v -timeout 20m ./test
```

---

### Group commit: defer fsync until replication (kept)

#### Problem

Every log append called `fsync` immediately. At high concurrency, many entries were already in the OS page cache before any could be batched.

#### Implementation

- `Logger.AppendLog` writes to the file but does not sync.
- Leader calls `Logger.Sync()` once before sending AppendEntries with new entries.
- Follower `AppendLogs` syncs once per batch (not per entry).

Files: `core/log.go`, `core/leader.go`

| Metric | conc=1 | conc=16 | conc=64 |
|---|---:|---:|---:|
| Write throughput | 138 to 149 | 3,411 to 2,827 | 2,887 to 18,698 |
| Write p50 (ms) | 7.83 to 6.84 | 3.45 to 5.98 | 18.18 to 3.06 |
| Write p99 (ms) | 12.26 to 10.57 | 19.38 to 11.44 | 75.39 to 13.37 |

Massive improvement at high concurrency. Modest at low. Durability is preserved (entries are fsynced before the replication RPC). Kept.

---

### Single fsync per batch via dirty flag (kept)

#### Problem

With group commit, each follower replication goroutine called `Sync()` before its RPC. A 3-node cluster triggered two redundant fsyncs per batch.

#### Implementation

`Logger` tracks a `dirty` flag. `Sync()` is a no-op when the log is already synced.

Files: `core/log.go`

| Metric | conc=1 | conc=16 | conc=64 |
|---|---:|---:|---:|
| Write throughput | 149 to 137 | 2,827 to 3,045 | 18,698 to 19,478 |
| Write p99 (ms) | 10.57 to 13.63 | 11.44 to 10.80 | 13.37 to 10.79 |

Modest but consistent improvement at conc ≥ 16. Kept.

---

## Experiments that did not pay off

### Pipelined commits on the leader (reverted)

Hypothesis: decoupling persistence from replication should raise write throughput. Three changes were implemented together:

1. A dedicated persister goroutine that batches the `fsync` off each follower replicator's hot path and publishes a durable index.
2. A durability gate in `UpdateCommitIndex`: the leader counts its own copy toward a quorum only up to the durable index, so the fsync can overlap with replication RPCs without ever committing a not-yet-durable entry.
3. Channel-generation commit/apply notifications replacing the broadcast condition variables, so advancing the commit index no longer wakes and serializes every waiter (thundering herd).

Result (3-node cluster, 5 s window, `--no-events`. Min to max across repeated runs on the same VM, which showed large run-to-run variance from noisy-neighbor effects):

| Concurrency | baseline (write ops/s) | pipelined (write ops/s) |
|---:|---:|---:|
| 16 | 9,836 to 12,710 (6 runs) | 10,750 to 14,688 (4 runs) |
| 64 | 13,006 to 15,124 (6 runs) | 12,410 to 14,538 (4 runs) |

No repeatable improvement. The arms' ranges overlap heavily. At peak concurrency (64 clients) the pipelined build is, if anything, slightly lower. The write path is already effectively pipelined: each client request runs in its own goroutine, and per-request latency tracks `concurrency / throughput` (Little's law), so requests already overlap. The added machinery was complexity without payoff, so the change was reverted.

### Disable debug event recording (`--no-events`) (not kept for perf)

Event recording on every client request and Raft RPC adds mutex and allocation overhead. A `--no-events` flag was added to `quorum` and to the benchmark harness.

| Metric | conc=1 | conc=16 | conc=64 |
|---|---:|---:|---:|
| Write throughput | 149 to 144 | 2,827 to 2,696 | 18,698 to 18,813 |
| Write p99 (ms) | 10.57 to 12.88 | 11.44 to 9.85 | 13.37 to 8.15 |

No consistent improvement (within run-to-run variance). Flag kept as an optional operational knob for deployments that do not need the playground, but not counted as a performance win.

---

## Low-hanging fruit

These removed accidental waits and duplicated work on the existing replication loop. They matter, and they were measured first historically, but they are not architectural changes to Raft's unit of work or to maintenance I/O.

### Wake replicators on append (kept)

After replicating all pending entries, follower goroutines sent a heartbeat and then slept 10 ms. A new client write during that sleep waited unnecessarily.

Added `ReplicateNotify` on `Node`. The leader calls `notifyReplicators()` after appending. Idle replication loops wait on the channel (with a heartbeat timeout) instead of a blind sleep.

Files: `core/node.go`, `core/leader.go`

| Metric | conc=1 | conc=16 | conc=64 |
|---|---:|---:|---:|
| Write throughput | 136 to 500 | 3,407 to 8,227 | 19,117 to 19,875 |
| Write p50 (ms) | 7.03 to 2.00 | 3.46 to 1.86 | 3.05 to 3.06 |
| Write p99 (ms) | 13.61 to 3.64 | 12.63 to 3.81 | 8.60 to 6.00 |

Largest single improvement after group commit. Kept.

### Remove replication failure backoff (kept)

On AppendEntries mismatch, the replicator slept 1 ms before retrying. It now retries immediately (decrement `nextIndex` and continue).

Files: `core/leader.go`

| Metric | conc=1 | conc=16 | conc=64 |
|---|---:|---:|---:|
| Write throughput | 500 to 711 | 8,227 to 7,974 | 19,875 to 19,529 |
| Write p50 (ms) | 2.00 to 1.04 | 1.86 to 1.96 | 3.06 to 3.13 |
| Write p99 (ms) | 3.64 to 3.60 | 3.81 to 3.83 | 6.00 to 6.47 |

Clear win at conc=1. Neutral at higher concurrency. Kept.

### Cache serialized command bytes (kept)

The leader re-marshaled each log entry to JSON on every AppendEntries RPC to every follower.

`LogEntry` stores pre-marshaled `Serialized []byte` at creation. Replication uses it directly. Rebuilt on log recovery from disk.

Files: `core/log.go`, `core/leader.go`

| Metric | conc=1 | conc=16 | conc=64 |
|---|---:|---:|---:|
| Write throughput | 137 to 136 | 3,045 to 3,407 | 19,478 to 19,117 |
| Write p99 (ms) | 13.63 to 13.61 | 10.80 to 12.63 | 10.79 to 8.60 |

Small CPU savings. No measurable downside. Kept.

### Skip replication sleep when entries are pending (kept)

`ReplicateToFollower` slept 10 ms after every AppendEntries RPC, including after successfully replicating entries. This added idle time on the critical path under load.

Only sleep when sending heartbeats with zero entries. On replication failure, retry immediately instead of sleeping.

Files: `core/leader.go`

| Metric | conc=1 | conc=16 | conc=64 |
|---|---:|---:|---:|
| Write throughput | 102 to 138 | 815 to 3,411 | 2,444 to 2,887 |
| Write p50 (ms) | 11.51 to 7.83 | 19.91 to 3.45 | 21.67 to 18.18 |
| Write p99 (ms) | 12.63 to 12.26 | 32.94 to 19.38 | 60.19 to 75.39 |

Large gains at low and medium concurrency. Kept.

---

## Not implemented (tradeoffs for discussion)

These items from [performance.md](../performance.md) were considered but not implemented because they either require a larger refactor, change consistency or durability guarantees, or did not show measurable benefit in quick testing.

| Idea | Expected benefit | Why deferred |
|---|---|---|
| Async / timer-based fsync | High write throughput | Weakens crash durability. Unsynced entries could be lost on power failure. |
| Optional `--sync=none` mode | Benchmark-only throughput | Useful for load testing but misleading unless clearly documented. |
| Skip apply wait in `Commit()` | Lower write latency | Write returns before the state machine applies. Breaks read-after-write linearizability on the leader. |
| Follower-local reads (stale) | High read throughput | Relaxed consistency. Reads may return outdated values. |
| Read index / leader leases | Linearizable follower reads | Significant Raft extension. The read path is already fast (about 70k ops/s). |
| Binary / protobuf log encoding | Lower CPU and disk bytes | Migration complexity. Fsync dominated until batching was in place (now addressed). |
| Shorter election timeouts | Faster failover | Increases false-election risk under load. |
| HTTP to gRPC client protocol | Modest latency reduction | API change. Consensus cost dominates writes. |

Leader-side write batching moved out of this table into the architectural section above (implemented, opt-in).

---

## How to reproduce

```bash
go test -count=1 ./core ./test
go run ./benchmarks --quick --concurrency=1,16,64
go run ./benchmarks --quick --concurrency=1,16,64 --batch-max-ops=32
go run ./benchmarks --quick --concurrency=64 --snapshot-threshold=2000
```

Results are written to `benchmarks/results/`. Baseline and per-optimization runs are stored under labels such as `baseline`, `final-ops1-rN`, and `final-compact-on-rN`.
