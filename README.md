# Quorum

[![CI](https://github.com/ryanssenn/quorum/actions/workflows/ci.yml/badge.svg)](https://github.com/ryanssenn/quorum/actions/workflows/ci.yml)

An implementation of the [Raft consensus algorithm](https://raft.github.io/raft.pdf) in Go.

Raft is a consensus algorithm that allows a cluster of servers to agree on the same sequence of operations, even if some of them fail. Instead of allowing every server to accept writes independently, Raft elects one server as the leader. Every write goes through that leader, which records it in its log and sends it to the other servers.

A write is only considered committed after a majority of the servers have stored it. At that point, every server eventually applies the write in the same order, guaranteeing that they all reach the same state. If the leader crashes, the remaining servers automatically elect a new leader and continue from the last committed state without losing data.

## Demo

<img width="800" height="404" alt="quorum_demo" src="https://github.com/user-attachments/assets/4eb1e2e3-e883-48a3-996a-cb1ad600c111" />

The playground starts a multi-node Raft cluster, drives it with a stress-test workload (concurrent writers, node failures, and leader failover), and exposes live metrics through Prometheus. Alongside the metrics, it renders an animated topology showing client requests, log replication, and leader elections as they happen.

```bash
go run ./playground
```

Requires [Docker Desktop](https://www.docker.com/products/docker-desktop/) (used to start Prometheus). Metrics are documented in [docs/observability.md](docs/observability.md).

## Features

Implements the core Raft protocol from the original paper:

- Leader election with randomized timeouts to reduce split votes
- Log replication with majority-based commits
- Persistent state and crash recovery
- Snapshotting and log compaction
- InstallSnapshot RPC for followers that fall behind compacted logs

Nodes communicate over gRPC. Clients use a small HTTP API and can connect to any node. Followers automatically forward requests to the current leader. The replicated state machine is a simple in-memory key-value store.

For a guided tour of the code (including a complete write traced from HTTP to disk), see [docs/guide.md](docs/guide.md).

## Benchmarks

Results from a 3-node cluster running on a single VM (4 vCPUs, 16 GB RAM, Go 1.24.0), with all nodes communicating over loopback. Full methodology, tables, and graphs: [docs/benchmarks/REPORT.md](docs/benchmarks/REPORT.md).

| Metric                              | Result       |
| ----------------------------------- | ------------ |
| Peak read throughput (64 clients)   | 94,501 ops/s |
| Peak write throughput (64 clients)  | 28,096 ops/s |
| Read latency, p99 (16 clients)      | 1.04 ms      |
| Write latency, p99 (16 clients)     | 2.8 ms       |
| Leader failover recovery (mean)     | ~1.4 s       |

These numbers measure implementation overhead on a single machine rather than network performance across multiple hosts. The gap between the two throughput rows is the cost of consensus: a read is an in-memory lookup on the leader, while a write must reach disk and a majority of nodes before it returns. Failover recovery measures the time from killing the leader under load until a write commits successfully on a surviving node, with no manual intervention.

Group commit, batched fsync, and replication wake-ups raised write throughput roughly 8× over the initial implementation. Per-change results and reverted experiments are in [docs/performance/optimizations.md](docs/performance/optimizations.md).

To reproduce:

```bash
go run ./benchmarks --quick --concurrency=1,16,64
```

## Tests

```bash
go test -race ./core      # unit tests: elections, commit/apply, voting, persistence
go test -v ./test         # integration: real 5-node cluster (failover, restarts, snapshot catch-up)
go test ./playground/...  # playground harness and API
```

The integration suite builds the binary, launches a live cluster as subprocesses, kills leaders under load, restarts nodes, and verifies that committed data survives.

## Manual setup

Each node exposes an HTTP API on `--port`. Raft RPC addresses are configured through `--peers` using the format `id=host:port`. A majority of the configured nodes must be up to commit writes. Three is the minimum that survives a node failure.

```bash
go build -o quorum .

./quorum \
  --id=node1 \
  --port=8001 \
  --peers=node1=127.0.0.1:9001,node2=127.0.0.1:9002,node3=127.0.0.1:9003 \
  --reset=true
```

Start `node2` and `node3` the same way with their own `--id` and `--port`. Use `--reset=false` on restart to recover from the persisted log. Then send requests to any node. Followers automatically forward them to the leader.

```bash
curl "http://127.0.0.1:8001/put?key=foo&value=bar"
curl "http://127.0.0.1:8002/get?key=foo"
curl "http://127.0.0.1:8003/status"
```

Each node exposes Prometheus metrics at `/metrics`. Disable metrics with `--metrics=false`.

## Limitations

Quorum implements the full core protocol but stops where a production system would continue.

- Cluster membership is fixed at startup. Adding or removing a node requires restarting the cluster with a new `--peers` list. Raft's joint-consensus membership changes are not implemented.
- Reads are linearizable except during partitions. The leader waits for its applied state to catch up to the commit index before serving reads, so clients always see committed writes in normal operation. There is no read-index or lease protocol, however, so a leader deposed during a network partition can serve stale reads until the partition heals.
- The log is a single file. Compaction rewrites the entire `.rlog` in place rather than rotating segments, so compaction cost grows with the size of the retained log.
