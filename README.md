# Nexus KV

A distributed, persistent key-value store with Raft consensus, automated leader election, primary-read replica routing, real-time log replication, and an interactive embedded Web Dashboard.

---

## Features

- **Write-Ahead Logging (WAL)** — Every `SET` and `DEL` is persisted to disk with monotonic indices before applying to in-memory state.
- **Atomic Snapshots** — Full state is snapshotted atomically (temp file + rename) with WAL truncation to keep log sizes bounded.
- **Raft Consensus & Auto-Election** — 3-node cluster with randomized election timers (150–300ms) and 50ms periodic heartbeats. Automatic leader failover in ~200ms without split-brain.
- **Primary-Read Replicas & Log Replication** — Mutations are accepted only by the Leader and replicated to Followers in real time. Followers serve fast local reads and reject writes with the current leader's address.
- **Embedded Web Client Dashboard** — Ships with an embedded dark-mode UI at `http://localhost:8080/` (or `:8001`) with live cluster topology, key-value mutator, key table, metrics, and activity terminal.
- **Observability** — Built-in JSON operational `/metrics`, `/healthz`, `/readyz`, and `/debug/pprof/` profiling.
- **Zero-Dependency Single Binary** — Ships as a single self-contained Go binary with UI assets embedded via `embed.FS`.

---

## Quick Start with `make`

The repository includes a comprehensive `Makefile` to build, run, and test single nodes or full clusters:

| Command | Description |
| :--- | :--- |
| `make run` | Builds and runs a standalone node on `http://localhost:8080` |
| `make cluster-start` | Launches a **3-node Raft cluster** in background (`:8001`, `:8002`, `:8003`) |
| `make cluster-status` | Displays real-time role (`Leader` vs `Follower`), term, and leader address |
| `make open-ui` | Opens the embedded Web Dashboard at `http://localhost:8001` in your browser |
| `make cluster-stop` | Gracefully stops all running cluster nodes |
| `make cluster-clean` | Stops the cluster and clears temporary cluster WAL logs |
| `make test` | Runs the full unit & concurrency test suite with `-race` enabled |
| `make clean` | Cleans built binaries and local WAL files |

---

## Running the 3-Node Raft Cluster

### 1. Launch with One Command
```sh
make cluster-start
```
Output:
```
Building nexus-server...
Build complete: ./nexus-server
Launching Node 1 on :8001 (Leader candidate)...
Launching Node 2 on :8002 (Follower replica)...
Launching Node 3 on :8003 (Follower replica)...

=== Raft Cluster Health & Roles ===
Node on :8001 -> {"id":"localhost:8001","leader":"localhost:8001","peers":["localhost:8002","localhost:8003"],"role":"Leader","term":1}
Node on :8002 -> {"id":"localhost:8002","leader":"localhost:8001","peers":["localhost:8001","localhost:8003"],"role":"Follower","term":1}
Node on :8003 -> {"id":"localhost:8003","leader":"localhost:8001","peers":["localhost:8001","localhost:8002"],"role":"Follower","term":1}

Open http://localhost:8001 in your browser to view the live dashboard!
```

### 2. Open the Web Dashboard
Visit **`http://localhost:8001`** in any web browser. You will see:
- **Topology Cards**: Real-time roles (`Leader` vs `Follower`), terms, and heartbeat pings for all 3 nodes.
- **Node Switcher**: Switch the UI target to inspect any node in the cluster.
- **Store Table**: Live search and inspection of all keys across the cluster.
- **Write Form**: Send `POST /set` to the Leader and watch all 3 nodes update in real time.
- **Follower Write Simulator**: Click "Test Follower Write" to watch a follower reject writes with HTTP 403 and redirect to the leader.
- **Activity Terminal**: Live streaming activity log of HTTP requests, status codes, and latency.

### 3. Manual Multi-Terminal Startup (Alternative)
```sh
# Terminal 1 (Node 1)
PORT=8001 NODE_ID=localhost:8001 PEERS=localhost:8002,localhost:8003 go run . wal1.log

# Terminal 2 (Node 2)
PORT=8002 NODE_ID=localhost:8002 PEERS=localhost:8001,localhost:8003 go run . wal2.log

# Terminal 3 (Node 3)
PORT=8003 NODE_ID=localhost:8003 PEERS=localhost:8001,localhost:8002 go run . wal3.log
```

---

## API Reference

All endpoints return JSON responses with `Content-Type: application/json` headers (plain text available on `/get` via `?format=raw` or `Accept: text/plain`).

| Method | Endpoint | Description |
| :--- | :--- | :--- |
| `GET` | `/` | Embedded Web Dashboard UI |
| `GET` | `/get?key=<k>` | Read key (served by Leader and all Read Replicas) |
| `GET` | `/list` | Return full key-value map |
| `POST` | `/set` | Write key-value (Leader only; replicated across cluster) |
| `POST` | `/del` | Delete key (Leader only; replicated across cluster) |
| `POST` | `/snapshot` | Trigger immediate atomic snapshot and log compaction |
| `GET` | `/config/snapshot` | Get snapshot interval |
| `POST` | `/config/snapshot` | Set snapshot interval (`{"interval_secs": 0}` disables) |
| `GET` | `/raft/status` | Current Raft state: role, term, leader address, and peers |
| `POST` | `/raft/request-vote` | Raft candidate vote request RPC |
| `POST` | `/raft/append-entries` | Raft heartbeat & log replication RPC |
| `GET` | `/healthz` | Health check endpoint |
| `GET` | `/readyz` | Readiness check endpoint (503 during shutdown) |
| `GET` | `/metrics` | JSON operational metrics (counts, WAL indices, uptime) |
| `GET` | `/debug/pprof/` | Go pprof runtime profiler |

---

## Testing

Run all race detector and unit tests:
```sh
make test
```
Or directly with Go:
```sh
go test -v -race ./...
```

---

## Documentation

- [Architecture & Invariants](docs/architecture.md)
- [Raft Leader Election & Replication Guide](docs/raft-election.md)
- [Consensus Roadmap](docs/raft-roadmap.md)
