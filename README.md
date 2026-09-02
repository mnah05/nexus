# nexus

A single-node, persistent key-value store in Go. Every write goes to a
write-ahead log (WAL) before it touches the in-memory map, and the full
state is periodically snapshotted to disk with the log cleared after.

## Features

- **Write-ahead logging** — SET/DEL are persisted before they are applied,
  so the store survives process crashes and restarts.
- **Snapshots** — the whole state is saved atomically (temp file + rename)
  every 5 minutes by default; the WAL is truncated afterwards to stay small.
- **Recovery** — on startup the latest snapshot is loaded first, then the
  WAL is replayed on top. With no snapshot, recovery falls back to the log.
- **Concurrent & race-free** — `RWMutex`-protected reads scale across cores;
  verified with `go test -race` under heavy concurrent load.
- **HTTP API** — simple JSON/REST endpoints served by [chi](https://github.com/go-chi/chi).

## Quick start

### Standalone Mode
```sh
go run .            # serves on :8080, wal file: wal.log
go run . /path/wal.log   # custom WAL path (snapshot lands next to it)
PORT=9090 go run .  # custom port
```

### 3-Node Raft Cluster Mode (Primary-Read Replicas with Auto-Election)
```sh
# Terminal 1 (Node 1)
PORT=8001 NODE_ID=localhost:8001 PEERS=localhost:8002,localhost:8003 go run . wal1.log

# Terminal 2 (Node 2)
PORT=8002 NODE_ID=localhost:8002 PEERS=localhost:8001,localhost:8003 go run . wal2.log

# Terminal 3 (Node 3)
PORT=8003 NODE_ID=localhost:8003 PEERS=localhost:8001,localhost:8002 go run . wal3.log
```
The cluster automatically elects a Leader (Primary). Writes are processed by the Leader; Followers act as read replicas and return the leader's address on write requests. If the leader goes down, the remaining nodes elect a new leader in ~200ms.


## API

All endpoints return JSON responses with explicit `Content-Type: application/json` headers (with raw plain-text available via `?format=raw` or `Accept: text/plain` on `/get`).

| Method | Endpoint             | Body / Query                  | Response                                         |
|--------|----------------------|-------------------------------|--------------------------------------------------|
| GET    | `/get?key=<k>`       | —                             | `{"key":"k","val":"v"}` or `404`                 |
| GET    | `/list`              | —                             | full map as JSON `{"k":"v", ...}`                |
| POST   | `/set`               | `{"key":"k","val":"v"}`       | `{"ok":true,"idx":<wal_idx>,"key":"k","val":"v"}`|
| POST   | `/del`               | `{"key":"k"}`                 | `{"ok":true,"idx":<wal_idx>,"key":"k"}`          |
| POST   | `/snapshot`          | —                             | `{"ok":true,"message":"snapshot complete"}`       |
| GET    | `/config/snapshot`   | —                             | `{"interval_secs":N}`                            |
| POST   | `/config/snapshot`   | `{"interval_secs":N}`         | `{"ok":true,"interval_secs":N}`                  |
| GET    | `/healthz`           | —                             | `{"status":"healthy"}`                           |
| GET    | `/readyz`            | —                             | `{"status":"ready"}` (or `503` if closing)       |
| GET    | `/metrics`           | —                             | JSON operational metrics                         |
| GET    | `/raft/status`       | —                             | `{"id":...,"role":"Leader\|Follower","term":N}`   |
| GET    | `/debug/pprof/`      | —                             | Go pprof profiler                                |

### Examples

```sh
curl -X POST localhost:8080/set -d '{"key":"foo","val":"bar"}'   # {"ok":true,"idx":1,"key":"foo","val":"bar"}
curl 'localhost:8080/get?key=foo'                                # {"key":"foo","val":"bar"}
curl 'localhost:8080/get?key=foo&format=raw'                      # bar
curl localhost:8080/list                                         # {"foo":"bar"}
curl -X POST localhost:8080/del -d '{"key":"foo"}'               # {"ok":true,"idx":2,"key":"foo"}
curl localhost:8080/healthz                                      # {"status":"healthy"}
curl localhost:8080/metrics                                      # JSON metrics summary
```

## Files

See [docs/architecture.md](docs/architecture.md) for what each file does.

## Testing

```sh
go test -race ./...
```

## Known limitations

- No `fsync`: process crashes are safe, but a power cut can lose the last
  few writes.
- Single node only — no replication or consensus.
- No auth/TLS — bind it behind a firewall or proxy.

