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

```sh
go run .            # serves on :8080, wal file: wal.log
go run . /path/wal.log   # custom WAL path (snapshot lands next to it)
PORT=9090 go run .  # custom port
```

## API

| Method | Endpoint             | Body / Query                  | Response              |
|--------|----------------------|-------------------------------|-----------------------|
| GET    | `/get?key=<k>`       | —                             | value or `404`        |
| GET    | `/list`              | —                             | full map as JSON      |
| POST   | `/set`               | `{"key":"k","val":"v"}`       | `OK <wal index>`      |
| POST   | `/del`               | `{"key":"k"}`                 | `OK <wal index>`      |
| POST   | `/snapshot`          | —                             | snapshot + truncate now |
| GET    | `/config/snapshot`   | —                             | `{"interval_secs":N}` |
| POST   | `/config/snapshot`   | `{"interval_secs":N}`         | set interval (`0` disables) |

### Examples

```sh
curl -X POST localhost:8080/set -d '{"key":"foo","val":"bar"}'   # OK 1
curl 'localhost:8080/get?key=foo'                                # bar
curl localhost:8080/list                                         # {"foo":"bar"}
curl -X POST localhost:8080/del -d '{"key":"foo"}'               # OK 2
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
- Snapshotting pauses writes for its duration.
