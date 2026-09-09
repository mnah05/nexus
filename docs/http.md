# `internal/http.go` — HTTP API

Thin chi router that maps REST endpoints onto KV (and optionally Raft)
methods. No business logic — decode input, call KV/raft, map errors to
status codes.

`NewRouter(kv, raftNode)` takes the KV service and an optional `*Node`.
When `raftNode` is non-nil the `/raft/*` endpoints are registered and
leader checks gate the mutation routes.

## Routes

| Method | Path                | KV call            | Errors |
|---|---|---|---|
| GET    | `/get?key=k`        | `kv.Get`           | 404 when missing |
| GET    | `/list`             | `kv.List`          | — |
| POST   | `/set`              | `kv.Set`           | 400 bad JSON, 500 WAL failure |
| POST   | `/del`              | `kv.Del`           | 400 bad JSON, 500 WAL failure |
| POST   | `/snapshot`         | `kv.Snapshot`      | 500 |
| GET    | `/config/snapshot`  | reads interval     | — |
| POST   | `/config/snapshot`  | `kv.SetTiming`     | 400 bad JSON / negative |
| GET    | `/raft/status`      | `raftNode.Status()` snapshot | — |
| POST   | `/raft/request-vote` | `raftNode.HandleRequestVote` | 400 bad JSON |
| POST   | `/raft/append-entries` | `raftNode.HandleAppendEntries` | 400 bad JSON |

In cluster mode `/set`, `/del`, and `/snapshot` require leadership: a
follower answers `403` with `{"error":"not leader","leader":"<addr>"}`
and the leader broadcasts each mutation via `raftNode.ReplicateEntry`.

## Conventions

- Writes take JSON bodies (`{"key":..., "val":...}`); reads use query params.
- Successful writes answer `OK <wal index>` — the index tells you the
  entry's position in the log, handy for debugging.
- `/list` responds `application/json`.
- Interval config uses seconds (`interval_secs`); `0` disables the
  automatic snapshot goroutine entirely.

## Why chi

Same `net/http` handlers and stdlib types, plus composable middleware and
clean method-based routing (`r.Get`, `r.Post`) without a heavy framework.
