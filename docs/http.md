# `internal/http.go` — HTTP API

Thin chi router that maps REST endpoints onto KV method calls.
No business logic — decode input, call KV, map errors to status codes.

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
