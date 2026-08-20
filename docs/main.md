# `main.go` — Entry Point

Wiring only; no business logic lives here.

## What it does

1. **Resolves the WAL path** — first CLI argument, defaults to `wal.log`.
   The snapshot file is derived from it (`wal.log.snap`).
2. **Constructs the KV service** via `internal.NewKV(walPath)`, which opens
   the WAL, recovers state from snapshot + log, and starts the periodic
   snapshot goroutine. A failure here is fatal — the server refuses to
   start with broken persistence.
3. **Resolves the listen address** — `PORT` env var, defaults to `:8080`.
4. **Serves** the chi router from `internal.NewRouter(kv)` with
   `http.ListenAndServe`.

## Example

```sh
go run .                  # wal.log on :8080
go run . /data/wal.log    # custom WAL location
PORT=9090 go run .        # custom port
```
