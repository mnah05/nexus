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
4. **Optional Raft cluster mode** — enabled by a node ID or peer list
   (`NODE_ID`/`PEERS` env, or `<nodeID>` / `<peer1,peer2>` CLI args):
   - builds a `Config` with `internal.DefaultConfig(nodeID, peers)`,
   - wraps peer communication in `internal.NewHTTPTransport(cfg.HTTPTimeout)`,
   - constructs the node with `internal.New(cfg, transport, kv)`,
   - starts the election loop with `go raftNode.Run()`.
   `raftNode.Close()` is called on every shutdown/restart path.
5. **Serves** the chi router from `internal.NewRouter(kv, raftNode)` with
   `http.ListenAndServe`.

## Example

```sh
go run .                  # wal.log on :8080
go run . /data/wal.log    # custom WAL location
PORT=9090 go run .        # custom port
go run . n1.wal localhost:8001 localhost:8002,localhost:8003   # raft cluster node
```
