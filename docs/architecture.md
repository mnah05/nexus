# Architecture & File Guide

nexus is a single-binary key-value store. The write path is:
**HTTP request → WAL append → in-memory map**. Periodically the reverse
happens for housekeeping: **map → snapshot file → WAL truncated**.

For the reasoning behind the design, see [how-it-works.md](how-it-works.md).

```
            GET /get, /list
client ────────────────────────► Store (in-memory map)
   │
   │  POST /set, /del
   ▼
  WAL (wal.log) ── every N secs ──► Snapshot (wal.log.snap) ──► WAL truncated
```

## Files

Per-file deep dives live in their own docs:

| Doc | Covers |
|---|---|
| [main.md](main.md) | Entry point and wiring |
| [kv.md](kv.md) | Core service: WAL-first writes, recovery, snapshots |
| [store.md](store.md) | In-memory concurrent map |
| [wal.md](wal.md) | Append-only write-ahead log |
| [walentry.md](walentry.md) | Log record format |
| [snapshot.md](snapshot.md) | Atomic snapshot save/load |
| [http.md](http.md) | HTTP API routes |
| [store_test.md](store_test.md) | Store test suite |
| [how-it-works.md](how-it-works.md) | Why the design works the way it does |

### `main.go`
Entry point and wiring only. Resolves the WAL path (first CLI arg,
default `wal.log`) and port (`PORT` env, default `:8080`), constructs
the KV service — which recovers state from disk — then serves the chi
router with `http.ListenAndServe`.

### `internal/kv.go`
The core service that ties everything together.

- `NewKV` — opens the WAL, recovers state, starts the snapshot goroutine.
- `Recover` — loads the latest snapshot first, then replays WAL entries on
  top (SET/DEL are idempotent, so replay after a snapshot is always safe).
- `Get` / `List` — read straight from the in-memory store.
- `Set` / `Del` — **WAL first, then apply to the store**, under one mutex so
  a mutation can never slip between log write and map update.
- `Snapshot` — copies the map, writes it atomically, truncates the WAL while
  holding the mutex (no lost writes between copy and truncate).
- `SetTiming` / `startSnapshooter` — a goroutine with a `time.Ticker` that
  triggers snapshots at the configured interval (default 5 min).

### `internal/store.go`
Generic in-memory map (`Store[V]`) guarded by a `sync.RWMutex`.
Reads take `RLock` (parallel across cores), writes take `Lock`.
`Snapshot()` returns a safe copy of the whole map. The `term` field is
reserved for future multi-node work.

### `internal/wal.go`
Append-only CSV write-ahead log.

- `OpenWal` — opens/creates the file in append mode.
- `Append` — serializes an entry as a CSV record, flushes it to the OS,
  returns its index. One mutex serializes all writers (~3 µs/op).
- `ReadAll` — reads every entry back for recovery.
- `Truncate` — clears the log and resets indices; called after a snapshot.

### `internal/walentry.go`
The WAL record format: `(Idx, Op, Term, Key, Val)` plus converters
between `WALEntry` structs and CSV records. Ops are `SET` and `DEL`;
`Val` is empty for deletes. `Term` is reserved for multi-node use.

### `internal/snapshot.go`
Point-in-time persistence of the full map as JSON.

- `SaveSnapshot` — writes to `<path>.tmp`, then renames onto `<path>`,
  so a crash mid-write never leaves a partial snapshot.
- `LoadSnapshot` — restores the map; returns nil (no error) when no
  snapshot exists yet, letting recovery fall back to the WAL alone.

### `internal/http.go`
chi router exposing the API: `/get`, `/list`, `/set`, `/del`,
`/snapshot`, and `/config/snapshot` (GET/POST). Handlers are thin —
they decode input, call KV, and map errors to status codes.

### `internal/store_test.go`
Tests for the store: get/set/del semantics, overwrite behavior,
deletes of missing keys, generic type support, and a concurrent stress
test meant to be run under `-race`.

## Data files (runtime, gitignored)

| File             | Purpose                                   |
|------------------|-------------------------------------------|
| `wal.log`        | Append-only operation log                 |
| `wal.log.snap`   | Latest full-state snapshot (JSON)         |
| `*.snap.tmp`     | Temp file used during atomic snapshotting |

## Recovery order

1. If `wal.log.snap` exists → load it into the map.
2. Replay all entries from `wal.log` on top (idempotent).
3. Serve traffic; new writes go to the WAL before the map.
