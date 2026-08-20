# `internal/kv.go` — Core Service

Ties the WAL, in-memory store, and snapshotting together. All business
rules live here.

## Type

```go
type KV struct {
    mu       sync.Mutex // serializes mutations with snapshot/truncate
    store    *Store[string]
    wal      *WAL
    snapPath string
    interval time.Duration
    stop     chan struct{}
}
```

## Methods

| Method | Purpose |
|---|---|
| `NewKV(walPath)` | Opens WAL, recovers state, starts snapshot goroutine |
| `Recover()` | Snapshot first, then WAL replay (see below) |
| `Get(key)` | Reads from memory only — never touches the WAL |
| `List()` | Point-in-time copy of the whole map |
| `Set(key, val)` | **WAL append first**, then apply to map; returns WAL index |
| `Del(key)` | Same order as Set; logs the delete before removing |
| `Snapshot()` | Copy map → atomic save → truncate WAL, all under `mu` |
| `SetTiming(d)` | Re-configures the interval (`0` disables auto-snapshots) |

## Key design decisions

- **Write-ahead ordering**: a mutation is only applied to the map after its
  log entry is flushed. If the process dies between the two, recovery
  replays the entry — no lost writes.
- **Single mutex around mutate/snapshot**: prevents a snapshot from copying
  the map between "logged" and "applied", which would silently drop a write
  on truncate.
- **Recovery is idempotent**: SET/DEL replayed over a fresh snapshot produce
  the same state, so snapshot + full-log replay is always safe.
- **Snapshot goroutine**: `startSnapshooter` runs a `time.Ticker` loop;
  `SetTiming` closes the old goroutine's stop channel and starts a new one.
  Default interval: `DefaultSnapshotInterval` = 5 minutes.
