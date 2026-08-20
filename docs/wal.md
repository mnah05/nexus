# `internal/wal.go` — Write-Ahead Log

An append-only CSV log. This is the durability layer: nothing is applied
to the map until it exists here.

## Why CSV?

Simple, human-readable (`1,SET,0,foo,bar`), debuggable with any editor,
and `encoding/csv` handles quoting of keys/values containing commas or
newlines. No custom binary format to maintain.

## Methods

| Method | Purpose |
|---|---|
| `OpenWal(path)` | Opens/creates the file with `O_APPEND\|O_CREATE\|O_WRONLY` |
| `Append(op, term, key, val)` | Writes one record under a mutex, flushes, returns its index |
| `ReadAll()` | Re-reads every entry from disk (used by recovery) |
| `Truncate()` | Empties the log and resets indices (used after snapshots) |

## How an append works

1. Take `w.mu` — all writers serialize here (~3 µs/op total).
2. Build the `WALEntry` with the next index.
3. `writer.Write` + `writer.Flush` — pushes bytes to the OS.
4. Check `writer.Error()` — a failed write returns an error *before* the
   caller touches the map, so the store never diverges from the log.
5. Increment `nextIdx`, return it.

## Truncate

`os.Truncate(name, 0)` empties the file; because the handle is `O_APPEND`,
the next write lands at offset 0 automatically. A fresh `csv.Writer` is
installed and `nextIdx` resets to 1. Safe only while no appends run —
the caller (`KV.Snapshot`) holds the KV mutex across the whole operation.

## Known limitation

Flush reaches the OS page cache but never calls `fsync`. Process crashes
are safe; a power cut can lose the last few entries.
