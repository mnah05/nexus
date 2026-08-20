# `internal/walentry.go` — Log Record Format

Defines what one line in `wal.log` looks like and how it converts
to/from CSV.

## Record layout

```
Idx,Op,Term,Key,Val
1,SET,0,foo,bar
2,DEL,0,foo,
```

| Field | Type | Meaning |
|---|---|---|
| `Idx` | uint64 | Monotonic sequence number, starts at 1 |
| `Op` | string | `SET` or `DEL` (`OpType`) |
| `Term` | int | Raft-style term, reserved for multi-node use (always 0 today) |
| `Key` | string | The key being written/deleted |
| `Val` | string | New value; empty for `DEL` |

## Functions

- `(e WALEntry) toRecord() []string` — struct → CSV row.
- `entryFromRecord(rec []string) (WALEntry, error)` — CSV row → struct,
  validating field count (must be exactly 5) and parsing `Idx`/`Term`
  with descriptive errors (`wal: malformed record...`, `wal: bad idx...`).

## Why both directions matter

`Append` serializes on the way in; `Recover` deserializes on the way out.
Strict validation during recovery means a corrupted log fails fast at
startup instead of silently producing wrong state later.
