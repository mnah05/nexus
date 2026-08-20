# How It All Works — and Why

## The problem

An in-memory map is fast but loses everything when the process dies.
Writing to disk on every request is durable but slow if done naively.
nexus gets both using the classic database technique: **write-ahead
logging + periodic snapshots**.

## The core idea

```
write request ──► 1. append to wal.log   (durable record)
                  2. apply to map        (fast reads)
```

The log is the source of truth; the map is just a cache of what the log
says. If the process crashes after step 1 but before step 2, restarting
replays the log and the write is not lost. If it dies before step 1,
the client got an error and knows the write didn't happen.

**Why this order?** Reversing it (map first, log second) creates a window
where the map says "saved" but the log doesn't — a crash there silently
loses acknowledged writes.

## Reads

Reads never touch the log. They hit the `RWMutex`-guarded map directly:
~14 ns, fully parallel across cores. This is why GET throughput scales
with CPU count while SET throughput does not.

## Recovery

```
startup ──► load wal.log.snap (if any) ──► replay wal.log ──► serve
```

- Snapshot restore is instant (one JSON file).
- Log replay re-executes each SET/DEL in order. Because these operations
  are idempotent, replaying entries that predate the snapshot is harmless —
  so we never need to track exactly which entries the snapshot covers.
- No snapshot (first boot)? Skip step one; the log alone rebuilds state.

## Snapshots & truncation

Every 5 minutes (configurable via `/config/snapshot`):

1. Take the KV mutex — pause writers.
2. Copy the map, save it atomically (`tmp` file + rename).
3. Truncate `wal.log`, reset indices.
4. Release the mutex.

**Why hold the lock the whole time?** Without it, a SET could land in the
log between "snapshot copied" and "log truncated" — the truncate would
erase a write that was acknowledged. Pausing writes for the duration
makes copy→truncate one indivisible step.

**Why atomic rename?** A crash during a plain write would leave a corrupt
snapshot; with rename, the file is either the old complete snapshot or
the new complete one.

## Concurrency model

| Layer | Guard | Effect |
|---|---|---|
| Map | `sync.RWMutex` | Parallel readers, exclusive writers |
| WAL | `sync.Mutex` | One writer at a time (~3 µs/op ceiling) |
| KV mutations/snapshots | `sync.Mutex` | Mutation = log+apply as one unit |

All verified with `go test -race` plus a 32-worker strict
read-after-write test — zero lost updates.

## Measured behavior (8-core Mac)

- GET: ~77k req/s over HTTP (scales with cores)
- SET: ~3.3k req/s regardless of concurrency (WAL serialization)
- Sweet spot around concurrency 100; latency climbs past 200

## Deliberate trade-offs

- **No fsync** — survives process crashes, not power cuts. Adding fsync
  costs ~100–1000× on the write path without group commit.
- **Single node** — the `term` fields exist so Raft-style replication can
  be added later without changing the record format.
- **Snapshot pauses writes** — fine at this scale; large stores would want
  copy-on-write or delta snapshots.
