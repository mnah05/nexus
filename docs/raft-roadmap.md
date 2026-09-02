# Raft Roadmap

Plan for adding basic Raft consensus to nexus, in phases. Phase 1 is the
current focus; Phase 2 items are deferred until election + replication work.

## Current state (what's ready)

- `WALEntry` already has `Idx` + `Term` fields → Raft RPC payloads map directly
  onto this struct (`internal/walentry.go`)
- `WAL.ReadAll()` gives the full log for `lastLogTerm` / prefix matching
- `store.Set` / `store.Del` reusable as "apply on commit"
- Single-package layout and chi-only deps keep things simple

## Hard rules from day one

- **Terms and indices are monotonic** — never reuse or reset them.
- **Discovery cleanup ≠ membership change.** Removing a dead node from the peer
  DB affects who you *dial*, never who counts toward *majority* (Phase 1 keeps
  the voting set static).
- **A higher term seen anywhere → become follower immediately.**

## Phase 1 — Core consensus (current focus)

### 1. Leader election (RequestVote)

- Election timeout; restart timer on valid heartbeat from current leader
- RPC: `RequestVote(term, candidateId, lastLogIdx, lastLogTerm)`
- Grant vote only if candidate's log is at least as up-to-date
  (compare lastLogTerm, then lastLogIdx)
- One vote per term per node (persisted ideally; in-memory OK to start)

### 2. Log replication (AppendEntries)

- Heartbeats double as log shipping — one RPC does both
- Consistency check: `prevLogIndex` / `prevLogTerm` must match follower's log
- Conflict resolution: leader decrements `nextIndex` on mismatch, then sends
  entries that overwrite any conflicting suffix on the follower
- Commit advance: majority of acks → bump `commitIndex` → apply to state machine

### 3. Build & test order

1. **Single node**: elect self immediately, append locally, commit instantly
2. **2–3 nodes**: elections + heartbeats survive `kill -9`
3. **Conflict scenario**: kill a leader mid-write, restart it, watch logs
   reconcile — this is where the algorithm's design makes sense

## Phase 2 — After core works (each independently shippable)

1. **Durability** — fsync on WAL append; monotonic indices (stop resetting
   `nextIdx` to 1 on snapshot truncate)
2. **Snapshots + compaction** — add last-included-index/term metadata;
   InstallSnapshot RPC for lagging nodes
3. **Peer discovery polish** — DB registration (upsert by node ID),
   TTL cleanup of dead peers, no elections until expected peer count registers
4. **Client UX** — redirect/forward writes arriving at a follower to the leader

## Peer discovery design (agreed approach)

- New node upserts its address into a shared DB at startup
- Nodes read other addresses from that DB
- A separate checker program (or embedded TTL heartbeat) prunes crashed nodes
- Pruned nodes are only removed from *discovery*, not from the voting set

## Known limitations accepted during Phase 1

- No fsync → commits are not crash-safe
- Periodic snapshots truncate the whole log and reset indices — disable them
  once clustered
- Static voting configuration; dynamic membership reconfiguration is future work
