# Raft Consensus & Cluster Roadmap

This roadmap tracks the design and milestone delivery for Raft consensus, durability, and replication in Nexus KV.

---

## Milestone Status Overview

- [x] **Milestone 1: Production Lifecycle & Durability Engine**
  - [x] Monotonic indices across WAL truncations and snapshots.
  - [x] Non-blocking atomic snapshotting with background worker decoupling.
  - [x] Monotonic recovery preserving WAL next index across node restarts.
  - [x] JSON standardized endpoints, input sanitization, and structured logging.

- [x] **Milestone 2: Raft Leader Election Engine**
  - [x] `NodeState` roles: Follower, Candidate, Leader.
  - [x] Randomized election timer (150ms–300ms) with background goroutine.
  - [x] Candidate election flow with parallel `RequestVote` RPC broadcasts.
  - [x] Quorum calculation ($\ge \lfloor N/2 \rfloor + 1$) for election victory.
  - [x] Immediate step-down to Follower upon seeing any higher term.
  - [x] Periodic 50ms heartbeat loop to suppress follower coup attempts.

- [x] **Milestone 3: Primary-Read Replica Routing & Log Replication**
  - [x] Followers reject mutations with HTTP `403 Forbidden` and return current leader's address.
  - [x] Reads (`GET /get`, `GET /list`) served by both Leader and Read Replicas.
  - [x] Leader replicates mutations (`POST /set`, `POST /del`) inside `AppendEntriesArgs.Entries`.
  - [x] Followers write replicated entries to their local WAL and apply to memory.
  - [x] Automated failover in ~200ms when leader crashes; revived leader steps down cleanly.

- [x] **Milestone 4: Embedded Web Dashboard & Tooling**
  - [x] Embedded Vanilla HTML/CSS/JS Single Page Application via `embed.FS`.
  - [x] Live 3-node cluster topology visualizer with real-time roles, terms, and ping latencies.
  - [x] Interactive Key-Value explorer (search, live table, insert form, delete).
  - [x] Raft failover and write-rejection demonstration playground.
  - [x] Comprehensive `Makefile` for single-command cluster management.

---

## Future Enhancements (Phase 3)

1. **Strict Quorum Commit Gate**:
   - Delay returning HTTP 200 to client until majority of followers have acknowledged the append RPC.
2. **InstallSnapshot RPC**:
   - Stream snapshot chunks to lagging nodes that join after WAL truncation.
3. **Dynamic Cluster Reconfiguration**:
   - Joint consensus protocol for dynamically adding/removing nodes at runtime.
