# Raft Leader Election, Log Replication & Cluster Guide

This document explains the Raft consensus, automated leader election, live log replication, and primary-read replica architecture implemented in Nexus KV.

---

## 1. Raft State Transitions

```
+---------------+   Timeout elapsed   +---------------+   Majority votes (>= 2)   +------------+
|   Follower    | ------------------> |   Candidate   | ------------------------> |   Leader   |
| (Read Replica)|                     |               |                           | (Primary)  |
+---------------+                     +---------------+                           +------------+
        ^                                     |                                          |
        |       Discovers higher term         |         Discovers higher term            |
        +-------------------------------------+------------------------------------------+
```

---

## 2. Invariants & Rules

### A. Candidate & Election Rules
- **Timeout Trigger**: If a Follower receives no heartbeat within its randomized election timeout ($150\text{ms} - 300\text{ms}$), it transitions to `Candidate`.
- **Term Increment & Self-Vote**: The candidate increments `currTerm`, votes for itself (`votedFor = ID`), and resets its countdown.
- **Parallel Vote Broadcast**: Sends `RequestVoteArgs` concurrently to all peers.
- **Majority Quorum**: If it secures $\ge \lfloor N/2 \rfloor + 1$ votes (2 votes in a 3-node cluster), it transitions to `Leader` and launches the 50ms heartbeat loop (`runHeartbeats()`).
- **Higher Term Demotion**: If any response contains `term > currTerm`, the candidate or leader immediately steps down to `Follower`.

### B. Voting Rules (`HandleRequestVote`)
- **Term Guard**: If `args.Term < n.currTerm`, reject vote (`VoteGranted = false`).
- **Term Advance**: If `args.Term > n.currTerm`, update term, demote to `Follower`, and clear `votedFor`.
- **Vote Allocation**: If `n.votedFor == ""` or `n.votedFor == args.CandidateID`, grant the vote and reset `lastHeartbeat` timer.

### C. Heartbeats & Log Replication (`HandleAppendEntries`)
- **Stale Leader Check**: If `args.Term < n.currTerm`, reject (`Success = false`).
- **Leader Recognition**: If `args.Term >= n.currTerm`, adopt term, become `Follower`, and recognize `leaderID`.
- **Election Suppression**: Reset `lastHeartbeat = time.Now()` to prevent coup elections.
- **Log Entry Application**: If `args.Entries` is non-empty, the follower applies each operation (`OpSet` or `OpDel`) to its local WAL and in-memory store.

---

## 3. Primary-Read Replica Mechanics

In a 3-node cluster:
- **Leader (Primary)**:
  - Serves mutations: `POST /set`, `POST /del`, `POST /snapshot`.
  - Replicates all writes to followers in parallel via `raftNode.ReplicateEntry(entry)`.
  - Continuously pulses heartbeats every 50ms to suppress elections.
- **Followers (Read Replicas)**:
  - Serve queries: `GET /get`, `GET /list`.
  - **Write Rejection Policy**: If a write (`POST /set` or `POST /del`) is received by a Follower, it rejects the request with HTTP `403 Forbidden` and returns the current leader's address:
    ```json
    {
      "error": "not leader",
      "leader": "localhost:8001"
    }
    ```

---

## 4. Cluster Management Commands

Use the provided `Makefile` to control the cluster:

```sh
# Start 3 nodes in the background (:8001, :8002, :8003)
make cluster-start

# Check cluster status across all 3 nodes
make cluster-status

# Open the embedded Web Dashboard in your browser
make open-ui

# Gracefully terminate all cluster nodes
make cluster-stop

# Clean temporary cluster logs
make cluster-clean
```

---

## 5. Failure & Self-Healing Scenarios

1. **Leader Crash**:
   - When the leader process crashes or is killed (`kill -9`), followers stop receiving heartbeats.
   - Within ~180ms, one follower's election timer expires, transitions to `Candidate`, secures the vote of the other follower, and becomes the new `Leader`.
   - **Failover duration**: ~200ms.
2. **Old Leader Revival**:
   - When the killed node restarts, it sees heartbeats with the newer term.
   - It immediately demotes itself to `Follower` and receives all replicated entries without split-brain.
