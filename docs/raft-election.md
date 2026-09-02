# Raft Leader Election & Primary-Read Replica Guide

This document explains the Raft consensus and leader election implementation in `internal/raft.go`, how the 3-node cluster operates, and the upcoming steps.

---

## 1. Raft Election Rules Implemented in `internal/raft.go`

### State Transitions

```
+---------------+   Timeout elapsed   +---------------+   Majority votes (>= 2)   +------------+
|   Follower    | ------------------> |   Candidate   | ------------------------> |   Leader   |
| (Read Replica)|                     |               |                           | (Primary)  |
+---------------+                     +---------------+                           +------------+
        ^                                     |                                          |
        |       Discovers higher term         |         Discovers higher term            |
        +-------------------------------------+------------------------------------------+
```

### Rule Reference

#### A. Candidate & Election Rules
- **Rule 1 (Timeout Trigger)**: If a Follower receives no heartbeat within its randomized election timeout ($150\text{ms} - 300\text{ms}$), it transitions to `Candidate`.
- **Rule 2 (New Term & Vote)**: The candidate increments `currTerm`, votes for itself (`votedFor = ID`), and resets its timer.
- **Rule 3 (Parallel RPCs)**: Sends `RequestVoteArgs` in parallel goroutines to all peers.
- **Rule 4 (Quorum Win)**: If it receives $\ge \lfloor N/2 \rfloor + 1$ votes (2 votes in a 3-node cluster), it transitions to `Leader` and launches `runHeartbeats()`.
- **Rule 5 (Higher Term Step-Down)**: If any response contains `term > currTerm`, it immediately demotes itself to `Follower`.

#### B. Voting Rules (`HandleRequestVote`)
- **Rule 1 (Term Check)**: If `args.Term < n.currTerm`, reject vote (`reply.VoteGranted = false`).
- **Rule 2 (Term Advance)**: If `args.Term > n.currTerm`, update term, demote to `Follower`, and clear `votedFor`.
- **Rule 3 (Vote Grant)**: If `n.votedFor == ""` or `n.votedFor == args.CandidateID`, grant vote and reset `lastHeartbeat` timer so the voter does not start its own election.

#### C. Heartbeat Rules (`HandleAppendEntries`)
- **Rule 1 (Stale Leader Check)**: If `args.Term < n.currTerm`, reject (`reply.Success = false`).
- **Rule 2 (Leader Recognition)**: If `args.Term >= n.currTerm`, update term, set `n.Role = StateFollower`, and remember `n.leaderID = args.LeaderID`.
- **Rule 3 (Election Suppression)**: Reset `lastHeartbeat = time.Now()`. This prevents followers from ever triggering an election as long as the leader is pulsing every 50ms.

---

## 2. Primary-Read Replica Mechanics (Your Target Architecture)

In a 3-node cluster:
- **Leader (Node 1)**:
  - Serves mutations: `POST /set`, `POST /del`, `POST /snapshot`.
  - Continuously pulses heartbeats every 50ms to Node 2 and Node 3.
- **Followers (Node 2 & Node 3)**:
  - Serve queries: `GET /get`, `GET /list`.
  - **Write Rejection Policy**: If a write (`POST /set`) arrives at a Follower, the follower rejects it with HTTP 400/403 and returns the leader address:
    ```json
    {
      "error": "not leader",
      "leader": "localhost:8001"
    }
    ```

### Failure Scenarios (3 Nodes: N1, N2, N3)

1. **Leader (N1) crashes**:
   - N2 and N3 stop receiving heartbeats.
   - One node's randomized timer expires first (e.g. N2 at 180ms).
   - N2 becomes Candidate (Term 2), votes for itself, requests vote from N3.
   - N3 grants vote $\rightarrow$ N2 reaches 2 votes (majority).
   - N2 becomes Leader and starts heartbeats to N3.
   - **Cluster downtime**: ~200ms. Mutations continue on N2!
2. **Old Leader (N1) revives**:
   - N1 boots up.
   - N2 sends a heartbeat with `Term 2`.
   - N1 sees `Term 2 > Term 1` $\rightarrow$ N1 immediately steps down to Follower.
   - N1 rejoins as a read replica without any split-brain!

---

## 3. The Next Steps

1. **Wire HTTP Router (`internal/http.go`)**:
   - Mount `POST /raft/request-vote` $\rightarrow$ calls `n.HandleRequestVote`.
   - Mount `POST /raft/append-entries` $\rightarrow$ calls `n.HandleAppendEntries`.
   - Update `POST /set` & `POST /del`:
     - If `!node.IsLeader()`: return `{"error": "not leader", "leader": node.LeaderID()}`.
2. **Wire Entrypoint (`main.go`)**:
   - Accept peer addresses via CLI args / env var (e.g. `./nexus-server :8001 :8002 :8003`).
   - Start Raft node on startup.
3. **Local 3-Node Cluster Run & Kill Test**:
   - Start 3 instances on ports `:8001`, `:8002`, `:8003`.
   - Verify Node 1 becomes leader.
   - Write to Node 1, read from Node 2 and Node 3.
   - Kill Node 1 (`kill -9`) $\rightarrow$ observe automatic election of Node 2/3.
   - Revive Node 1 $\rightarrow$ observe it rejoining as follower.
