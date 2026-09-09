# `internal/raft.go` — Raft Consensus Node

Implements the Raft consensus state machine that makes a KV node part of a
replicated cluster: leader election, heartbeats, and fire-and-forget log
replication. Behavior-level docs live in [raft-election.md](raft-election.md);
this page is the code map.

## Structure

The file is organized by responsibility so the consensus flow can be read
without jumping through unrelated code:

| Piece | Purpose |
|---|---|
| `Config` / `DefaultConfig` | All timings and the logger, injected up front — no magic numbers. |
| `Transport` | Seam for reaching peers; Raft code never touches HTTP directly. |
| `Node` | The consensus node. All mutable state is private, guarded by `mu`. |
| Election loop | `runElectionTimer` and `startElection`. |
| State transitions | Election and term changes are kept inline; `stepDown` handles higher peer terms. |
| RPC handlers | `HandleRequestVote` and `HandleAppendEntries`. |
| Replication loop | `runHeartbeats`, `ReplicateEntry`, and `applyEntries`. |

### `Config`

```go
type Config struct {
    ID                string
    Peers             []string
    ElectionMin       time.Duration // random election timeout lower bound
    ElectionMax       time.Duration // upper bound
    HeartbeatInterval time.Duration // leader heartbeat cadence
    HTTPTimeout       time.Duration // per-peer HTTP request timeout
    Log               *slog.Logger
}
```

`DefaultConfig(id, peers)` supplies the demo timings: 150–300 ms randomized
election timeout, 50 ms heartbeats, 100 ms per-peer HTTP timeout.

### `Transport`

```go
type Transport interface {
    RequestVote(ctx context.Context, addr string, args RequestVoteArgs) (RequestVoteReply, error)
    AppendEntries(ctx context.Context, addr string, args AppendEntriesArgs) (AppendEntriesReply, error)
}
```

Raft issues RPCs through this interface only. `NewHTTPTransport(timeout)`
returns the production HTTP/JSON implementation (`httpTransport`). Its two
RPC methods each decode their concrete reply type explicitly; the small
`postJSON` helper only builds and sends the shared HTTP request. Tests swap in
an in-memory fake, so elections run without listening sockets.

### `Node` lifecycle

- `New(cfg, transport, store)` — **pure constructor**. Starts no goroutines.
  The `store` is a minimal `applyStore` interface (`Set`/`Del`) that `*KV`
  satisfies, so raft doesn't depend on the concrete store.
- `Run()` — starts the background election loop; blocks until `Close`.
- `Close()` — cancels the node's context (cancelling in-flight RPCs) and
  waits for the election loop to drain.

This is the fix for the old design, where the constructor spawned the
election goroutine and heartbeats were launched ad-hoc and never waited on.

### Private state & read API

`role`, `currTerm`, `votedFor`, `leaderID`, `lastHeartbeat` live behind `mu`.
Readers use `IsLeader()`, `LeaderID()`, `Term()`, and a single consistent
`Status()` snapshot (`Status{ID, Role, Term, LeaderID, Peers}`) — the HTTP
`/raft/status` handler reads one snapshot rather than several lock
acquisitions that could disagree.

### Transitions

- `startElection` — performs the candidate conversion (increment term,
  self-vote, reset the countdown), then fans out `RequestVote` RPCs; single-
  node clusters elect themselves immediately.
- `stepDown(term, source)` — the shared "peer reported a higher term →
  adopt term, become Follower, clear `votedFor`" path, so the demotion
  invariant lives in one place.
- `runHeartbeats` / `runElectionTimer` — the two long-lived loops, both
  exiting on context cancellation.

`HandleRequestVote`, `HandleAppendEntries`, and `ReplicateEntry` keep the
external RPC surface used by the HTTP layer (`internal/http.go`).
`ReplicateEntry` remains intentionally fire-and-forget: entries are broadcast
in parallel and replies are ignored.
