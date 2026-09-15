package raft

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"
)

var (
	// ErrNotLeader is returned when a client submits a mutation to a follower.
	ErrNotLeader = errors.New("raft: not leader")
	// ErrNoQuorum is returned when an entry could not be replicated to a majority.
	ErrNoQuorum = errors.New("raft: quorum unavailable")
)

// RaftNodeState is the role a node currently plays in the cluster.
type RaftNodeState int

const (
	Follower RaftNodeState = iota
	Candidate
	Leader
)

// String implements fmt.Stringer for logging.
func (s RaftNodeState) String() string {
	switch s {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		return "Unknown"
	}
}

// RaftConfig holds a node's identity and timing parameters. Election timeouts
// are spread across a range so followers do not all campaign at the same time.
type RaftConfig struct {
	ID                string
	Peers             []string
	ElectionMaxTime   time.Duration
	ElectionMinTime   time.Duration
	HeartbeatInterval time.Duration // must stay well below ElectionMinTime
	RequestTimeout    time.Duration
	Log               *slog.Logger
}

// DefaultRaftConfig returns a RaftConfig with local-demo timings.
func DefaultRaftConfig(id string, peers []string) *RaftConfig {
	return &RaftConfig{
		ID:                id,
		Peers:             peers,
		ElectionMaxTime:   500 * time.Millisecond,
		ElectionMinTime:   300 * time.Millisecond,
		HeartbeatInterval: 100 * time.Millisecond,
		RequestTimeout:    100 * time.Millisecond,
		Log:               slog.Default(),
	}
}

// RequestArgs is the RequestVote payload a candidate sends to each peer.
// LastLogIndex and LastLogTerm let a voter reject a candidate whose log is behind.
type RequestArgs struct {
	Term         int64  `json:"term"`
	CandidateID  string `json:"candidate_id"`
	LastLogIndex uint64 `json:"last_log_index"`
	LastLogTerm  uint64 `json:"last_log_term"`
}

// RequestReply is a peer's response to RequestArgs. A higher Term means the
// candidate is stale and must step down.
type RequestReply struct {
	Term        int64 `json:"term"`
	VoteGranted bool  `json:"vote_granted"`
}

// AppendArgs is sent by the leader for heartbeats and log replication. An empty
// Entries slice is a heartbeat. PrevLogIndex/PrevLogTerm let the follower verify
// that its log matches the leader's right before the new entries.
type AppendArgs struct {
	Term         int64      `json:"term"`
	LeaderID     string     `json:"leader_id"`
	PrevLogIndex uint64     `json:"prev_log_index"`
	PrevLogTerm  uint64     `json:"prev_log_term"`
	Entries      []WALEntry `json:"entries,omitempty"`
	LeaderCommit uint64     `json:"leader_commit"`
}

// AppendReply is a follower's response to AppendArgs.
type AppendReply struct {
	Term    int64 `json:"term"`
	Success bool  `json:"success"`
}

// NodeTransport sends Raft RPCs to peers. Implemented over HTTP in production
// and in-memory in tests.
type NodeTransport interface {
	RequestVote(ctx context.Context, addr string, args RequestArgs) (RequestReply, error)
	AppendEntries(ctx context.Context, addr string, args AppendArgs) (AppendReply, error)
}

// ApplyStore applies committed log entries to the local key-value store. Get is
// included because any node, including a read replica, can serve reads.
type ApplyStore interface {
	Set(key, val string) (uint64, error)
	Get(key string) (string, bool)
	Del(key string) (uint64, error)
}

// OpType identifies a replicated key/value mutation.
type OpType string

const (
	OpSet OpType = "SET"
	OpDel OpType = "DEL"
)

// WALEntry is the replicated command stored in the Raft log.
type WALEntry struct {
	Idx  uint64
	Op   OpType
	Term int
	Key  string
	Val  string
}

// RaftNode is a single cluster member. Config, Store and Transport are injected
// at construction; all consensus state below is guarded by mu.
type RaftNode struct {
	Config    *RaftConfig
	Store     ApplyStore
	Transport NodeTransport

	mu sync.Mutex
	// proposalMu keeps proposals ordered through replication, commitment, and
	// application. Raft log entries must be applied in index order.
	proposalMu sync.Mutex
	state      RaftNodeState
	currTerm   int64
	votedFor   string // candidate voted for in currTerm; empty if none
	leaderID   string

	LastHeartbeat time.Time // last contact from a leader; drives the election timeout
	wg            sync.WaitGroup

	logEntries   []WALEntry
	lastLogIndex uint64 // sent in RequestArgs to prove log freshness
	lastLogTerm  uint64

	commitIndex uint64 // highest log index known to be committed

	ctx    context.Context // cancelled by Close to stop background loops
	cancel context.CancelFunc
}

// RaftStatus is a consistent snapshot of a node's public consensus state.
type RaftStatus struct {
	ID       string
	Role     RaftNodeState
	Term     int64
	LeaderID string
	Peers    []string
}
