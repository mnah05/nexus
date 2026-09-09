package internal

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"math/rand"
	"net/http"
	"strings"
	"sync"
	"time"
)

// NodeState represents the role of a node in the Raft consensus group.
type NodeState int

const (
	StateFollower NodeState = iota
	StateCandidate
	StateLeader
)

func (s NodeState) String() string {
	switch s {
	case StateFollower:
		return "Follower"
	case StateCandidate:
		return "Candidate"
	case StateLeader:
		return "Leader"
	default:
		return "Unknown"
	}
}

// Config controls the timing and logging behaviour of a Raft node.
type Config struct {
	ID                string
	Peers             []string
	ElectionMin       time.Duration // random election timeout lower bound
	ElectionMax       time.Duration // random election timeout upper bound
	HeartbeatInterval time.Duration // leader heartbeat cadence
	HTTPTimeout       time.Duration // per-peer HTTP request timeout
	Log               *slog.Logger
}

// DefaultConfig returns a Config with the standard demo timings.
func DefaultConfig(id string, peers []string) Config {
	return Config{
		ID:                id,
		Peers:             peers,
		ElectionMin:       150 * time.Millisecond,
		ElectionMax:       300 * time.Millisecond,
		HeartbeatInterval: 50 * time.Millisecond,
		HTTPTimeout:       100 * time.Millisecond,
		Log:               slog.Default(),
	}
}

// RequestVoteArgs represents the payload sent by candidates to gather votes.
type RequestVoteArgs struct {
	Term         int64  `json:"term"`
	CandidateID  string `json:"candidate_id"`
	LastLogIndex uint64 `json:"last_log_index"`
	LastLogTerm  uint64 `json:"last_log_term"`
}

// RequestVoteReply represents the response sent by peers back to the candidate.
type RequestVoteReply struct {
	Term        int64 `json:"term"`
	VoteGranted bool  `json:"vote_granted"`
}

// AppendEntriesArgs represents the heartbeat / log replication payload sent by the leader.
type AppendEntriesArgs struct {
	Term     int64      `json:"term"`
	LeaderID string     `json:"leader_id"`
	Entries  []WALEntry `json:"entries,omitempty"`
}

// AppendEntriesReply represents the response from followers to the leader.
type AppendEntriesReply struct {
	Term    int64 `json:"term"`
	Success bool  `json:"success"`
}

// Transport sends Raft RPCs to peer nodes. Peers are addressed by ID.
type Transport interface {
	RequestVote(ctx context.Context, addr string, args RequestVoteArgs) (RequestVoteReply, error)
	AppendEntries(ctx context.Context, addr string, args AppendEntriesArgs) (AppendEntriesReply, error)
}

const (
	requestVotePath   = "/raft/request-vote"
	appendEntriesPath = "/raft/append-entries"
)

// httpTransport implements Transport over HTTP/JSON.
type httpTransport struct {
	client *http.Client
}

// NewHTTPTransport returns a Transport that talks to peers over HTTP.
func NewHTTPTransport(timeout time.Duration) Transport {
	return &httpTransport{client: &http.Client{Timeout: timeout}}
}

// postJSON sends a JSON request and returns the HTTP response. The caller is
// responsible for decoding the response into the expected reply type.
func postJSON(t *httpTransport, ctx context.Context, addr, path string, args interface{}) (*http.Response, error) {
	data, err := json.Marshal(args)
	if err != nil {
		return nil, err
	}

	target := addr
	if !strings.HasPrefix(target, "http://") && !strings.HasPrefix(target, "https://") {
		target = "http://" + target
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, target+path, bytes.NewBuffer(data))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := t.client.Do(req)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (t *httpTransport) RequestVote(ctx context.Context, addr string, args RequestVoteArgs) (RequestVoteReply, error) {
	resp, err := postJSON(t, ctx, addr, requestVotePath, args)
	if err != nil {
		return RequestVoteReply{}, err
	}
	defer resp.Body.Close()

	var reply RequestVoteReply
	if err := json.NewDecoder(resp.Body).Decode(&reply); err != nil {
		return RequestVoteReply{}, err
	}
	return reply, nil
}

func (t *httpTransport) AppendEntries(ctx context.Context, addr string, args AppendEntriesArgs) (AppendEntriesReply, error) {
	resp, err := postJSON(t, ctx, addr, appendEntriesPath, args)
	if err != nil {
		return AppendEntriesReply{}, err
	}
	defer resp.Body.Close()

	var reply AppendEntriesReply
	if err := json.NewDecoder(resp.Body).Decode(&reply); err != nil {
		return AppendEntriesReply{}, err
	}
	return reply, nil
}

// applyStore is the local key/value store that followers update on replication.
type applyStore interface {
	Set(key, val string) (uint64, error)
	Del(key string) (uint64, error)
}

// Node represents a single Raft consensus node.
type Node struct {
	cfg Config

	mu       sync.Mutex
	role     NodeState
	currTerm int64
	votedFor string
	leaderID string

	lastHeartbeat time.Time

	transport Transport
	store     applyStore

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// New initializes a Raft node in the Follower state. No background work is
// started until Run is called.
func New(cfg Config, tr Transport, store applyStore) *Node {
	if cfg.Log == nil {
		cfg.Log = slog.Default()
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Node{
		cfg:           cfg,
		transport:     tr,
		store:         store,
		ctx:           ctx,
		cancel:        cancel,
		role:          StateFollower,
		lastHeartbeat: time.Now(),
	}
}

// Run starts the background election loop. It blocks until Close is called.
func (n *Node) Run() {
	n.wg.Add(1)
	n.runElectionTimer()
}

// Close stops all background Raft loops (election timer and heartbeats).
func (n *Node) Close() {
	n.cancel()
	n.wg.Wait()
}

func (n *Node) log() *slog.Logger {
	return n.cfg.Log
}

// Status is a consistent snapshot of the node's consensus state.
type Status struct {
	ID       string
	Role     NodeState
	Term     int64
	LeaderID string
	Peers    []string
}

// Status returns a consistent snapshot of the node's current state.
func (n *Node) Status() Status {
	n.mu.Lock()
	defer n.mu.Unlock()
	return Status{
		ID:       n.cfg.ID,
		Role:     n.role,
		Term:     n.currTerm,
		LeaderID: n.leaderID,
		Peers:    append([]string(nil), n.cfg.Peers...),
	}
}

// IsLeader reports whether this node is currently the cluster leader.
func (n *Node) IsLeader() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.role == StateLeader
}

// LeaderID returns the address of the current known leader.
func (n *Node) LeaderID() string {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.leaderID
}

// Term returns the current term of the node.
func (n *Node) Term() int64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.currTerm
}

// randomTimeout returns an election timeout in [min, max).
func randomTimeout(min, max time.Duration) time.Duration {
	return min + time.Duration(rand.Int63n(int64(max-min)))
}

// runElectionTimer waits for an election timeout and starts elections when the
// node has not heard from a leader recently.
func (n *Node) runElectionTimer() {
	defer n.wg.Done()

	for {
		timeout := randomTimeout(n.cfg.ElectionMin, n.cfg.ElectionMax)

		select {
		case <-n.ctx.Done():
			return
		case <-time.After(timeout):
		}

		n.mu.Lock()
		// Leaders are the authority and must never start an election.
		if n.role != StateLeader && time.Since(n.lastHeartbeat) >= timeout {
			n.log().Info("election timeout elapsed, starting election", "node", n.cfg.ID, "term", n.currTerm+1)
			n.startElection()
		}
		n.mu.Unlock()
	}
}

// startElection transitions the node to Candidate, then requests votes from
// every peer. The caller must hold n.mu.
func (n *Node) startElection() {
	// Becoming a candidate increments the term, votes for this node, and
	// resets the election timer.
	n.role = StateCandidate
	n.currTerm++
	n.votedFor = n.cfg.ID
	n.lastHeartbeat = time.Now()

	votes := 1
	term := n.currTerm
	peers := append([]string(nil), n.cfg.Peers...)

	// If there are no peers (single node cluster), become leader immediately.
	if len(peers) == 0 {
		n.log().Info("single node cluster: elected self as leader", "node", n.cfg.ID, "term", n.currTerm)
		n.role = StateLeader
		n.leaderID = n.cfg.ID
		go n.runHeartbeats()
		return
	}

	// Request votes concurrently so one slow or unavailable peer cannot block
	// the rest of the election.
	for _, peer := range peers {
		go func(addr string) {
			reply, err := n.transport.RequestVote(n.ctx, addr, RequestVoteArgs{
				Term:        term,
				CandidateID: n.cfg.ID,
			})
			if err != nil {
				return
			}

			n.mu.Lock()
			defer n.mu.Unlock()

			if reply.Term > n.currTerm {
				n.stepDown(reply.Term, "RequestVote reply")
				return
			}

			if reply.VoteGranted && n.role == StateCandidate && n.currTerm == term {
				votes++
				// A majority is more than half of all nodes, including this node.
				if votes >= (len(n.cfg.Peers)+1)/2+1 {
					n.log().Info("majority votes achieved, elected leader!", "node", n.cfg.ID, "term", n.currTerm, "votes", votes)
					n.role = StateLeader
					n.leaderID = n.cfg.ID
					go n.runHeartbeats()
				}
			}
		}(peer)
	}
}

// stepDown records a higher term seen in a peer reply and reverts to Follower.
func (n *Node) stepDown(peerTerm int64, source string) {
	n.log().Info("discovered higher term in "+source+", stepping down", "current", n.currTerm, "peer_term", peerTerm)
	n.currTerm = peerTerm
	n.role = StateFollower
	n.votedFor = ""
}

// HandleRequestVote processes an incoming RequestVote RPC on this node.
func (n *Node) HandleRequestVote(args RequestVoteArgs) RequestVoteReply {
	n.mu.Lock()
	defer n.mu.Unlock()

	if args.Term < n.currTerm {
		return RequestVoteReply{Term: n.currTerm}
	}

	if args.Term > n.currTerm {
		n.currTerm = args.Term
		n.role = StateFollower
		n.votedFor = ""
	}

	if n.votedFor != "" && n.votedFor != args.CandidateID {
		return RequestVoteReply{Term: n.currTerm}
	}

	n.votedFor = args.CandidateID
	n.lastHeartbeat = time.Now()
	n.log().Info("granted vote to candidate", "voter", n.cfg.ID, "candidate", args.CandidateID, "term", args.Term)
	return RequestVoteReply{Term: n.currTerm, VoteGranted: true}
}

// runHeartbeats sends periodic heartbeats to all peers while this node is Leader.
func (n *Node) runHeartbeats() {
	ticker := time.NewTicker(n.cfg.HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-n.ctx.Done():
			return
		case <-ticker.C:
			n.mu.Lock()
			if n.role != StateLeader {
				n.mu.Unlock()
				return
			}
			term := n.currTerm
			peers := append([]string(nil), n.cfg.Peers...)
			n.mu.Unlock()

			// Broadcast heartbeat to all peers in parallel.
			for _, peer := range peers {
				go func(addr string) {
					args := AppendEntriesArgs{
						Term:     term,
						LeaderID: n.cfg.ID,
					}
					reply, err := n.transport.AppendEntries(n.ctx, addr, args)
					if err != nil {
						return
					}

					n.mu.Lock()
					defer n.mu.Unlock()

					if reply.Term > n.currTerm {
						n.stepDown(reply.Term, "heartbeat reply")
					}
				}(peer)
			}
		}
	}
}

// HandleAppendEntries processes an incoming AppendEntries heartbeat on this node.
func (n *Node) HandleAppendEntries(args AppendEntriesArgs) AppendEntriesReply {
	n.mu.Lock()
	defer n.mu.Unlock()

	if args.Term < n.currTerm {
		return AppendEntriesReply{Term: n.currTerm}
	}

	if args.Term > n.currTerm || n.role != StateFollower {
		n.currTerm = args.Term
		n.role = StateFollower
		n.votedFor = ""
	}
	n.leaderID = args.LeaderID

	n.lastHeartbeat = time.Now()

	if n.store != nil && len(args.Entries) > 0 {
		n.applyEntries(args.Entries)
	}
	return AppendEntriesReply{Term: n.currTerm, Success: true}
}

// applyEntries applies replicated WAL entries to the local store.
func (n *Node) applyEntries(entries []WALEntry) {
	for _, entry := range entries {
		switch entry.Op {
		case OpSet:
			_, _ = n.store.Set(entry.Key, entry.Val)
		case OpDel:
			_, _ = n.store.Del(entry.Key)
		}
	}
}

// ReplicateEntry broadcasts a new WAL mutation to all followers in parallel.
func (n *Node) ReplicateEntry(entry WALEntry) {
	n.mu.Lock()
	term := n.currTerm
	peers := append([]string(nil), n.cfg.Peers...)
	n.mu.Unlock()

	for _, peer := range peers {
		go func(addr string) {
			_, _ = n.transport.AppendEntries(n.ctx, addr, AppendEntriesArgs{
				LeaderID: n.cfg.ID,
				Term:     term,
				Entries:  []WALEntry{entry},
			})
		}(peer)
	}
}
