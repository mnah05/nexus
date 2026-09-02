package internal

import (
	"bytes"
	"encoding/json"
	"fmt"
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

// Node represents a single Raft consensus node.
type Node struct {
	mu    sync.Mutex
	ID    string   // This node's address/identifier (e.g. "localhost:8001")
	peers []string // Addresses of peer nodes in the cluster

	Role NodeState

	currTerm int64
	votedFor string
	leaderID string

	lastHeartbeat time.Time

	httpClient *http.Client
	stop       chan struct{}
	wg         sync.WaitGroup
	kv         *KV
}

// NewNode initializes a new Raft node in the Follower state.
func NewNode(id string, peers []string, kv *KV) *Node {
	n := &Node{
		ID:            id,
		peers:         peers,
		Role:          StateFollower,
		currTerm:      0,
		votedFor:      "",
		leaderID:      "",
		lastHeartbeat: time.Now(),
		httpClient:    &http.Client{Timeout: 100 * time.Millisecond},
		stop:          make(chan struct{}),
		kv:            kv,
	}

	// Start background election timer
	n.wg.Add(1)
	go n.runElectionTimer()

	return n
}

// Close cleanly stops background Raft loops (election timer and heartbeats).
func (n *Node) Close() {
	n.mu.Lock()
	select {
	case <-n.stop:
		n.mu.Unlock()
		return
	default:
		close(n.stop)
	}
	n.mu.Unlock()
	n.wg.Wait()
}

// IsLeader reports whether this node is currently the cluster leader.
func (n *Node) IsLeader() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.Role == StateLeader
}

// LeaderID returns the address of the current known leader.
func (n *Node) LeaderID() string {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.leaderID
}

// GetRole returns the current node role.
func (n *Node) GetRole() NodeState {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.Role
}

// Term returns the current term of the node.
func (n *Node) Term() int64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.currTerm
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

func randomTimeout(min, max time.Duration) time.Duration {
	diff := max - min
	return min + time.Duration(rand.Int63n(int64(diff)))
}

// runElectionTimer continuously checks if the leader heartbeat has timed out.
func (n *Node) runElectionTimer() {
	defer n.wg.Done()

	for {
		timeout := randomTimeout(150*time.Millisecond, 300*time.Millisecond)

		select {
		case <-n.stop:
			return
		case <-time.After(timeout):
		}

		n.mu.Lock()
		// RULE: Only Followers and Candidates start elections when their timer expires.
		// Leaders never run election timers because they are the authority.
		if n.Role != StateLeader && time.Since(n.lastHeartbeat) >= timeout {
			slog.Info("election timeout elapsed, starting election", "node", n.ID, "term", n.currTerm+1)
			n.startElection()
		}
		n.mu.Unlock()
	}
}

// startElection transitions the node to Candidate and requests votes from peers.
// Assumes n.mu is already locked by the caller.
func (n *Node) startElection() {
	// RULE: On conversion to candidate, start election:
	// 1. Increment currentTerm
	// 2. Vote for self
	// 3. Reset election timer
	n.Role = StateCandidate
	n.currTerm++
	n.votedFor = n.ID
	n.lastHeartbeat = time.Now()

	votes := 1
	term := n.currTerm
	peers := n.peers

	// If there are no peers (single node cluster), become leader immediately.
	if len(peers) == 0 {
		n.Role = StateLeader
		n.leaderID = n.ID
		slog.Info("single node cluster: elected self as leader", "node", n.ID, "term", n.currTerm)
		go n.runHeartbeats()
		return
	}

	// RULE: Send RequestVote RPCs to all other servers in parallel.
	for _, peer := range peers {
		go func(addr string) {
			args := RequestVoteArgs{
				Term:        term,
				CandidateID: n.ID,
			}
			reply, err := n.sendRequestVote(addr, args)
			if err != nil {
				return // Peer may be unreachable or offline
			}

			n.mu.Lock()
			defer n.mu.Unlock()

			// RULE: If RPC response contains term T > currentTerm, step down to Follower.
			if reply.Term > n.currTerm {
				slog.Info("discovered higher term in RequestVote reply, stepping down", "current", n.currTerm, "peer_term", reply.Term)
				n.currTerm = reply.Term
				n.Role = StateFollower
				n.votedFor = ""
				return
			}

			// RULE: If votes received from majority of servers: become leader.
			if reply.VoteGranted && n.Role == StateCandidate && n.currTerm == term {
				votes++
				majority := (len(n.peers)+1)/2 + 1
				if votes >= majority {
					slog.Info("majority votes achieved, elected leader!", "node", n.ID, "term", n.currTerm, "votes", votes)
					n.Role = StateLeader
					n.leaderID = n.ID
					go n.runHeartbeats()
				}
			}
		}(peer)
	}
}

// sendRequestVote sends a RequestVote RPC to a peer over HTTP.
func (n *Node) sendRequestVote(addr string, args RequestVoteArgs) (*RequestVoteReply, error) {
	data, err := json.Marshal(args)
	if err != nil {
		return nil, err
	}

	target := addr
	if !strings.HasPrefix(target, "http://") && !strings.HasPrefix(target, "https://") {
		target = "http://" + target
	}
	url := fmt.Sprintf("%s/raft/request-vote", target)

	resp, err := n.httpClient.Post(url, "application/json", bytes.NewBuffer(data))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var reply RequestVoteReply
	if err := json.NewDecoder(resp.Body).Decode(&reply); err != nil {
		return nil, err
	}
	return &reply, nil
}

// HandleRequestVote processes an incoming RequestVote RPC on this node.
func (n *Node) HandleRequestVote(args RequestVoteArgs) RequestVoteReply {
	n.mu.Lock()
	defer n.mu.Unlock()

	reply := RequestVoteReply{
		Term:        n.currTerm,
		VoteGranted: false,
	}

	// RULE 1: Reply false if term < currentTerm.
	if args.Term < n.currTerm {
		return reply
	}

	// RULE 2: If term > currentTerm, update currentTerm and transition to Follower.
	if args.Term > n.currTerm {
		n.currTerm = args.Term
		n.Role = StateFollower
		n.votedFor = ""
	}

	// RULE 3: If votedFor is null or candidateId, grant vote and reset election timer.
	if n.votedFor == "" || n.votedFor == args.CandidateID {
		n.votedFor = args.CandidateID
		n.lastHeartbeat = time.Now()
		reply.VoteGranted = true
		slog.Info("granted vote to candidate", "voter", n.ID, "candidate", args.CandidateID, "term", args.Term)
	}

	reply.Term = n.currTerm
	return reply
}

// runHeartbeats sends periodic heartbeats to all peers while this node is Leader.
func (n *Node) runHeartbeats() {
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-n.stop:
			return
		case <-ticker.C:
			n.mu.Lock()
			// If leadership was lost, terminate the heartbeat loop.
			if n.Role != StateLeader {
				n.mu.Unlock()
				return
			}
			term := n.currTerm
			leaderID := n.ID
			peers := n.peers
			n.mu.Unlock()

			// Broadcast heartbeat to all peers in parallel.
			for _, peer := range peers {
				go func(addr string) {
					args := AppendEntriesArgs{
						Term:     term,
						LeaderID: leaderID,
					}
					reply, err := n.sendAppendEntries(addr, args)
					if err != nil {
						return // Peer might be offline
					}

					n.mu.Lock()
					defer n.mu.Unlock()

					// RULE: If peer returns a higher term, step down to Follower immediately.
					if reply.Term > n.currTerm {
						slog.Info("discovered higher term in heartbeat reply, stepping down", "current", n.currTerm, "peer_term", reply.Term)
						n.currTerm = reply.Term
						n.Role = StateFollower
						n.votedFor = ""
					}
				}(peer)
			}
		}
	}
}

// sendAppendEntries sends an AppendEntries heartbeat RPC to a peer over HTTP.
func (n *Node) sendAppendEntries(addr string, args AppendEntriesArgs) (*AppendEntriesReply, error) {
	data, err := json.Marshal(args)
	if err != nil {
		return nil, err
	}

	target := addr
	if !strings.HasPrefix(target, "http://") && !strings.HasPrefix(target, "https://") {
		target = "http://" + target
	}
	url := fmt.Sprintf("%s/raft/append-entries", target)

	resp, err := n.httpClient.Post(url, "application/json", bytes.NewBuffer(data))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var reply AppendEntriesReply
	if err := json.NewDecoder(resp.Body).Decode(&reply); err != nil {
		return nil, err
	}
	return &reply, nil
}

// HandleAppendEntries processes an incoming AppendEntries heartbeat on this node.
func (n *Node) HandleAppendEntries(args AppendEntriesArgs) AppendEntriesReply {
	n.mu.Lock()
	defer n.mu.Unlock()

	reply := AppendEntriesReply{
		Term:    n.currTerm,
		Success: false,
	}

	// RULE 1: Reply false if term < currentTerm.
	if args.Term < n.currTerm {
		return reply
	}

	// RULE 2: If term > currentTerm or we were a candidate, step down to Follower.
	if args.Term > n.currTerm || n.Role != StateFollower {
		n.currTerm = args.Term
		n.Role = StateFollower
		n.votedFor = ""
	}
	n.leaderID = args.LeaderID

	// RULE 3: Valid heartbeat received from current leader, reset election countdown timer.
	n.lastHeartbeat = time.Now()

	// RULE 4: If entries is not empty, apply each entry to our local store!
	if n.kv != nil && len(args.Entries) > 0 {
		for _, entry := range args.Entries {
			switch entry.Op {
			case OpSet:
				n.kv.Set(entry.Key, entry.Val)
			case OpDel:
				n.kv.Del(entry.Key)
			}
		}
	}
	reply.Success = true
	reply.Term = n.currTerm
	return reply
}

// ReplicateEntry broadcasts a new WAL mutation to all followers in parallel.
func (n *Node) ReplicateEntry(entry WALEntry) {
	n.mu.Lock()
	term := n.currTerm
	leaderID := n.ID
	peers := n.peers
	n.mu.Unlock()
	for _, peer := range peers {
		go func(addr string) {
			args := AppendEntriesArgs{
				Term:     term,
				LeaderID: leaderID,
				Entries:  []WALEntry{entry},
			}
			_, _ = n.sendAppendEntries(addr, args)
		}(peer)
	}
}
