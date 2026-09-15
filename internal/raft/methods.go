package raft

import (
	"context"
	"math/rand"
	"time"
)

// NewNode creates a Raft node in the Follower state. Call Run to start it.
func NewNode(ctx context.Context, id string, peers []string, kv ApplyStore, tr NodeTransport) *RaftNode {
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithCancel(ctx)
	return &RaftNode{
		Config:        DefaultRaftConfig(id, peers),
		Store:         kv,
		Transport:     tr,
		state:         Follower,
		logEntries:    []WALEntry{},
		LastHeartbeat: time.Now(),
		ctx:           ctx,
		cancel:        cancel,
	}
}

// Run starts the background election timer.
func (n *RaftNode) Run() {
	n.wg.Add(1)
	go n.RunElectionTimer()
}

// RunElectionTimer campaigns for leadership whenever no leader has been heard
// from for a randomized election timeout.
func (n *RaftNode) RunElectionTimer() {
	defer n.wg.Done()

	for {
		timeout := randomTimeout(n.Config.ElectionMinTime, n.Config.ElectionMaxTime)

		select {
		case <-n.ctx.Done():
			return
		case <-time.After(timeout):
		}

		n.mu.Lock()
		if n.state != Leader && time.Since(n.LastHeartbeat) >= timeout {
			n.startElection()
		}
		n.mu.Unlock()
	}
}

// startElection transitions this node to Candidate and requests votes from every
// peer. The caller must hold n.mu.
func (n *RaftNode) startElection() {
	n.state = Candidate
	n.currTerm++
	n.votedFor = n.Config.ID
	n.LastHeartbeat = time.Now()

	votes := 1
	term := n.currTerm
	lastLogIndex := n.lastLogIndex
	lastLogTerm := n.lastLogTerm
	peers := append([]string(nil), n.Config.Peers...)

	// A single-node cluster is leader immediately.
	if len(peers) == 0 {
		n.state = Leader
		n.leaderID = n.Config.ID
		n.startHeartbeats()
		return
	}

	for _, peer := range peers {
		go func(addr string) {
			reply, err := n.Transport.RequestVote(n.ctx, addr, RequestArgs{
				Term:         term,
				CandidateID:  n.Config.ID,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			})
			if err != nil {
				return
			}

			n.mu.Lock()
			defer n.mu.Unlock()

			if reply.Term > n.currTerm {
				n.currTerm = reply.Term
				n.state = Follower
				n.votedFor = ""
				return
			}

			if reply.VoteGranted && n.state == Candidate && n.currTerm == term {
				votes++
				if votes >= (len(n.Config.Peers)+1)/2+1 {
					n.state = Leader
					n.leaderID = n.Config.ID
					n.startHeartbeats()
				}
			}
		}(peer)
	}
}

// startHeartbeats starts the leader loop. The caller must hold n.mu.
func (n *RaftNode) startHeartbeats() {
	n.wg.Add(1)
	go n.runHeartbeats()
}

// runHeartbeats sends AppendEntries heartbeats while this node is leader.
func (n *RaftNode) runHeartbeats() {
	defer n.wg.Done()
	ticker := time.NewTicker(n.Config.HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-n.ctx.Done():
			return
		case <-ticker.C:
		}

		n.mu.Lock()
		if n.state != Leader {
			n.mu.Unlock()
			return
		}
		term := n.currTerm
		leaderID := n.Config.ID
		peers := append([]string(nil), n.Config.Peers...)
		var prevTerm uint64
		if n.lastLogIndex > 0 {
			prevTerm = uint64(n.logEntries[n.lastLogIndex-1].Term)
		}
		prevIndex := n.lastLogIndex
		commitIndex := n.commitIndex
		n.mu.Unlock()

		for _, peer := range peers {
			go func(addr string) {
				_, _ = n.Transport.AppendEntries(n.ctx, addr, AppendArgs{
					Term: term, LeaderID: leaderID,
					PrevLogIndex: prevIndex, PrevLogTerm: prevTerm,
					LeaderCommit: commitIndex,
				})
			}(peer)
		}
	}
}

// Close stops the background loops started by Run.
func (n *RaftNode) Close() {
	n.cancel()
	n.wg.Wait()
}

// randomTimeout returns a duration in [min, max).
func randomTimeout(min, max time.Duration) time.Duration {
	if max <= min {
		if min > 0 {
			return min
		}
		return time.Millisecond
	}
	return min + time.Duration(rand.Int63n(int64(max-min)))
}

func (n *RaftNode) isLeader() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.state == Leader
}

// IsLeader reports whether this node currently believes it is leader.
func (n *RaftNode) IsLeader() bool { return n.isLeader() }

// LeaderID returns the last known leader ID.
func (n *RaftNode) LeaderID() string {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.leaderID
}

// Term returns the node's current term.
func (n *RaftNode) Term() int64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.currTerm
}

// Status returns a consistent snapshot for the HTTP status endpoint.
func (n *RaftNode) Status() RaftStatus {
	n.mu.Lock()
	defer n.mu.Unlock()
	return RaftStatus{
		ID:       n.Config.ID,
		Role:     n.state,
		Term:     n.currTerm,
		LeaderID: n.leaderID,
		Peers:    append([]string(nil), n.Config.Peers...),
	}
}

// ReplicateEntry commits entry only after a majority, including this leader,
// has acknowledged it. The returned index is the Raft log index, not a store
// implementation detail.
func (n *RaftNode) ReplicateEntry(ctx context.Context, entry WALEntry) (uint64, error) {
	n.proposalMu.Lock()
	defer n.proposalMu.Unlock()

	if ctx == nil {
		ctx = n.ctx
	}
	n.mu.Lock()
	if n.state != Leader {
		n.mu.Unlock()
		return 0, ErrNotLeader
	}
	// A leader only commits entries in its current term. Do not accept a term
	// supplied by an API caller: it would weaken Raft's commitment rule.
	entry.Term = int(n.currTerm)
	term := n.currTerm
	index := n.lastLogIndex + 1
	var prevTerm uint64
	if index > 1 {
		prevTerm = uint64(n.logEntries[index-2].Term)
	}
	n.logEntries = append(n.logEntries, entry)
	n.lastLogIndex = index
	entry.Idx = index
	n.logEntries[len(n.logEntries)-1] = entry
	n.lastLogTerm = uint64(entry.Term)
	peers := append([]string(nil), n.Config.Peers...)
	n.mu.Unlock()

	type result struct {
		reply AppendReply
		err   error
	}
	replies := make(chan result, len(peers))
	for _, peer := range peers {
		go func(addr string) {
			reply, err := n.Transport.AppendEntries(ctx, addr, AppendArgs{
				Term: term, LeaderID: n.Config.ID,
				PrevLogIndex: index - 1, PrevLogTerm: prevTerm,
				Entries: []WALEntry{entry},
			})
			replies <- result{reply: reply, err: err}
		}(peer)
	}

	acks := 1 // the leader's append above is its own acknowledgement
	for range peers {
		select {
		case reply := <-replies:
			if reply.err != nil {
				continue
			}
			if reply.reply.Term > term {
				n.mu.Lock()
				if reply.reply.Term > n.currTerm {
					n.currTerm = reply.reply.Term
					n.state = Follower
					n.votedFor = ""
				}
				n.mu.Unlock()
			} else if reply.reply.Success {
				acks++
			}
		case <-ctx.Done():
			n.rollbackProposal(term, index)
			return 0, ctx.Err()
		}
	}

	n.mu.Lock()
	if n.state != Leader || n.currTerm != term {
		n.mu.Unlock()
		n.rollbackProposal(term, index)
		return 0, ErrNotLeader
	}
	if acks < (len(peers)+1)/2+1 {
		n.mu.Unlock()
		n.rollbackProposal(term, index)
		return 0, ErrNoQuorum
	}
	entries := n.commitThroughLocked(index)
	commitIndex := n.commitIndex
	n.mu.Unlock()

	if err := n.apply(entries); err != nil {
		return 0, err
	}
	// Notify replicas immediately; the periodic heartbeat is only a retry.
	for _, peer := range peers {
		go func(addr string) {
			_, _ = n.Transport.AppendEntries(n.ctx, addr, AppendArgs{
				Term: term, LeaderID: n.Config.ID,
				PrevLogIndex: index, PrevLogTerm: uint64(entry.Term),
				LeaderCommit: commitIndex,
			})
		}(peer)
	}
	return index, nil
}

func (n *RaftNode) rollbackProposal(term int64, index uint64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.currTerm != term || n.lastLogIndex < index || n.commitIndex >= index {
		return
	}
	n.logEntries = n.logEntries[:index-1]
	n.lastLogIndex = index - 1
	if n.lastLogIndex == 0 {
		n.lastLogTerm = 0
	} else {
		n.lastLogTerm = uint64(n.logEntries[n.lastLogIndex-1].Term)
	}
}

// commitThroughLocked advances the commit point and returns entries that must
// be applied locally. The caller holds mu.
func (n *RaftNode) commitThroughLocked(index uint64) []WALEntry {
	if index <= n.commitIndex {
		return nil
	}
	old := n.commitIndex
	n.commitIndex = min(index, n.lastLogIndex)
	return append([]WALEntry(nil), n.logEntries[old:n.commitIndex]...)
}

func (n *RaftNode) apply(entries []WALEntry) error {
	if n.Store == nil {
		return nil
	}
	for _, entry := range entries {
		if store, ok := n.Store.(interface {
			ApplyRaft(op string, idx uint64, term int, key, val string) error
		}); ok {
			if err := store.ApplyRaft(string(entry.Op), entry.Idx, entry.Term, entry.Key, entry.Val); err != nil {
				return err
			}
			continue
		}
		var err error
		switch entry.Op {
		case OpSet:
			_, err = n.Store.Set(entry.Key, entry.Val)
		case OpDel:
			_, err = n.Store.Del(entry.Key)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func (n *RaftNode) HandleRequestVote(ctx context.Context, args RequestArgs) (RequestReply, error) {
	// Stop early if the request or node is shutting down.
	if err := ctx.Err(); err != nil {
		return RequestReply{}, err
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	reply := RequestReply{
		Term:        n.currTerm,
		VoteGranted: false,
	}

	if args.Term < n.currTerm {
		return reply, nil
	}

	if args.Term > n.currTerm {
		n.currTerm = args.Term
		n.state = Follower
		n.votedFor = ""
	}

	if n.votedFor == "" || n.votedFor == args.CandidateID {
		if args.LastLogTerm > n.lastLogTerm || (args.LastLogTerm == n.lastLogTerm && args.LastLogIndex >= n.lastLogIndex) {
			n.votedFor = args.CandidateID
			reply.VoteGranted = true
			n.LastHeartbeat = time.Now()
		}
	}

	// Report the (possibly updated) term, not the one captured before we adopted it.
	reply.Term = n.currTerm
	return reply, nil
}

// HandleAppendEntries processes an incoming AppendEntries RPC (heartbeat or log
// replication) from the leader.
func (n *RaftNode) HandleAppendEntries(ctx context.Context, args AppendArgs) (AppendReply, error) {
	// Stop early if the request or node is shutting down.
	if err := ctx.Err(); err != nil {
		return AppendReply{}, err
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	reply := AppendReply{Term: n.currTerm, Success: false}

	// Stale leader: reject and report our term so it can step down.
	if args.Term < n.currTerm {
		return reply, nil
	}

	// Valid leader contact: adopt its term, follow it, and reset the election
	// timer. A candidate at the same term also steps down here.
	if args.Term > n.currTerm || n.state != Follower {
		n.currTerm = args.Term
		n.state = Follower
		n.votedFor = ""
	}
	n.leaderID = args.LeaderID
	n.LastHeartbeat = time.Now()

	// Consistency check: the entry just before the new ones must match ours.
	if args.PrevLogIndex > n.lastLogIndex {
		return reply, nil
	}
	if args.PrevLogIndex > 0 && uint64(n.logEntries[args.PrevLogIndex-1].Term) != args.PrevLogTerm {
		return reply, nil
	}

	// Append new entries, truncating any conflicting suffix.
	for i, entry := range args.Entries {
		index := args.PrevLogIndex + uint64(i) + 1
		if index <= n.lastLogIndex {
			if n.logEntries[index-1].Term == entry.Term {
				continue // already have this entry
			}
			n.logEntries = n.logEntries[:index-1] // drop the conflicting suffix
			n.lastLogIndex = index - 1
		}
		n.logEntries = append(n.logEntries, entry)
		n.lastLogIndex = index
		n.lastLogTerm = uint64(entry.Term)
	}
	// Remove a stale uncommitted suffix when the leader's log ends earlier.
	end := args.PrevLogIndex + uint64(len(args.Entries))
	if n.lastLogIndex > end {
		if end < n.commitIndex {
			return reply, nil
		}
		n.logEntries = n.logEntries[:end]
		n.lastLogIndex = end
		if end == 0 {
			n.lastLogTerm = 0
		} else {
			n.lastLogTerm = uint64(n.logEntries[end-1].Term)
		}
	}

	// Advance the commit index and apply whatever just became committed.
	if args.LeaderCommit > n.commitIndex {
		entries := n.commitThroughLocked(args.LeaderCommit)
		// The store is part of the local state machine. Its failures cannot be
		// repaired by pretending the append succeeded.
		if err := n.apply(entries); err != nil {
			return reply, err
		}
	}

	reply.Term = n.currTerm
	reply.Success = true
	return reply, nil
}
