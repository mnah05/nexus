package internal

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// newTestNode builds a Raft node with default timings and an in-memory transport.
func newTestNode(id string, peers []string, kv *KV) *Node {
	return New(DefaultConfig(id, peers), &testTransport{}, kv)
}

// testTransport forwards Raft RPCs to nodes registered in memory, bypassing HTTP.
type testTransport struct {
	mu    sync.Mutex
	nodes map[string]*Node
}

func (t *testTransport) add(n *Node) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.nodes == nil {
		t.nodes = make(map[string]*Node)
	}
	t.nodes[n.cfg.ID] = n
}

func (t *testTransport) peer(addr string) (*Node, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	n, ok := t.nodes[addr]
	if !ok {
		return nil, fmt.Errorf("unknown peer %s", addr)
	}
	return n, nil
}

func (t *testTransport) RequestVote(ctx context.Context, addr string, args RequestVoteArgs) (RequestVoteReply, error) {
	n, err := t.peer(addr)
	if err != nil {
		return RequestVoteReply{}, err
	}
	return n.HandleRequestVote(args), nil
}

func (t *testTransport) AppendEntries(ctx context.Context, addr string, args AppendEntriesArgs) (AppendEntriesReply, error) {
	n, err := t.peer(addr)
	if err != nil {
		return AppendEntriesReply{}, err
	}
	return n.HandleAppendEntries(args), nil
}

func TestRaftElectionAndHeartbeatDirect(t *testing.T) {
	// Initialize 3 nodes
	node1 := newTestNode("node1", []string{"node2", "node3"}, nil)
	node2 := newTestNode("node2", []string{"node1", "node3"}, nil)
	node3 := newTestNode("node3", []string{"node1", "node2"}, nil)
	defer node1.Close()
	defer node2.Close()
	defer node3.Close()

	// Direct RPC testing: test HandleRequestVote logic
	// Node 1 asks Node 2 for a vote in Term 1
	reply := node2.HandleRequestVote(RequestVoteArgs{
		Term:        1,
		CandidateID: "node1",
	})
	if !reply.VoteGranted {
		t.Fatalf("expected node2 to grant vote to node1 in term 1")
	}

	// Node 3 also asks Node 2 for a vote in Term 1 (should be rejected since node2 already voted for node1)
	reply = node2.HandleRequestVote(RequestVoteArgs{
		Term:        1,
		CandidateID: "node3",
	})
	if reply.VoteGranted {
		t.Fatalf("expected node2 to REJECT vote for node3 because it already voted for node1 in term 1")
	}

	// Node 3 asks in Term 2 (higher term) -> node 2 should update term and grant vote
	reply = node2.HandleRequestVote(RequestVoteArgs{
		Term:        2,
		CandidateID: "node3",
	})
	if !reply.VoteGranted || reply.Term != 2 {
		t.Fatalf("expected node2 to grant vote in higher term 2, got reply: %+v", reply)
	}

	// Test HandleAppendEntries logic
	// Node 3 sends heartbeat as leader of Term 2
	heartbeatReply := node2.HandleAppendEntries(AppendEntriesArgs{
		Term:     2,
		LeaderID: "node3",
	})
	if !heartbeatReply.Success {
		t.Fatalf("expected node2 to accept valid heartbeat from node3")
	}
	if node2.LeaderID() != "node3" {
		t.Fatalf("expected node2 leader to be node3, got %s", node2.LeaderID())
	}

	// Stale heartbeat from old term 1 should be rejected
	staleReply := node2.HandleAppendEntries(AppendEntriesArgs{
		Term:     1,
		LeaderID: "node1",
	})
	if staleReply.Success {
		t.Fatalf("expected node2 to reject stale heartbeat from term 1")
	}
}

func TestRaftSingleNodeElection(t *testing.T) {
	// A single node with 0 peers should elect itself leader immediately on timeout
	node := newTestNode("standalone", []string{}, nil)
	defer node.Close()
	go node.Run()

	// Wait up to 500ms for election timeout to trigger
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if node.IsLeader() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}

	t.Fatalf("standalone node did not elect itself leader within 500ms")
}

func TestRaftMultiNodeElection(t *testing.T) {
	tr := &testTransport{}
	ids := []string{"node1", "node2", "node3"}
	peersOf := func(self string) []string {
		var peers []string
		for _, id := range ids {
			if id != self {
				peers = append(peers, id)
			}
		}
		return peers
	}
	makeNode := func(id string) *Node {
		cfg := DefaultConfig(id, peersOf(id))
		cfg.ElectionMin = 40 * time.Millisecond
		cfg.ElectionMax = 120 * time.Millisecond
		cfg.HeartbeatInterval = 20 * time.Millisecond
		return New(cfg, tr, nil)
	}

	var nodes []*Node
	for _, id := range ids {
		nodes = append(nodes, makeNode(id))
	}
	for _, n := range nodes {
		tr.add(n)
	}
	for _, n := range nodes {
		defer n.Close()
	}
	for _, n := range nodes {
		go n.Run()
	}

	// Wait until every node agrees on the same leader.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		leaders := make(map[string]bool)
		leaderNode := true
		for _, n := range nodes {
			leaders[n.LeaderID()] = true
			if n.LeaderID() == "" {
				leaderNode = false
			}
		}
		if leaderNode && len(leaders) == 1 {
			var leaderID string
			for id := range leaders {
				leaderID = id
			}
			for _, n := range nodes {
				if n.cfg.ID == leaderID && !n.IsLeader() {
					t.Fatalf("node %s reported as leader but is not", leaderID)
				}
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}

	t.Fatalf("cluster did not converge on a single leader within 5s")
}

func TestRaftLogReplicationToFollower(t *testing.T) {
	tmpDir := t.TempDir()
	kv, err := NewKV(filepath.Join(tmpDir, "replica.wal"))
	if err != nil {
		t.Fatalf("NewKV failed: %v", err)
	}
	defer kv.Close()

	// Follower node backed by real KV store
	follower := newTestNode("node2", []string{"node1"}, kv)
	defer follower.Close()

	// Leader sends an entry: SET key1 = val1
	reply := follower.HandleAppendEntries(AppendEntriesArgs{
		Term:     1,
		LeaderID: "node1",
		Entries: []WALEntry{
			{
				Idx:  1,
				Op:   OpSet,
				Term: 1,
				Key:  "key1",
				Val:  "val1",
			},
		},
	})

	if !reply.Success {
		t.Fatalf("expected append entries to succeed on follower")
	}

	// Verify follower's store now has the replicated key!
	val, ok := kv.Get("key1")
	if !ok || val != "val1" {
		t.Fatalf("expected key1=val1 on follower store, got %q (ok=%v)", val, ok)
	}

	// Leader sends another entry: DEL key1
	reply = follower.HandleAppendEntries(AppendEntriesArgs{
		Term:     1,
		LeaderID: "node1",
		Entries: []WALEntry{
			{
				Idx:  2,
				Op:   OpDel,
				Term: 1,
				Key:  "key1",
			},
		},
	})

	if !reply.Success {
		t.Fatalf("expected del append entries to succeed")
	}

	// Verify follower applied the delete
	if _, ok := kv.Get("key1"); ok {
		t.Fatalf("expected key1 to be deleted on follower")
	}
}
