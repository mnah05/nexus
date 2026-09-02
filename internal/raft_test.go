package internal

import (
	"testing"
	"time"
)

func TestRaftElectionAndHeartbeatDirect(t *testing.T) {
	// Initialize 3 nodes
	node1 := NewNode("node1", []string{"node2", "node3"})
	node2 := NewNode("node2", []string{"node1", "node3"})
	node3 := NewNode("node3", []string{"node1", "node2"})
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
	node := NewNode("standalone", []string{})
	defer node.Close()

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
