package raft

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

type testStore struct {
	mu   sync.Mutex
	data map[string]string
}

func (s *testStore) Set(key, val string) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.data == nil {
		s.data = make(map[string]string)
	}
	s.data[key] = val
	return uint64(len(s.data)), nil
}

func (s *testStore) Get(key string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	val, ok := s.data[key]
	return val, ok
}

func (s *testStore) Del(key string) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.data, key)
	return uint64(len(s.data)), nil
}

type testTransport struct {
	mu    sync.Mutex
	nodes map[string]*RaftNode
}

func (t *testTransport) add(node *RaftNode) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.nodes == nil {
		t.nodes = make(map[string]*RaftNode)
	}
	t.nodes[node.Config.ID] = node
}

func (t *testTransport) peer(addr string) (*RaftNode, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	node, ok := t.nodes[addr]
	if !ok {
		return nil, fmt.Errorf("unknown peer %s", addr)
	}
	return node, nil
}

func (t *testTransport) RequestVote(ctx context.Context, addr string, args RequestArgs) (RequestReply, error) {
	node, err := t.peer(addr)
	if err != nil {
		return RequestReply{}, err
	}
	return node.HandleRequestVote(ctx, args)
}

func (t *testTransport) AppendEntries(ctx context.Context, addr string, args AppendArgs) (AppendReply, error) {
	node, err := t.peer(addr)
	if err != nil {
		return AppendReply{}, err
	}
	return node.HandleAppendEntries(ctx, args)
}

func TestRequestVoteAndAppendEntries(t *testing.T) {
	transport := &testTransport{}
	ctx := context.Background()
	node := NewNode(ctx, "node-2", []string{"node-1"}, nil, transport)
	transport.add(node)

	reply, err := node.HandleRequestVote(ctx, RequestArgs{Term: 1, CandidateID: "node-1"})
	if err != nil || !reply.VoteGranted {
		t.Fatalf("expected vote to be granted, reply=%+v err=%v", reply, err)
	}

	reply, err = node.HandleRequestVote(ctx, RequestArgs{Term: 1, CandidateID: "node-3"})
	if err != nil || reply.VoteGranted {
		t.Fatalf("expected second candidate to be rejected, reply=%+v err=%v", reply, err)
	}

	appendReply, err := node.HandleAppendEntries(ctx, AppendArgs{
		Term:     2,
		LeaderID: "node-1",
	})
	if err != nil || !appendReply.Success {
		t.Fatalf("expected heartbeat to succeed, reply=%+v err=%v", appendReply, err)
	}
	if node.LeaderID() != "node-1" {
		t.Fatalf("expected leader node-1, got %q", node.LeaderID())
	}
}

func TestAppendEntriesAppliesSetAndDelete(t *testing.T) {
	store := &testStore{}
	node := NewNode(context.Background(), "node-2", nil, store, nil)

	reply, err := node.HandleAppendEntries(context.Background(), AppendArgs{
		Term:         1,
		LeaderID:     "node-1",
		Entries:      []WALEntry{{Idx: 1, Op: OpSet, Term: 1, Key: "key", Val: "value"}},
		LeaderCommit: 1,
	})
	if err != nil || !reply.Success {
		t.Fatalf("set append failed, reply=%+v err=%v", reply, err)
	}
	if value, ok := store.Get("key"); !ok || value != "value" {
		t.Fatalf("expected key=value, got %q ok=%v", value, ok)
	}

	reply, err = node.HandleAppendEntries(context.Background(), AppendArgs{
		Term:         1,
		LeaderID:     "node-1",
		PrevLogIndex: 1,
		PrevLogTerm:  1,
		Entries:      []WALEntry{{Idx: 2, Op: OpDel, Term: 1, Key: "key"}},
		LeaderCommit: 2,
	})
	if err != nil || !reply.Success {
		t.Fatalf("delete append failed, reply=%+v err=%v", reply, err)
	}
	if _, ok := store.Get("key"); ok {
		t.Fatal("expected key to be deleted")
	}
}

func TestSingleNodeElection(t *testing.T) {
	node := NewNode(context.Background(), "standalone", nil, nil, nil)
	node.Config.ElectionMinTime = 5 * time.Millisecond
	node.Config.ElectionMaxTime = 10 * time.Millisecond
	go node.Run()
	defer node.Close()

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if node.IsLeader() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("single node did not become leader")
}

func TestReplicateEntryCommitsAfterMajorityAndAppliesReplicas(t *testing.T) {
	transport := &testTransport{}
	leaderStore, followerOneStore, followerTwoStore := &testStore{}, &testStore{}, &testStore{}
	leader := NewNode(context.Background(), "node-1", []string{"node-2", "node-3", "node-4"}, leaderStore, transport)
	followerOne := NewNode(context.Background(), "node-2", []string{"node-1", "node-3"}, followerOneStore, transport)
	followerTwo := NewNode(context.Background(), "node-3", []string{"node-1", "node-2"}, followerTwoStore, transport)
	transport.add(leader)
	transport.add(followerOne)
	transport.add(followerTwo)

	leader.mu.Lock()
	leader.state = Leader
	leader.currTerm = 1
	leader.leaderID = "node-1"
	leader.mu.Unlock()

	idx, err := leader.ReplicateEntry(context.Background(), WALEntry{Op: OpSet, Key: "quorum", Val: "committed"})
	if err != nil || idx != 1 {
		t.Fatalf("ReplicateEntry() = (%d, %v), want (1, nil)", idx, err)
	}
	if value, ok := leaderStore.Get("quorum"); !ok || value != "committed" {
		t.Fatalf("leader did not apply committed entry: value=%q ok=%v", value, ok)
	}

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		one, okOne := followerOneStore.Get("quorum")
		two, okTwo := followerTwoStore.Get("quorum")
		if okOne && one == "committed" && okTwo && two == "committed" {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("followers did not apply the leader's committed entry")
}

func TestReplicateEntryDoesNotApplyWithoutQuorum(t *testing.T) {
	transport := &testTransport{}
	leaderStore, followerStore := &testStore{}, &testStore{}
	leader := NewNode(context.Background(), "node-1", []string{"node-2", "node-3", "node-4"}, leaderStore, transport)
	follower := NewNode(context.Background(), "node-2", nil, followerStore, transport)
	transport.add(leader)
	transport.add(follower) // nodes 3 and 4 are deliberately unavailable

	leader.mu.Lock()
	leader.state = Leader
	leader.currTerm = 1
	leader.leaderID = "node-1"
	leader.mu.Unlock()

	if _, err := leader.ReplicateEntry(context.Background(), WALEntry{Op: OpSet, Key: "unsafe", Val: "write"}); err != ErrNoQuorum {
		t.Fatalf("ReplicateEntry() error = %v, want %v", err, ErrNoQuorum)
	}
	if _, ok := leaderStore.Get("unsafe"); ok {
		t.Fatal("leader applied an entry without a quorum")
	}
	if _, ok := followerStore.Get("unsafe"); ok {
		t.Fatal("follower applied an uncommitted entry")
	}
	leader.mu.Lock()
	leaderCommit := leader.commitIndex
	leader.mu.Unlock()
	follower.mu.Lock()
	followerCommit := follower.commitIndex
	follower.mu.Unlock()
	if leaderCommit != 0 || followerCommit != 0 {
		t.Fatalf("commit indexes advanced without a quorum: leader=%d follower=%d", leaderCommit, followerCommit)
	}
}
