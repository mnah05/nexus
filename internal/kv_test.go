package internal

import (
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestKVMonotonicIndicesAcrossSnapshots(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "test.wal")

	kv, err := NewKV(walPath)
	if err != nil {
		t.Fatalf("NewKV failed: %v", err)
	}
	defer kv.Close()

	// Perform initial writes
	idx1, err := kv.Set("k1", "v1")
	if err != nil || idx1 != 1 {
		t.Fatalf("expected idx 1, got %d, err: %v", idx1, err)
	}

	idx2, err := kv.Set("k2", "v2")
	if err != nil || idx2 != 2 {
		t.Fatalf("expected idx 2, got %d, err: %v", idx2, err)
	}

	// Trigger snapshot
	if err := kv.Snapshot(); err != nil {
		t.Fatalf("snapshot failed: %v", err)
	}

	// Next write MUST NOT reset to 1
	idx3, err := kv.Set("k3", "v3")
	if err != nil || idx3 != 3 {
		t.Fatalf("expected idx 3 after snapshot, got %d, err: %v", idx3, err)
	}

	idx4, err := kv.Del("k1")
	if err != nil || idx4 != 4 {
		t.Fatalf("expected idx 4 after del, got %d, err: %v", idx4, err)
	}

	// Verify state
	if _, ok := kv.Get("k1"); ok {
		t.Fatalf("k1 should be deleted")
	}
	if v, ok := kv.Get("k3"); !ok || v != "v3" {
		t.Fatalf("expected k3=v3, got %s, ok=%v", v, ok)
	}
}

func TestKVRecoveryPreservesMonotonicIndex(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "recovery.wal")

	// Phase 1: create and populate KV, snapshot, add more entries
	kv1, err := NewKV(walPath)
	if err != nil {
		t.Fatalf("NewKV failed: %v", err)
	}

	kv1.Set("user:1", "alice")
	kv1.Set("user:2", "bob")
	if err := kv1.Snapshot(); err != nil {
		t.Fatalf("snapshot failed: %v", err)
	}

	idx3, err := kv1.Set("user:3", "charlie")
	if err != nil || idx3 != 3 {
		t.Fatalf("expected idx 3, got %d", idx3)
	}

	if err := kv1.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}

	// Phase 2: recover from disk
	kv2, err := NewKV(walPath)
	if err != nil {
		t.Fatalf("recovery NewKV failed: %v", err)
	}
	defer kv2.Close()

	// Verify recovered state
	if v, ok := kv2.Get("user:1"); !ok || v != "alice" {
		t.Fatalf("expected user:1=alice, got %s", v)
	}
	if v, ok := kv2.Get("user:2"); !ok || v != "bob" {
		t.Fatalf("expected user:2=bob, got %s", v)
	}
	if v, ok := kv2.Get("user:3"); !ok || v != "charlie" {
		t.Fatalf("expected user:3=charlie, got %s", v)
	}

	// Verify next write continues monotonically
	idx4, err := kv2.Set("user:4", "dave")
	if err != nil || idx4 != 4 {
		t.Fatalf("expected monotonic idx 4 on recovered store, got %d", idx4)
	}
}

func TestKVNonBlockingSnapshotConcurrentWrites(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "concurrent.wal")

	kv, err := NewKV(walPath)
	if err != nil {
		t.Fatalf("NewKV failed: %v", err)
	}
	defer kv.Close()

	// Pre-populate with 1000 keys
	for i := 0; i < 1000; i++ {
		_, err := kv.Set(fmt.Sprintf("init_%d", i), fmt.Sprintf("val_%d", i))
		if err != nil {
			t.Fatalf("init set failed: %v", err)
		}
	}

	var wg sync.WaitGroup
	writeCount := 500
	errChan := make(chan error, writeCount+2)

	// Concurrently write while triggering snapshots
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 3; i++ {
			time.Sleep(5 * time.Millisecond)
			if err := kv.Snapshot(); err != nil {
				errChan <- fmt.Errorf("snapshot error: %w", err)
			}
		}
	}()

	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				key := fmt.Sprintf("w_%d_%d", workerID, j)
				val := fmt.Sprintf("v_%d_%d", workerID, j)
				_, err := kv.Set(key, val)
				if err != nil {
					errChan <- fmt.Errorf("set error: %w", err)
					return
				}
			}
		}(i)
	}

	wg.Wait()
	close(errChan)

	for err := range errChan {
		t.Fatalf("concurrent test encountered error: %v", err)
	}

	// Verify count
	if kv.KeyCount() != 1500 {
		t.Fatalf("expected 1500 keys, got %d", kv.KeyCount())
	}
}

func TestKVGracefulClose(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "close.wal")

	kv, err := NewKV(walPath)
	if err != nil {
		t.Fatalf("NewKV failed: %v", err)
	}

	// Set short timing to verify snapshooter loop stops without hanging
	if err := kv.SetTiming(20 * time.Millisecond); err != nil {
		t.Fatalf("SetTiming failed: %v", err)
	}

	time.Sleep(50 * time.Millisecond)

	if err := kv.Close(); err != nil {
		t.Fatalf("first close failed: %v", err)
	}

	// Idempotent close
	if err := kv.Close(); err != nil {
		t.Fatalf("second close failed: %v", err)
	}

	// Subsequent mutations must be rejected with ErrClosed
	_, err = kv.Set("foo", "bar")
	if !errors.Is(err, ErrClosed) {
		t.Fatalf("expected ErrClosed on Set after Close, got %v", err)
	}

	_, err = kv.Del("foo")
	if !errors.Is(err, ErrClosed) {
		t.Fatalf("expected ErrClosed on Del after Close, got %v", err)
	}

	err = kv.Snapshot()
	if !errors.Is(err, ErrClosed) {
		t.Fatalf("expected ErrClosed on Snapshot after Close, got %v", err)
	}
}
