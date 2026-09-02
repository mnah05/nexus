package internal

import (
	"fmt"
	"log/slog"
	"sync"
	"time"
)

// DefaultSnapshotInterval is how often the periodic snapshot runs (5 min).
const DefaultSnapshotInterval = 5 * time.Minute

// KV is the key-value service: every mutation is persisted to the
// write-ahead log before being applied to the in-memory store.
// Periodically (default 5 min) the store is snapshotted to disk and
// the log is cleared, keeping both bounded.
type KV struct {
	mu       sync.Mutex // serializes mutations and protects closed state
	snapMu   sync.Mutex // serializes background snapshot executions
	store    *Store[string]
	wal      *WAL
	snapPath string
	interval time.Duration
	stop     chan struct{}
	wg       sync.WaitGroup
	closed   bool
}

// NewKV opens (or creates) the WAL, recovers state, and starts the
// periodic snapshot goroutine.
func NewKV(walPath string) (*KV, error) {
	wal, err := OpenWal(walPath)
	if err != nil {
		return nil, err
	}
	kv := &KV{
		store:    NewMap[string](),
		wal:      wal,
		snapPath: walPath + ".snap",
		interval: DefaultSnapshotInterval,
	}
	if err := kv.Recover(); err != nil {
		_ = wal.Close()
		return nil, err
	}
	kv.startSnapshooter()
	return kv, nil
}

// Recover restores state on startup: it loads the latest snapshot and
// then replays subsequent WAL entries on top.
// It also sets the WAL's nextIdx to be strictly monotonic.
func (kv *KV) Recover() error {
	snap, err := LoadSnapshot[string](kv.snapPath)
	if err != nil {
		return fmt.Errorf("kv: load snapshot: %w", err)
	}

	for k, v := range snap.Entries {
		kv.store.Set(k, v)
	}

	entries, err := kv.wal.ReadAll()
	if err != nil {
		return fmt.Errorf("kv: read wal: %w", err)
	}

	maxIdx := snap.LastIdx
	for _, e := range entries {
		if e.Idx > snap.LastIdx {
			switch e.Op {
			case OpSet:
				kv.store.Set(e.Key, e.Val)
			case OpDel:
				kv.store.Del(e.Key)
			default:
				return fmt.Errorf("kv: recover: unknown op %q at idx %d", e.Op, e.Idx)
			}
		}
		if e.Idx > maxIdx {
			maxIdx = e.Idx
		}
	}

	// Ensure next index is monotonic
	kv.wal.SetNextIdx(maxIdx + 1)
	slog.Info("kv recovered", "snapshot_keys", len(snap.Entries), "wal_entries", len(entries), "next_idx", maxIdx+1)
	return nil
}

// Get reads straight from the in-memory store (reads never touch the WAL).
func (kv *KV) Get(key string) (string, bool) {
	return kv.store.Get(key)
}

// List returns a point-in-time snapshot of the whole store.
func (kv *KV) List() map[string]string {
	return kv.store.Snapshot()
}

// Set appends to the WAL first, then applies to the store.
// Returns the WAL index of the written entry.
func (kv *KV) Set(key, val string) (uint64, error) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.closed {
		return 0, ErrClosed
	}
	idx, err := kv.wal.Append(OpSet, 0, key, val)
	if err != nil {
		return 0, err
	}
	kv.store.Set(key, val)
	return idx, nil
}

// Del logs the delete before removing the key from the store.
func (kv *KV) Del(key string) (uint64, error) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.closed {
		return 0, ErrClosed
	}
	idx, err := kv.wal.Append(OpDel, 0, key, "")
	if err != nil {
		return 0, err
	}
	kv.store.Del(key)
	return idx, nil
}

// Snapshot persists the store to disk without blocking concurrent writes.
// kv.mu is only held briefly to copy the in-memory map and capture the WAL watermark.
// The JSON serialization, disk file write, and WAL compaction proceed concurrently with Set/Del.
func (kv *KV) Snapshot() error {
	kv.snapMu.Lock()
	defer kv.snapMu.Unlock()

	// 1. Briefly hold kv.mu to capture in-memory store snapshot and WAL watermark
	kv.mu.Lock()
	if kv.closed {
		kv.mu.Unlock()
		return ErrClosed
	}
	entries := kv.store.Snapshot()
	watermark := kv.wal.LastIdx()
	kv.mu.Unlock()

	// 2. Perform disk I/O and JSON serialization WITHOUT holding kv.mu!
	// Mutations (Set/Del) continue concurrently without delay.
	snap := Snapshot[string]{
		LastIdx: watermark,
		Entries: entries,
	}
	if err := SaveSnapshot(kv.snapPath, snap); err != nil {
		return fmt.Errorf("kv: save snapshot: %w", err)
	}

	// 3. Compact the WAL up to watermark, preserving any writes that occurred during snapshot
	if err := kv.wal.TruncateBefore(watermark); err != nil {
		return fmt.Errorf("kv: truncate wal: %w", err)
	}

	slog.Info("snapshot complete", "keys", len(entries), "last_idx", watermark)
	return nil
}

// SetTiming re-configures the snapshot interval. Passing 0 disables
// automatic snapshots; a negative interval is rejected.
func (kv *KV) SetTiming(d time.Duration) error {
	if d < 0 {
		return fmt.Errorf("kv: interval must be >= 0")
	}

	kv.mu.Lock()
	if kv.closed {
		kv.mu.Unlock()
		return ErrClosed
	}
	kv.interval = d
	if kv.stop != nil {
		close(kv.stop)
		kv.stop = nil
	}
	kv.mu.Unlock()

	// Wait for any previous snapshooter loop to finish
	kv.wg.Wait()

	if d > 0 {
		kv.mu.Lock()
		if !kv.closed {
			kv.startSnapshooterLocked()
		}
		kv.mu.Unlock()
	}
	return nil
}

// Interval returns the current snapshot interval.
func (kv *KV) Interval() time.Duration {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	return kv.interval
}

// Closed reports whether the KV service has been closed.
func (kv *KV) Closed() bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	return kv.closed
}

// KeyCount returns the current number of keys in the in-memory store.
func (kv *KV) KeyCount() int {
	return len(kv.store.Snapshot())
}

// NextIdx returns the index that the next mutation will receive.
func (kv *KV) NextIdx() uint64 {
	return kv.wal.NextIdx()
}

// Close gracefully stops the snapshot worker, waits for any in-flight snapshot,
// marks the service closed, and closes the WAL file cleanly.
func (kv *KV) Close() error {
	kv.mu.Lock()
	if kv.closed {
		kv.mu.Unlock()
		return nil
	}
	if kv.stop != nil {
		close(kv.stop)
		kv.stop = nil
	}
	kv.mu.Unlock()

	// Wait for snapshooter loop to exit
	kv.wg.Wait()

	// Wait for any running snapshot disk write to complete
	kv.snapMu.Lock()
	defer kv.snapMu.Unlock()

	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.closed = true

	if err := kv.wal.Close(); err != nil {
		return fmt.Errorf("kv: close wal: %w", err)
	}
	slog.Info("kv closed cleanly")
	return nil
}

// startSnapshooter launches the goroutine that snapshots every interval.
func (kv *KV) startSnapshooter() {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.closed || kv.interval <= 0 {
		return
	}
	kv.startSnapshooterLocked()
}

func (kv *KV) startSnapshooterLocked() {
	kv.stop = make(chan struct{})
	stop := kv.stop
	interval := kv.interval
	kv.wg.Add(1)

	go func() {
		defer kv.wg.Done()
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-t.C:
				if err := kv.Snapshot(); err != nil {
					slog.Error("kv: snapshot failed", "error", err)
				}
			case <-stop:
				return
			}
		}
	}()
}
