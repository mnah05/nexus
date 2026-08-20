package internal

import (
	"fmt"
	"log"
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
	mu       sync.Mutex // serializes mutations with snapshot/truncate
	store    *Store[string]
	wal      *WAL
	snapPath string
	interval time.Duration
	stop     chan struct{}
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
		return nil, err
	}
	kv.startSnapshooter()
	return kv, nil
}

// Recover restores state on startup: it prefers the last snapshot and
// then replays the WAL on top. Replay is safe because SET/DEL are
// idempotent, so entries logged after the snapshot are applied too.
func (kv *KV) Recover() error {
	m, err := LoadSnapshot(kv.snapPath)
	if err != nil {
		return fmt.Errorf("kv: load snapshot: %w", err)
	}

	for k, v := range m {
		kv.store.Set(k, v)
	}

	entries, err := kv.wal.ReadAll()
	if err != nil {
		return fmt.Errorf("kv: read wal: %w", err)
	}
	for _, e := range entries {
		switch e.Op {
		case OpSet:
			kv.store.Set(e.Key, e.Val)
		case OpDel:
			kv.store.Del(e.Key)
		default:
			return fmt.Errorf("kv: recover: unknown op %q at idx %d", e.Op, e.Idx)
		}
	}
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
	idx, err := kv.wal.Append(OpDel, 0, key, "")
	if err != nil {
		return 0, err
	}
	kv.store.Del(key)
	return idx, nil
}

// Snapshot persists the whole store to disk and clears the WAL while
// holding the KV mutex, so no mutation can slip in between the copy,
// the save, and the truncate.
func (kv *KV) Snapshot() error {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	entries := kv.store.Snapshot()
	if err := SaveSnapshot(kv.snapPath, entries); err != nil {
		return err
	}
	return kv.wal.Truncate()
}

// SetTiming re-configures the snapshot interval. Passing 0 disables
// automatic snapshots; a negative interval is rejected.
func (kv *KV) SetTiming(d time.Duration) error {
	if d < 0 {
		return fmt.Errorf("kv: interval must be >= 0")
	}
	kv.mu.Lock()
	kv.interval = d
	if kv.stop != nil {
		close(kv.stop)
		kv.stop = nil
	}
	kv.mu.Unlock()
	if d > 0 {
		kv.startSnapshooter()
	}
	return nil
}

// startSnapshooter launches the goroutine that snapshots every interval.
func (kv *KV) startSnapshooter() {
	kv.mu.Lock()
	interval := kv.interval
	kv.stop = make(chan struct{})
	stop := kv.stop
	kv.mu.Unlock()

	go func() {
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-t.C:
				if err := kv.Snapshot(); err != nil {
					log.Printf("kv: snapshot failed: %v", err)
				}
			case <-stop:
				return
			}
		}
	}()
}
