package internal

import (
	"encoding/json"
	"os"
)

// Snapshot is the on-disk wire format: a full copy of the key-value map
// along with the last applied WAL index.
type Snapshot[V any] struct {
	LastIdx uint64       `json:"last_idx,omitempty"`
	Entries map[string]V `json:"entries"`
}

// SaveSnapshot atomically persists a snapshot via temp file + rename,
// so a crash mid-write never leaves a partial snapshot behind.
func SaveSnapshot[V any](path string, snap Snapshot[V]) error {
	b, err := json.Marshal(snap)
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, b, 0644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// LoadSnapshot restores a Snapshot from path. If no snapshot file exists,
// it returns an empty Snapshot with nil error, so recovery can fall back to the WAL.
func LoadSnapshot[V any](path string) (Snapshot[V], error) {
	b, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return Snapshot[V]{Entries: make(map[string]V)}, nil
		}
		return Snapshot[V]{}, err
	}
	var s Snapshot[V]
	if err := json.Unmarshal(b, &s); err != nil {
		return Snapshot[V]{}, err
	}
	if s.Entries == nil {
		s.Entries = make(map[string]V)
	}
	return s, nil
}
