package internal

import (
	"encoding/json"
	"os"
)

// snapshot is the on-disk wire format: a full copy of the key-value map.
type snapshot struct {
	Entries map[string]string `json:"entries"`
}

// SaveSnapshot atomically persists a copy of the map via temp file + rename,
// so a crash mid-write never leaves a partial snapshot behind.
func SaveSnapshot(path string, m map[string]string) error {
	b, err := json.Marshal(snapshot{Entries: m})
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, b, 0644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// LoadSnapshot restores a map from path. It returns (nil, nil) when no
// snapshot exists yet, so recovery can fall back to the WAL.
func LoadSnapshot(path string) (map[string]string, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var s snapshot
	if err := json.Unmarshal(b, &s); err != nil {
		return nil, err
	}
	return s.Entries, nil
}
