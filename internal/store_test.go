package internal

import (
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestStoreGetSet(t *testing.T) {
	s := NewMap[string]()
	s.Set("foo", "bar")

	v, ok := s.Get("foo")
	if !ok || v != "bar" {
		t.Fatalf("expected (bar, true), got (%v, %v)", v, ok)
	}

	if v, ok := s.Get("missing"); ok {
		t.Fatalf("expected missing key, got %v", v)
	}
}

func TestStoreSetOverwrite(t *testing.T) {
	s := NewMap[int]()
	s.Set("k", 1)
	s.Set("k", 2)

	v, _ := s.Get("k")
	if v != 2 {
		t.Fatalf("expected 2, got %v", v)
	}
}

func TestStoreDel(t *testing.T) {
	s := NewMap[int]()
	s.Set("k", 42)

	s.Del("k")
	if _, ok := s.Get("k"); ok {
		t.Fatal("key should be deleted")
	}

	s.Del("never-set")
}

func TestStoreConcurrent(t *testing.T) {
	s := NewMap[int]()
	var wg sync.WaitGroup

	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for j := 0; j < 10000; j++ {
				s.Set("k", g)
				s.Get("k")
			}
		}(i)
	}

	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 10000; j++ {
				s.Del("k")
			}
		}()
	}

	wg.Wait()
}

func TestStoreGenericTypes(t *testing.T) {
	strStore := NewMap[string]()
	strStore.Set("name", "nexus")
	if v, _ := strStore.Get("name"); v != "nexus" {
		t.Fatalf("string store mismatch: %v", v)
	}

	structStore := NewMap[struct{ count int }]()
	structStore.Set("s", struct{ count int }{count: 7})
	if v, _ := structStore.Get("s"); v.count != 7 {
		t.Fatalf("struct store mismatch: %v", v.count)
	}
}

func TestSnapshotGenericTypes(t *testing.T) {
	tmpDir := t.TempDir()

	// Test Store[int] snapshot
	intStore := NewMap[int]()
	intStore.Set("apples", 42)
	intStore.Set("oranges", 99)

	snapPath := filepath.Join(tmpDir, "int_store.snap")
	err := SaveSnapshot(snapPath, Snapshot[int]{
		LastIdx: 123,
		Entries: intStore.Snapshot(),
	})
	if err != nil {
		t.Fatalf("SaveSnapshot failed: %v", err)
	}

	loadedSnap, err := LoadSnapshot[int](snapPath)
	if err != nil {
		t.Fatalf("LoadSnapshot failed: %v", err)
	}

	if loadedSnap.LastIdx != 123 {
		t.Fatalf("expected LastIdx 123, got %d", loadedSnap.LastIdx)
	}
	if loadedSnap.Entries["apples"] != 42 || loadedSnap.Entries["oranges"] != 99 {
		t.Fatalf("unexpected loaded entries: %v", loadedSnap.Entries)
	}

	// Test Store[struct] snapshot
	type Item struct {
		Name  string `json:"name"`
		Price int    `json:"price"`
	}

	itemStore := NewMap[Item]()
	itemStore.Set("book", Item{Name: "Go Programming", Price: 30})

	itemSnapPath := filepath.Join(tmpDir, "item_store.snap")
	err = SaveSnapshot(itemSnapPath, Snapshot[Item]{
		LastIdx: 500,
		Entries: itemStore.Snapshot(),
	})
	if err != nil {
		t.Fatalf("SaveSnapshot for items failed: %v", err)
	}

	loadedItemSnap, err := LoadSnapshot[Item](itemSnapPath)
	if err != nil {
		t.Fatalf("LoadSnapshot for items failed: %v", err)
	}
	if loadedItemSnap.LastIdx != 500 {
		t.Fatalf("expected LastIdx 500, got %d", loadedItemSnap.LastIdx)
	}
	if loadedItemSnap.Entries["book"].Name != "Go Programming" || loadedItemSnap.Entries["book"].Price != 30 {
		t.Fatalf("item mismatch: %+v", loadedItemSnap.Entries["book"])
	}

	// Test non-existent snapshot path returns empty snapshot without error
	emptySnap, err := LoadSnapshot[int](filepath.Join(tmpDir, "does_not_exist.snap"))
	if err != nil {
		t.Fatalf("expected nil error on nonexistent file, got %v", err)
	}
	if emptySnap.Entries == nil || len(emptySnap.Entries) != 0 {
		t.Fatalf("expected empty entries map, got %v", emptySnap.Entries)
	}
}

func TestSnapshotAtomic(t *testing.T) {
	tmpDir := t.TempDir()
	snapPath := filepath.Join(tmpDir, "test.snap")

	snap := Snapshot[string]{
		LastIdx: 10,
		Entries: map[string]string{"foo": "bar"},
	}

	if err := SaveSnapshot(snapPath, snap); err != nil {
		t.Fatalf("SaveSnapshot failed: %v", err)
	}

	// Verify temp file was cleaned up by rename
	if _, err := os.Stat(snapPath + ".tmp"); !os.IsNotExist(err) {
		t.Fatalf("expected tmp file to be gone, got err: %v", err)
	}

	// Verify target file exists
	if _, err := os.Stat(snapPath); err != nil {
		t.Fatalf("expected snapshot file to exist: %v", err)
	}
}
