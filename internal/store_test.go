package internal

import (
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
