package internal

import "maps"

import "sync"

type Store[V any] struct {
	mu   sync.RWMutex
	m    map[string]V
	term int //for future multi node store use
}

func NewMap[V any]() *Store[V] {
	return &Store[V]{
		m: make(map[string]V),
	}
}

func (s *Store[V]) Get(key string) (V, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.m[key]
	return v, ok
}

func (s *Store[V]) Set(key string, val V) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.m[key] = val
}

func (s *Store[V]) Del(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.m, key)
}

// Snapshot returns a copy of the whole map, safe to read after release.
func (s *Store[V]) Snapshot() map[string]V {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[string]V, len(s.m))
	maps.Copy(out, s.m)
	return out
}
