package storage

import "sync"

// Store is a simple thread-safe in-memory key-value store.
type Store struct {
	mu   sync.RWMutex
	data map[string]string
}

// NewStore initialises and returns an empty Store.
func NewStore() *Store {
	return &Store{data: make(map[string]string)}
}

// Set stores the value under key. Returns ErrEmptyKey for blank keys.
func (s *Store) Set(key, value string) error {
	if key == "" {
		return ErrEmptyKey
	}
	s.mu.Lock()
	s.data[key] = value
	s.mu.Unlock()
	return nil
}

// Get retrieves the value for key. Returns ErrKeyNotFound when absent.
func (s *Store) Get(key string) (string, error) {
	s.mu.RLock()
	v, ok := s.data[key]
	s.mu.RUnlock()
	if !ok {
		return "", ErrKeyNotFound
	}
	return v, nil
}

// Delete removes key from the store. Returns ErrKeyNotFound when absent.
func (s *Store) Delete(key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.data[key]; !ok {
		return ErrKeyNotFound
	}
	delete(s.data, key)
	return nil
}

// Keys returns a snapshot of all keys currently in the store.
func (s *Store) Keys() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	keys := make([]string, 0, len(s.data))
	for k := range s.data {
		keys = append(keys, k)
	}
	return keys
}
