package storage

import (
	"errors"
	"sync"
)

// ErrKeyNotFound is returned when a key does not exist in the store.
var ErrKeyNotFound = errors.New("key not found")

// ErrEmptyKey is returned when an empty key is provided.
var ErrEmptyKey = errors.New("key must not be empty")

// Store is a thread-safe in-memory key-value store.
type Store struct {
	mu   sync.RWMutex
	data map[string][]byte
}

// NewStore creates and returns a new Store instance.
func NewStore() *Store {
	return &Store{
		data: make(map[string][]byte),
	}
}

// Set stores the value for the given key.
func (s *Store) Set(key string, value []byte) error {
	if key == "" {
		return ErrEmptyKey
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = value
	return nil
}

// Get retrieves the value associated with the given key.
func (s *Store) Get(key string) ([]byte, error) {
	if key == "" {
		return nil, ErrEmptyKey
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.data[key]
	if !ok {
		return nil, ErrKeyNotFound
	}
	return v, nil
}

// Delete removes the key and its value from the store.
func (s *Store) Delete(key string) error {
	if key == "" {
		return ErrEmptyKey
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.data, key)
	return nil
}

// Keys returns all keys currently in the store.
func (s *Store) Keys() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	keys := make([]string, 0, len(s.data))
	for k := range s.data {
		keys = append(keys, k)
	}
	return keys
}
