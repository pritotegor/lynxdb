package storage

import (
	"sync"
	"time"
)

// TTLStore wraps Store with expiration support.
type TTLStore struct {
	store   *Store
	expiry  map[string]time.Time
	mu      sync.RWMutex
	stopCh  chan struct{}
}

// NewTTLStore creates a TTLStore and starts the background cleaner.
func NewTTLStore(s *Store) *TTLStore {
	ts := &TTLStore{
		store:  s,
		expiry: make(map[string]time.Time),
		stopCh: make(chan struct{}),
	}
	go ts.cleanupLoop()
	return ts
}

// SetWithTTL stores a key-value pair with an expiration duration.
func (ts *TTLStore) SetWithTTL(key, value string, ttl time.Duration) error {
	if err := ts.store.Set(key, value); err != nil {
		return err
	}
	ts.mu.Lock()
	ts.expiry[key] = time.Now().Add(ttl)
	ts.mu.Unlock()
	return nil
}

// Get retrieves a value, returning ErrKeyNotFound if expired.
func (ts *TTLStore) Get(key string) (string, error) {
	ts.mu.RLock()
	exp, hasExp := ts.expiry[key]
	ts.mu.RUnlock()

	if hasExp && time.Now().After(exp) {
		_ = ts.store.Delete(key)
		ts.mu.Lock()
		delete(ts.expiry, key)
		ts.mu.Unlock()
		return "", ErrKeyNotFound
	}
	return ts.store.Get(key)
}

// Delete removes a key and its TTL metadata.
func (ts *TTLStore) Delete(key string) error {
	ts.mu.Lock()
	delete(ts.expiry, key)
	ts.mu.Unlock()
	return ts.store.Delete(key)
}

// TTLOf returns the remaining TTL for a key, or 0 if none/expired.
func (ts *TTLStore) TTLOf(key string) time.Duration {
	ts.mu.RLock()
	exp, ok := ts.expiry[key]
	ts.mu.RUnlock()
	if !ok {
		return 0
	}
	remaining := time.Until(exp)
	if remaining < 0 {
		return 0
	}
	return remaining
}

// Stop halts the background cleanup goroutine.
func (ts *TTLStore) Stop() {
	close(ts.stopCh)
}

func (ts *TTLStore) cleanupLoop() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			ts.evictExpired()
		case <-ts.stopCh:
			return
		}
	}
}

func (ts *TTLStore) evictExpired() {
	now := time.Now()
	ts.mu.Lock()
	var expired []string
	for k, exp := range ts.expiry {
		if now.After(exp) {
			expired = append(expired, k)
		}
	}
	for _, k := range expired {
		delete(ts.expiry, k)
	}
	ts.mu.Unlock()
	for _, k := range expired {
		_ = ts.store.Delete(k)
	}
}
