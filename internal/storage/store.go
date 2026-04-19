package storage

import "time"

// Store defines the unified interface for key-value storage with optional TTL support.
type Store interface {
	// Set stores a value under the given key.
	Set(key, value string) error

	// Get retrieves the value associated with the given key.
	Get(key string) (string, error)

	// Delete removes the key and its associated value from the store.
	Delete(key string) error

	// Has returns true if the key exists in the store.
	Has(key string) bool
}

// TTLStore extends Store with time-to-live capabilities.
type TTLStore interface {
	Store

	// SetWithTTL stores a value under the given key with an expiration duration.
	SetWithTTL(key, value string, ttl time.Duration) error

	// TTLOf returns the remaining time-to-live for the given key.
	// Returns ErrNotFound if the key does not exist or has no TTL.
	TTLOf(key string) (time.Duration, error)

	// Persist removes the TTL from a key, making it permanent.
	Persist(key string) error
}
