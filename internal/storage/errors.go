package storage

import "errors"

// ErrKeyNotFound is returned when a requested key does not exist in the store.
var ErrKeyNotFound = errors.New("key not found")

// ErrEmptyKey is returned when an empty key is provided to store operations.
var ErrEmptyKey = errors.New("key must not be empty")
