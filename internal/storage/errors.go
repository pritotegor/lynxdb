package storage

import "errors"

// ErrKeyNotFound is returned when a requested key does not exist in the store.
var ErrKeyNotFound = errors.New("key not found")

// ErrEmptyKey is returned when an empty key is provided to store operations.
var ErrEmptyKey = errors.New("key must not be empty")

// ErrKeyTooLarge is returned when a key exceeds the maximum allowed key size.
var ErrKeyTooLarge = errors.New("key exceeds maximum allowed size")

// ErrValueTooLarge is returned when a value exceeds the maximum allowed value size.
var ErrValueTooLarge = errors.New("value exceeds maximum allowed size")
