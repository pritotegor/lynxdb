// Package ingest implements the distributed ingestion layer for LynxDB clustering.
// It handles event routing to shard primaries, batcher replication to ISR peers,
// S3-backed part cataloging, batch sequence tracking, and graceful draining.
package ingest

import (
	"sync"
)

// BatchSequencer generates monotonically increasing batch sequence numbers
// per shard. Used to order batches for ISR replication dedup and to detect
// gaps during failover recovery.
//
// Each shard has its own independent counter. The primary increments the
// counter on every batch committed to the batcher. Replicas track the
// highest replicated sequence to determine ISR membership and failover
// resume points.
//
// Thread-safe.
type BatchSequencer struct {
	mu       sync.Mutex
	counters map[string]uint64 // shardID.String() -> last assigned seq
}

// NewBatchSequencer creates an empty BatchSequencer.
func NewBatchSequencer() *BatchSequencer {
	return &BatchSequencer{
		counters: make(map[string]uint64),
	}
}

// Next returns the next sequence number for the given shard.
// The first call for a shard returns 1.
func (s *BatchSequencer) Next(shardID string) uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.counters[shardID]++

	return s.counters[shardID]
}

// Current returns the current (last assigned) sequence number for the given shard.
// Returns 0 if no sequence has been assigned yet.
func (s *BatchSequencer) Current(shardID string) uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.counters[shardID]
}

// Set explicitly sets the sequence number for a shard.
// Used during startup to restore state from the part catalog's last BatchSeq.
func (s *BatchSequencer) Set(shardID string, seq uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.counters[shardID] = seq
}
