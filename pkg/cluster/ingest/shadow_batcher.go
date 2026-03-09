package ingest

import (
	"sync"

	"github.com/lynxbase/lynxdb/pkg/event"
)

// shadowEntry holds a replicated batch waiting to be flushed on failover.
type shadowEntry struct {
	Events   []*event.Event
	BatchSeq uint64
}

// ShadowBatcher buffers replicated events on replica nodes. Unlike the
// primary's async batcher, the shadow batcher does NOT flush to disk
// during normal operation. It only flushes during failover, when the
// replica is promoted to primary.
//
// On failover, the new primary calls FlushShadow to get all events
// with batch_seq > the last committed BatchSeq from the part catalog.
// This ensures exactly-once semantics: events already committed to parts
// are not re-flushed, while events in-flight during the primary failure
// are recovered from the shadow buffer.
//
// Thread-safe.
type ShadowBatcher struct {
	mu      sync.Mutex
	buffers map[string][]shadowEntry // shardID string -> entries
}

// NewShadowBatcher creates an empty ShadowBatcher.
func NewShadowBatcher() *ShadowBatcher {
	return &ShadowBatcher{
		buffers: make(map[string][]shadowEntry),
	}
}

// Add appends a replicated batch to the shadow buffer for the given shard.
func (s *ShadowBatcher) Add(shardID string, events []*event.Event, batchSeq uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Make a copy of the events slice to avoid aliasing.
	cp := make([]*event.Event, len(events))
	copy(cp, events)

	s.buffers[shardID] = append(s.buffers[shardID], shadowEntry{
		Events:   cp,
		BatchSeq: batchSeq,
	})
}

// FlushShadow returns all buffered events for the given shard with
// batch_seq strictly greater than afterSeq. The returned events are
// in batch_seq order. The shadow buffer for this shard is cleared.
//
// Used during failover: afterSeq is the last committed BatchSeq from
// the part catalog, so only un-committed events are recovered.
func (s *ShadowBatcher) FlushShadow(shardID string, afterSeq uint64) []*event.Event {
	s.mu.Lock()
	defer s.mu.Unlock()

	entries, ok := s.buffers[shardID]
	if !ok {
		return nil
	}

	var result []*event.Event
	for _, e := range entries {
		if e.BatchSeq > afterSeq {
			result = append(result, e.Events...)
		}
	}

	// Clear the buffer for this shard.
	delete(s.buffers, shardID)

	return result
}

// BufferedCount returns the number of buffered entries for a shard.
// Used for monitoring/debugging.
func (s *ShadowBatcher) BufferedCount(shardID string) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	return len(s.buffers[shardID])
}

// Clear removes all shadow entries for a shard. Called when the shard
// is no longer a replica on this node (e.g., after shard rebalancing).
func (s *ShadowBatcher) Clear(shardID string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.buffers, shardID)
}
