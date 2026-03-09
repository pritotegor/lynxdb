package ingest

import (
	"sync"

	"github.com/lynxbase/lynxdb/pkg/cluster/sharding"
)

// ISRTracker tracks in-sync replica (ISR) membership and replication progress
// per shard. The primary updates replica progress as ACKs arrive from the
// ReplicateBatch RPC. ISR membership is computed from the set of replicas
// whose last replicated batch_seq is within a configurable lag of the primary's
// current sequence.
//
// Thread-safe.
type ISRTracker struct {
	mu     sync.RWMutex
	shards map[string]*isrState // key: shardID string
}

// isrState holds the replication state for a single shard.
type isrState struct {
	replicas   map[sharding.NodeID]uint64 // nodeID -> last replicated batch_seq
	isrMembers []sharding.NodeID          // current ISR member list
}

// NewISRTracker creates an empty ISR tracker.
func NewISRTracker() *ISRTracker {
	return &ISRTracker{
		shards: make(map[string]*isrState),
	}
}

// getOrCreate returns the isrState for a shard, creating it if needed.
// Must be called under write lock.
func (t *ISRTracker) getOrCreate(shardID string) *isrState {
	s, ok := t.shards[shardID]
	if !ok {
		s = &isrState{
			replicas: make(map[sharding.NodeID]uint64),
		}
		t.shards[shardID] = s
	}

	return s
}

// UpdateReplicatedSeq records that a replica has confirmed replication up to
// the given batch sequence. Called when the primary receives a ReplicateBatch ACK.
func (t *ISRTracker) UpdateReplicatedSeq(shardID string, nodeID sharding.NodeID, batchSeq uint64) {
	t.mu.Lock()
	defer t.mu.Unlock()

	s := t.getOrCreate(shardID)
	// Only advance — never go backwards (handles out-of-order ACKs).
	if batchSeq > s.replicas[nodeID] {
		s.replicas[nodeID] = batchSeq
	}
}

// ReplicaSeq returns the last replicated batch sequence for a specific replica.
// Returns 0 if the replica is not tracked.
func (t *ISRTracker) ReplicaSeq(shardID string, nodeID sharding.NodeID) uint64 {
	t.mu.RLock()
	defer t.mu.RUnlock()

	s, ok := t.shards[shardID]
	if !ok {
		return 0
	}

	return s.replicas[nodeID]
}

// ISRMembers returns the current ISR member list for a shard.
// Returns nil if the shard is not tracked.
func (t *ISRTracker) ISRMembers(shardID string) []sharding.NodeID {
	t.mu.RLock()
	defer t.mu.RUnlock()

	s, ok := t.shards[shardID]
	if !ok {
		return nil
	}

	// Return a copy to avoid data races.
	result := make([]sharding.NodeID, len(s.isrMembers))
	copy(result, s.isrMembers)

	return result
}

// SetISR explicitly sets the ISR membership for a shard.
// Called when the meta leader broadcasts updated ISR membership via shard map.
func (t *ISRTracker) SetISR(shardID string, members []sharding.NodeID) {
	t.mu.Lock()
	defer t.mu.Unlock()

	s := t.getOrCreate(shardID)
	s.isrMembers = make([]sharding.NodeID, len(members))
	copy(s.isrMembers, members)
}

// HighestReplicatedNode returns the replica with the highest replicated
// batch_seq for the given shard, along with its sequence number.
// Returns ("", 0) if no replicas are tracked or the shard is unknown.
// Used during failover to select the best candidate for new primary.
func (t *ISRTracker) HighestReplicatedNode(shardID string) (sharding.NodeID, uint64) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	s, ok := t.shards[shardID]
	if !ok {
		return "", 0
	}

	var bestNode sharding.NodeID
	var bestSeq uint64

	for nodeID, seq := range s.replicas {
		if seq > bestSeq {
			bestSeq = seq
			bestNode = nodeID
		}
	}

	return bestNode, bestSeq
}

// AllReplicaSeqs returns all tracked replica sequences for a shard.
// Returns nil if the shard is unknown.
func (t *ISRTracker) AllReplicaSeqs(shardID string) map[sharding.NodeID]uint64 {
	t.mu.RLock()
	defer t.mu.RUnlock()

	s, ok := t.shards[shardID]
	if !ok {
		return nil
	}

	result := make(map[sharding.NodeID]uint64, len(s.replicas))
	for k, v := range s.replicas {
		result[k] = v
	}

	return result
}
