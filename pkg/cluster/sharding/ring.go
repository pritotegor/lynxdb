package sharding

import (
	"fmt"
	"sort"
	"sync"

	"github.com/cespare/xxhash/v2"
)

// RingEntry represents a single point on the consistent hash ring.
type RingEntry struct {
	Hash   uint64
	NodeID NodeID
	VNode  int
}

// HashRing implements consistent hashing for partition-to-node assignment.
// It uses virtual nodes (vnodes) to improve distribution across physical nodes.
//
// Thread-safe: all methods acquire appropriate locks.
type HashRing struct {
	vnodesPerNode int
	ring          []RingEntry
	nodes         map[NodeID]bool
	mu            sync.RWMutex
}

// NewHashRing creates a new consistent hash ring.
// vnodesPerNode controls how many virtual nodes each physical node contributes
// to the ring. Higher values give better distribution but use more memory.
// Recommended: 128-256 for clusters <100 nodes.
func NewHashRing(vnodesPerNode int) *HashRing {
	if vnodesPerNode < 1 {
		vnodesPerNode = 128
	}

	return &HashRing{
		vnodesPerNode: vnodesPerNode,
		nodes:         make(map[NodeID]bool),
	}
}

// AddNode adds a physical node to the ring, creating vnodesPerNode virtual entries.
// No-op if the node is already present.
func (r *HashRing) AddNode(nodeID NodeID) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.nodes[nodeID] {
		return
	}

	r.nodes[nodeID] = true

	for i := 0; i < r.vnodesPerNode; i++ {
		key := fmt.Sprintf("%s#%d", string(nodeID), i)
		h := xxhash.Sum64String(key)
		r.ring = append(r.ring, RingEntry{
			Hash:   h,
			NodeID: nodeID,
			VNode:  i,
		})
	}

	sort.Slice(r.ring, func(i, j int) bool {
		return r.ring[i].Hash < r.ring[j].Hash
	})
}

// RemoveNode removes a physical node and all its vnodes from the ring.
func (r *HashRing) RemoveNode(nodeID NodeID) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if !r.nodes[nodeID] {
		return
	}

	delete(r.nodes, nodeID)

	// Filter out all entries for this node.
	filtered := make([]RingEntry, 0, len(r.ring)-r.vnodesPerNode)
	for _, e := range r.ring {
		if e.NodeID != nodeID {
			filtered = append(filtered, e)
		}
	}

	r.ring = filtered
}

// AssignPartition returns the primary node responsible for the given partition.
// Uses O(log N) binary search on the sorted ring.
// Returns empty NodeID if the ring is empty.
func (r *HashRing) AssignPartition(partition uint32) NodeID {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.ring) == 0 {
		return ""
	}

	key := fmt.Sprintf("partition#%d", partition)
	h := xxhash.Sum64String(key)

	// Binary search for the first entry with hash >= h.
	idx := sort.Search(len(r.ring), func(i int) bool {
		return r.ring[i].Hash >= h
	})

	// Wrap around to the first entry if we're past the end.
	if idx >= len(r.ring) {
		idx = 0
	}

	return r.ring[idx].NodeID
}

// AssignPartitionReplicas returns the primary plus rf-1 distinct replica nodes
// for the given partition. Walks the ring clockwise from the partition's position,
// skipping duplicate nodes. Returns fewer nodes if the ring has fewer distinct
// physical nodes than rf.
func (r *HashRing) AssignPartitionReplicas(partition uint32, rf int) []NodeID {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.ring) == 0 || rf < 1 {
		return nil
	}

	key := fmt.Sprintf("partition#%d", partition)
	h := xxhash.Sum64String(key)

	idx := sort.Search(len(r.ring), func(i int) bool {
		return r.ring[i].Hash >= h
	})
	if idx >= len(r.ring) {
		idx = 0
	}

	seen := make(map[NodeID]bool, rf)
	result := make([]NodeID, 0, rf)

	for i := 0; i < len(r.ring) && len(result) < rf; i++ {
		entry := r.ring[(idx+i)%len(r.ring)]
		if !seen[entry.NodeID] {
			seen[entry.NodeID] = true
			result = append(result, entry.NodeID)
		}
	}

	return result
}

// NodeCount returns the number of distinct physical nodes in the ring.
func (r *HashRing) NodeCount() int {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return len(r.nodes)
}

// Nodes returns a copy of all node IDs in the ring.
func (r *HashRing) Nodes() []NodeID {
	r.mu.RLock()
	defer r.mu.RUnlock()

	nodes := make([]NodeID, 0, len(r.nodes))
	for id := range r.nodes {
		nodes = append(nodes, id)
	}

	return nodes
}
