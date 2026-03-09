package cluster

import (
	"sync/atomic"

	"github.com/lynxbase/lynxdb/pkg/cluster/sharding"
)

// ShardMapCache provides lock-free read access to the current shard map.
// Writers call Update() which performs an atomic pointer swap; readers
// call Get() which is a single atomic load with zero allocation.
type ShardMapCache struct {
	current atomic.Pointer[sharding.ShardMap]
}

// NewShardMapCache creates a new cache initialized with an empty shard map.
func NewShardMapCache() *ShardMapCache {
	c := &ShardMapCache{}
	c.current.Store(sharding.NewShardMap())

	return c
}

// Get returns the current shard map snapshot.
// Safe for concurrent use. Zero allocation, zero lock.
func (c *ShardMapCache) Get() *sharding.ShardMap {
	return c.current.Load()
}

// Update atomically replaces the cached shard map.
func (c *ShardMapCache) Update(sm *sharding.ShardMap) {
	c.current.Store(sm)
}
