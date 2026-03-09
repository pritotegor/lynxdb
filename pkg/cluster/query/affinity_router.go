package query

import (
	"log/slog"
	"sync"
	"sync/atomic"

	"github.com/lynxbase/lynxdb/pkg/cluster"
	"github.com/lynxbase/lynxdb/pkg/cluster/sharding"
)

// defaultLoadThreshold is the load count above which we consider a node
// overloaded and prefer routing to a replica instead.
const defaultLoadThreshold = 100

// AffinityRouter selects which node to query for a given shard, preferring
// the primary but falling back to replicas when the primary is overloaded.
// This is a soft optimization — queries always succeed even if affinity
// routing fails (the ShardPruner already resolves a valid node).
type AffinityRouter struct {
	shardMapCache *cluster.ShardMapCache
	mu            sync.RWMutex
	nodeLoad      map[sharding.NodeID]*atomic.Int64
	logger        *slog.Logger
}

// NewAffinityRouter creates a new affinity router.
func NewAffinityRouter(shardMapCache *cluster.ShardMapCache, logger *slog.Logger) *AffinityRouter {
	return &AffinityRouter{
		shardMapCache: shardMapCache,
		nodeLoad:      make(map[sharding.NodeID]*atomic.Int64),
		logger:        logger,
	}
}

// PickNode selects the best node for a shard query. Prefers the primary
// unless it's overloaded, in which case it picks the least-loaded replica.
func (ar *AffinityRouter) PickNode(primary sharding.NodeID, replicas []sharding.NodeID) sharding.NodeID {
	primaryLoad := ar.getLoad(primary)
	if primaryLoad < defaultLoadThreshold {
		return primary
	}

	// Primary overloaded — find least-loaded replica.
	bestNode := primary
	bestLoad := primaryLoad

	for _, replica := range replicas {
		load := ar.getLoad(replica)
		if load < bestLoad {
			bestNode = replica
			bestLoad = load
		}
	}

	return bestNode
}

// IncrementLoad increments the active query count for a node.
func (ar *AffinityRouter) IncrementLoad(nodeID sharding.NodeID) {
	ar.getOrCreateCounter(nodeID).Add(1)
}

// DecrementLoad decrements the active query count for a node.
func (ar *AffinityRouter) DecrementLoad(nodeID sharding.NodeID) {
	ar.getOrCreateCounter(nodeID).Add(-1)
}

func (ar *AffinityRouter) getLoad(nodeID sharding.NodeID) int64 {
	ar.mu.RLock()
	counter, ok := ar.nodeLoad[nodeID]
	ar.mu.RUnlock()
	if !ok {
		return 0
	}

	return counter.Load()
}

func (ar *AffinityRouter) getOrCreateCounter(nodeID sharding.NodeID) *atomic.Int64 {
	ar.mu.RLock()
	counter, ok := ar.nodeLoad[nodeID]
	ar.mu.RUnlock()
	if ok {
		return counter
	}

	ar.mu.Lock()
	defer ar.mu.Unlock()
	// Double-check.
	if counter, ok = ar.nodeLoad[nodeID]; ok {
		return counter
	}
	counter = &atomic.Int64{}
	ar.nodeLoad[nodeID] = counter

	return counter
}
