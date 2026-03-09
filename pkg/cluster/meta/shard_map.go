package meta

import (
	"fmt"

	"github.com/lynxbase/lynxdb/pkg/cluster/sharding"
)

// RecomputeShardMap rebuilds the shard map from the hash ring.
// Called after node addition/removal to rebalance partition assignments.
// This computes assignments for all VirtualPartitionCount partitions
// against the current hash ring.
func (s *MetaState) RecomputeShardMap(vPartCount uint32, rf int) {
	sm := sharding.NewShardMap()
	sm.Epoch = s.ShardMap.Epoch + 1

	for p := uint32(0); p < vPartCount; p++ {
		nodes := s.Ring.AssignPartitionReplicas(p, rf)
		if len(nodes) == 0 {
			continue
		}

		key := fmt.Sprintf("p%d", p)
		sm.Assignments[key] = &sharding.ShardAssignment{
			ShardID: sharding.ShardID{
				Partition: p,
			},
			Primary:  nodes[0],
			Replicas: nodes[1:],
			State:    sharding.ShardActive,
			Epoch:    sm.Epoch,
		}
	}

	s.ShardMap = sm
	s.Version++
}

// TransitionShard changes the state of a shard assignment.
// Returns an error if the shard is not found.
func (s *MetaState) TransitionShard(shardKey string, state sharding.ShardState) error {
	a, ok := s.ShardMap.Assignments[shardKey]
	if !ok {
		return fmt.Errorf("meta.TransitionShard: shard %q not found", shardKey)
	}

	a.State = state
	s.Version++

	return nil
}
