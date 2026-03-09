package meta

import (
	"testing"

	"github.com/lynxbase/lynxdb/pkg/cluster/sharding"
)

func TestRecomputeShardMap_BasicAssignment(t *testing.T) {
	state := NewMetaState()

	// Add 3 nodes to the ring.
	state.Ring.AddNode("node-1")
	state.Ring.AddNode("node-2")
	state.Ring.AddNode("node-3")

	state.RecomputeShardMap(16, 2) // 16 partitions, RF=2

	if len(state.ShardMap.Assignments) != 16 {
		t.Fatalf("expected 16 assignments, got %d", len(state.ShardMap.Assignments))
	}

	// Every assignment should have a primary and at least 1 replica.
	for key, a := range state.ShardMap.Assignments {
		if a.Primary == "" {
			t.Errorf("partition %s has no primary", key)
		}
		if len(a.Replicas) < 1 {
			t.Errorf("partition %s has %d replicas, want >=1", key, len(a.Replicas))
		}
		if a.State != sharding.ShardActive {
			t.Errorf("partition %s state = %d, want ShardActive", key, a.State)
		}
	}

	// Epoch should increment.
	if state.ShardMap.Epoch != 1 {
		t.Errorf("expected epoch 1, got %d", state.ShardMap.Epoch)
	}
}

func TestRecomputeShardMap_EmptyRing(t *testing.T) {
	state := NewMetaState()

	state.RecomputeShardMap(8, 1)

	if len(state.ShardMap.Assignments) != 0 {
		t.Errorf("expected 0 assignments with empty ring, got %d", len(state.ShardMap.Assignments))
	}
}

func TestRecomputeShardMap_SingleNode(t *testing.T) {
	state := NewMetaState()
	state.Ring.AddNode("node-1")

	state.RecomputeShardMap(4, 3) // RF=3 but only 1 node

	for key, a := range state.ShardMap.Assignments {
		if a.Primary != "node-1" {
			t.Errorf("partition %s primary = %s, want node-1", key, a.Primary)
		}
		// Replicas will be empty since there's only 1 node.
	}
}

func TestRecomputeShardMap_EpochIncrement(t *testing.T) {
	state := NewMetaState()
	state.Ring.AddNode("node-1")

	state.ShardMap.Epoch = 5
	state.RecomputeShardMap(2, 1)

	if state.ShardMap.Epoch != 6 {
		t.Errorf("expected epoch 6, got %d", state.ShardMap.Epoch)
	}
}

func TestTransitionShard(t *testing.T) {
	state := NewMetaState()
	state.Ring.AddNode("node-1")
	state.RecomputeShardMap(4, 1)

	// Transition a shard.
	err := state.TransitionShard("p0", sharding.ShardDraining)
	if err != nil {
		t.Fatalf("TransitionShard: %v", err)
	}

	a := state.ShardMap.Assignments["p0"]
	if a.State != sharding.ShardDraining {
		t.Errorf("expected ShardDraining, got %d", a.State)
	}

	// Transition nonexistent shard.
	err = state.TransitionShard("p999", sharding.ShardDraining)
	if err == nil {
		t.Error("expected error for nonexistent shard")
	}
}
