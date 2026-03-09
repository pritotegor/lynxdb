package sharding

import (
	"fmt"
	"math"
	"testing"
)

func TestHashRing_Empty(t *testing.T) {
	ring := NewHashRing(128)

	if got := ring.AssignPartition(0); got != "" {
		t.Errorf("empty ring returned node %q, want empty", got)
	}
	if got := ring.AssignPartitionReplicas(0, 3); len(got) != 0 {
		t.Errorf("empty ring returned replicas %v, want empty", got)
	}
}

func TestHashRing_SingleNode(t *testing.T) {
	ring := NewHashRing(128)
	ring.AddNode("node-1")

	// All partitions should go to node-1.
	for p := uint32(0); p < 100; p++ {
		if got := ring.AssignPartition(p); got != "node-1" {
			t.Errorf("partition %d assigned to %q, want node-1", p, got)
		}
	}
}

func TestHashRing_AddRemove(t *testing.T) {
	ring := NewHashRing(128)
	ring.AddNode("node-1")
	ring.AddNode("node-2")
	ring.AddNode("node-3")

	if ring.NodeCount() != 3 {
		t.Errorf("NodeCount() = %d, want 3", ring.NodeCount())
	}

	ring.RemoveNode("node-2")

	if ring.NodeCount() != 2 {
		t.Errorf("NodeCount() after remove = %d, want 2", ring.NodeCount())
	}

	// All partitions should only go to node-1 or node-3.
	for p := uint32(0); p < 100; p++ {
		got := ring.AssignPartition(p)
		if got != "node-1" && got != "node-3" {
			t.Errorf("partition %d assigned to %q, expected node-1 or node-3", p, got)
		}
	}
}

func TestHashRing_AddNode_Idempotent(t *testing.T) {
	ring := NewHashRing(128)
	ring.AddNode("node-1")
	ring.AddNode("node-1") // duplicate

	if ring.NodeCount() != 1 {
		t.Errorf("NodeCount() = %d, want 1 after duplicate add", ring.NodeCount())
	}
}

func TestHashRing_Distribution(t *testing.T) {
	ring := NewHashRing(128)
	nodes := []NodeID{"node-1", "node-2", "node-3", "node-4"}
	for _, n := range nodes {
		ring.AddNode(n)
	}

	counts := make(map[NodeID]int)
	totalPartitions := 1024

	for p := uint32(0); p < uint32(totalPartitions); p++ {
		n := ring.AssignPartition(p)
		counts[n]++
	}

	// Expect roughly 256 per node (1024/4). Allow +-50% deviation.
	expected := float64(totalPartitions) / float64(len(nodes))

	for _, n := range nodes {
		count := float64(counts[n])
		deviation := math.Abs(count-expected) / expected

		if deviation > 0.5 {
			t.Errorf("node %s has %d partitions (expected ~%.0f, deviation %.0f%%)",
				n, counts[n], expected, deviation*100)
		}
	}
}

func TestHashRing_Replicas(t *testing.T) {
	ring := NewHashRing(128)
	ring.AddNode("node-1")
	ring.AddNode("node-2")
	ring.AddNode("node-3")

	replicas := ring.AssignPartitionReplicas(42, 3)
	if len(replicas) != 3 {
		t.Fatalf("expected 3 replicas, got %d: %v", len(replicas), replicas)
	}

	// All replicas should be distinct.
	seen := make(map[NodeID]bool)
	for _, r := range replicas {
		if seen[r] {
			t.Errorf("duplicate node in replicas: %q", r)
		}
		seen[r] = true
	}
}

func TestHashRing_ReplicasExceedsNodes(t *testing.T) {
	ring := NewHashRing(128)
	ring.AddNode("node-1")
	ring.AddNode("node-2")

	// Request 5 replicas but only 2 nodes exist.
	replicas := ring.AssignPartitionReplicas(42, 5)
	if len(replicas) != 2 {
		t.Errorf("expected 2 replicas (capped by node count), got %d", len(replicas))
	}
}

func TestHashRing_Stability(t *testing.T) {
	// Adding a new node should only reassign ~1/N of partitions.
	ring := NewHashRing(128)
	ring.AddNode("node-1")
	ring.AddNode("node-2")
	ring.AddNode("node-3")

	before := make(map[uint32]NodeID)
	totalPartitions := 1024

	for p := uint32(0); p < uint32(totalPartitions); p++ {
		before[p] = ring.AssignPartition(p)
	}

	ring.AddNode("node-4")

	changed := 0
	for p := uint32(0); p < uint32(totalPartitions); p++ {
		after := ring.AssignPartition(p)
		if before[p] != after {
			changed++
		}
	}

	// With 4 nodes, adding one should reassign ~25% of partitions.
	// Allow up to 50% to account for hash distribution variance.
	maxChanged := totalPartitions / 2
	if changed > maxChanged {
		t.Errorf("adding a node changed %d/%d partitions (too many, expected ~%d)",
			changed, totalPartitions, totalPartitions/4)
	}

	t.Logf("adding node-4 changed %d/%d partitions (%.1f%%)",
		changed, totalPartitions, float64(changed)*100/float64(totalPartitions))
}

func BenchmarkHashRing_AssignPartition(b *testing.B) {
	ring := NewHashRing(128)
	for i := 0; i < 10; i++ {
		ring.AddNode(NodeID(fmt.Sprintf("node-%d", i)))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ring.AssignPartition(uint32(i))
	}
}
