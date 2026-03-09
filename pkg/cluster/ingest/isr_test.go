package ingest

import (
	"testing"

	"github.com/lynxbase/lynxdb/pkg/cluster/sharding"
)

func TestISRTracker_UpdateAndQuery(t *testing.T) {
	tracker := NewISRTracker()

	tracker.UpdateReplicatedSeq("shard-a", "node-1", 5)
	tracker.UpdateReplicatedSeq("shard-a", "node-2", 3)

	if got := tracker.ReplicaSeq("shard-a", "node-1"); got != 5 {
		t.Errorf("expected 5, got %d", got)
	}
	if got := tracker.ReplicaSeq("shard-a", "node-2"); got != 3 {
		t.Errorf("expected 3, got %d", got)
	}
}

func TestISRTracker_NeverGoesBackward(t *testing.T) {
	tracker := NewISRTracker()

	tracker.UpdateReplicatedSeq("shard-a", "node-1", 10)
	tracker.UpdateReplicatedSeq("shard-a", "node-1", 5) // out-of-order

	if got := tracker.ReplicaSeq("shard-a", "node-1"); got != 10 {
		t.Errorf("expected 10 (no backward), got %d", got)
	}
}

func TestISRTracker_UnknownShard(t *testing.T) {
	tracker := NewISRTracker()

	if got := tracker.ReplicaSeq("unknown", "node-1"); got != 0 {
		t.Errorf("expected 0 for unknown shard, got %d", got)
	}
	if got := tracker.ISRMembers("unknown"); got != nil {
		t.Errorf("expected nil for unknown shard, got %v", got)
	}
	node, seq := tracker.HighestReplicatedNode("unknown")
	if node != "" || seq != 0 {
		t.Errorf("expected empty for unknown shard, got %s/%d", node, seq)
	}
}

func TestISRTracker_SetISR(t *testing.T) {
	tracker := NewISRTracker()

	members := []sharding.NodeID{"node-1", "node-2", "node-3"}
	tracker.SetISR("shard-a", members)

	got := tracker.ISRMembers("shard-a")
	if len(got) != 3 {
		t.Fatalf("expected 3 members, got %d", len(got))
	}

	// Verify it's a copy (mutation of original doesn't affect tracker).
	members[0] = "node-modified"
	got = tracker.ISRMembers("shard-a")
	if got[0] != "node-1" {
		t.Error("ISRMembers should return a copy, not reference to original")
	}
}

func TestISRTracker_HighestReplicatedNode(t *testing.T) {
	tracker := NewISRTracker()

	tracker.UpdateReplicatedSeq("shard-a", "node-1", 5)
	tracker.UpdateReplicatedSeq("shard-a", "node-2", 10)
	tracker.UpdateReplicatedSeq("shard-a", "node-3", 7)

	node, seq := tracker.HighestReplicatedNode("shard-a")
	if node != "node-2" {
		t.Errorf("expected node-2, got %s", node)
	}
	if seq != 10 {
		t.Errorf("expected 10, got %d", seq)
	}
}

func TestISRTracker_HighestReplicatedNodeEmpty(t *testing.T) {
	tracker := NewISRTracker()

	// Shard exists but no replicas.
	tracker.SetISR("shard-a", []sharding.NodeID{"node-1"})

	node, seq := tracker.HighestReplicatedNode("shard-a")
	if node != "" || seq != 0 {
		t.Errorf("expected empty for shard with no replica seqs, got %s/%d", node, seq)
	}
}

func TestISRTracker_AllReplicaSeqs(t *testing.T) {
	tracker := NewISRTracker()

	tracker.UpdateReplicatedSeq("shard-a", "node-1", 5)
	tracker.UpdateReplicatedSeq("shard-a", "node-2", 10)

	seqs := tracker.AllReplicaSeqs("shard-a")
	if len(seqs) != 2 {
		t.Fatalf("expected 2, got %d", len(seqs))
	}
	if seqs["node-1"] != 5 || seqs["node-2"] != 10 {
		t.Errorf("unexpected seqs: %v", seqs)
	}

	// Returned map should be a copy.
	seqs["node-1"] = 999
	if tracker.ReplicaSeq("shard-a", "node-1") != 5 {
		t.Error("AllReplicaSeqs should return a copy")
	}
}

func TestISRTracker_IndependentShards(t *testing.T) {
	tracker := NewISRTracker()

	tracker.UpdateReplicatedSeq("shard-a", "node-1", 5)
	tracker.UpdateReplicatedSeq("shard-b", "node-1", 10)

	if got := tracker.ReplicaSeq("shard-a", "node-1"); got != 5 {
		t.Errorf("shard-a: expected 5, got %d", got)
	}
	if got := tracker.ReplicaSeq("shard-b", "node-1"); got != 10 {
		t.Errorf("shard-b: expected 10, got %d", got)
	}
}
