package meta

import (
	"testing"
	"time"
)

func TestApplyGrantCompaction_Success(t *testing.T) {
	state := NewMetaState()

	payload, _ := MarshalPayload(GrantCompactionPayload{
		ShardID:        "shard-1",
		AssignedNode:   "node-1",
		CatalogVersion: 10,
		InputParts:     []string{"part-a", "part-b"},
		TargetLevel:    1,
		TTL:            5 * time.Minute,
	})

	if err := state.applyGrantCompaction(payload); err != nil {
		t.Fatalf("applyGrantCompaction: %v", err)
	}

	task := state.Compaction["shard-1"]
	if task == nil {
		t.Fatal("expected compaction task")
	}
	if task.AssignedNode != "node-1" {
		t.Errorf("expected assigned node-1, got %s", task.AssignedNode)
	}
	if task.CatalogVersion != 10 {
		t.Errorf("expected catalog version 10, got %d", task.CatalogVersion)
	}
	if len(task.InputParts) != 2 {
		t.Errorf("expected 2 input parts, got %d", len(task.InputParts))
	}
	if task.TargetLevel != 1 {
		t.Errorf("expected target level 1, got %d", task.TargetLevel)
	}
}

func TestApplyGrantCompaction_DuplicateFails(t *testing.T) {
	state := NewMetaState()

	payload, _ := MarshalPayload(GrantCompactionPayload{
		ShardID:      "shard-1",
		AssignedNode: "node-1",
		TTL:          5 * time.Minute,
	})

	if err := state.applyGrantCompaction(payload); err != nil {
		t.Fatalf("first grant: %v", err)
	}

	// Second grant for same shard should fail.
	payload2, _ := MarshalPayload(GrantCompactionPayload{
		ShardID:      "shard-1",
		AssignedNode: "node-2",
		TTL:          5 * time.Minute,
	})

	if err := state.applyGrantCompaction(payload2); err == nil {
		t.Error("expected error for duplicate compaction grant")
	}
}

func TestApplyReleaseCompaction(t *testing.T) {
	state := NewMetaState()

	// Grant first.
	p, _ := MarshalPayload(GrantCompactionPayload{
		ShardID:      "shard-1",
		AssignedNode: "node-1",
		TTL:          5 * time.Minute,
	})
	_ = state.applyGrantCompaction(p)

	// Release.
	rp, _ := MarshalPayload(ReleaseCompactionPayload{
		ShardID:     "shard-1",
		Success:     true,
		OutputParts: []string{"part-c"},
	})

	if err := state.applyReleaseCompaction(rp); err != nil {
		t.Fatalf("applyReleaseCompaction: %v", err)
	}

	if len(state.Compaction) != 0 {
		t.Errorf("expected 0 compaction tasks, got %d", len(state.Compaction))
	}
}

func TestExpireCompactionTasks(t *testing.T) {
	state := NewMetaState()
	baseTime := time.Date(2026, 3, 9, 12, 0, 0, 0, time.UTC)

	// Add two tasks: one expired, one still valid.
	state.Compaction["shard-1"] = &CompactionTask{
		ShardID:      "shard-1",
		AssignedNode: "node-1",
		CreatedAt:    baseTime,
		ExpiresAt:    baseTime.Add(5 * time.Minute),
	}
	state.Compaction["shard-2"] = &CompactionTask{
		ShardID:      "shard-2",
		AssignedNode: "node-2",
		CreatedAt:    baseTime,
		ExpiresAt:    baseTime.Add(30 * time.Minute),
	}

	clock := &testClock{now: baseTime.Add(10 * time.Minute)}
	expired := state.ExpireCompactionTasks(clock)

	if len(expired) != 1 {
		t.Fatalf("expected 1 expired task, got %d", len(expired))
	}
	if expired[0].ShardID != "shard-1" {
		t.Errorf("expected shard-1 expired, got %s", expired[0].ShardID)
	}
	if len(state.Compaction) != 1 {
		t.Errorf("expected 1 remaining task, got %d", len(state.Compaction))
	}
	if state.Compaction["shard-2"] == nil {
		t.Error("expected shard-2 to remain")
	}
}

func TestExpireCompactionTasks_NoneExpired(t *testing.T) {
	state := NewMetaState()
	baseTime := time.Date(2026, 3, 9, 12, 0, 0, 0, time.UTC)
	versionBefore := state.Version

	state.Compaction["shard-1"] = &CompactionTask{
		ShardID:   "shard-1",
		ExpiresAt: baseTime.Add(30 * time.Minute),
	}

	clock := &testClock{now: baseTime.Add(5 * time.Minute)}
	expired := state.ExpireCompactionTasks(clock)

	if len(expired) != 0 {
		t.Errorf("expected 0 expired, got %d", len(expired))
	}
	// Version should not change when nothing expires.
	if state.Version != versionBefore {
		t.Errorf("version should not change, got %d", state.Version)
	}
}
