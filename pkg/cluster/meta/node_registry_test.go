package meta

import (
	"testing"
	"time"

	"github.com/lynxbase/lynxdb/pkg/cluster/sharding"
)

func TestProcessHeartbeat_UpdatesTimestamp(t *testing.T) {
	state := NewMetaState()
	now := time.Date(2026, 3, 9, 12, 0, 0, 0, time.UTC)

	state.Nodes["node-1"] = &NodeEntry{
		Info:          NodeInfo{ID: "node-1"},
		State:         NodeAlive,
		LastHeartbeat: now,
	}

	state.ProcessHeartbeat("node-1", NodeResourceReport{})

	entry := state.Nodes["node-1"]
	if !entry.LastHeartbeat.After(now) {
		t.Error("heartbeat should update LastHeartbeat")
	}
}

func TestProcessHeartbeat_RecoversSuspectNode(t *testing.T) {
	state := NewMetaState()

	state.Nodes["node-1"] = &NodeEntry{
		Info:  NodeInfo{ID: "node-1"},
		State: NodeSuspect,
	}

	state.ProcessHeartbeat("node-1", NodeResourceReport{})

	if state.Nodes["node-1"].State != NodeAlive {
		t.Errorf("expected NodeAlive, got %s", state.Nodes["node-1"].State)
	}
}

func TestProcessHeartbeat_UnknownNodeIsNoop(t *testing.T) {
	state := NewMetaState()
	// Should not panic.
	state.ProcessHeartbeat("nonexistent", NodeResourceReport{})
}

func TestDetectFailures_AliveToSuspect(t *testing.T) {
	state := NewMetaState()
	baseTime := time.Date(2026, 3, 9, 12, 0, 0, 0, time.UTC)

	state.Nodes["node-1"] = &NodeEntry{
		Info:          NodeInfo{ID: "node-1"},
		State:         NodeAlive,
		LastHeartbeat: baseTime,
	}

	clock := &testClock{now: baseTime.Add(15 * time.Second)}
	dead := state.DetectFailures(clock, 10*time.Second, 30*time.Second)

	if len(dead) != 0 {
		t.Errorf("expected no dead nodes, got %d", len(dead))
	}
	if state.Nodes["node-1"].State != NodeSuspect {
		t.Errorf("expected NodeSuspect, got %s", state.Nodes["node-1"].State)
	}
}

func TestDetectFailures_SuspectToDead(t *testing.T) {
	state := NewMetaState()
	baseTime := time.Date(2026, 3, 9, 12, 0, 0, 0, time.UTC)

	state.Nodes["node-1"] = &NodeEntry{
		Info:          NodeInfo{ID: "node-1"},
		State:         NodeSuspect,
		LastHeartbeat: baseTime,
	}

	clock := &testClock{now: baseTime.Add(35 * time.Second)}
	dead := state.DetectFailures(clock, 10*time.Second, 30*time.Second)

	if len(dead) != 1 {
		t.Fatalf("expected 1 dead node, got %d", len(dead))
	}
	if dead[0] != sharding.NodeID("node-1") {
		t.Errorf("expected node-1 in dead list, got %s", dead[0])
	}
	if state.Nodes["node-1"].State != NodeDead {
		t.Errorf("expected NodeDead, got %s", state.Nodes["node-1"].State)
	}
}

func TestDetectFailures_DeadNodeNotTransitionedAgain(t *testing.T) {
	state := NewMetaState()
	baseTime := time.Date(2026, 3, 9, 12, 0, 0, 0, time.UTC)

	state.Nodes["node-1"] = &NodeEntry{
		Info:          NodeInfo{ID: "node-1"},
		State:         NodeDead,
		LastHeartbeat: baseTime,
	}

	clock := &testClock{now: baseTime.Add(60 * time.Second)}
	dead := state.DetectFailures(clock, 10*time.Second, 30*time.Second)

	// Already dead, should not appear in new dead list.
	if len(dead) != 0 {
		t.Errorf("expected no new dead nodes, got %d", len(dead))
	}
}

func TestNodeState_String(t *testing.T) {
	tests := []struct {
		state NodeState
		want  string
	}{
		{NodeAlive, "alive"},
		{NodeSuspect, "suspect"},
		{NodeDead, "dead"},
		{NodeState(99), "unknown"},
	}

	for _, tt := range tests {
		if got := tt.state.String(); got != tt.want {
			t.Errorf("NodeState(%d).String() = %q, want %q", tt.state, got, tt.want)
		}
	}
}
