package query

import (
	"testing"
)

func TestCheckPartialFailure_AllSucceeded(t *testing.T) {
	c := &Coordinator{
		cfg: CoordinatorConfig{PartialFailureThreshold: 0.5},
	}
	meta := &QueryMeta{ShardsTotal: 10, ShardsSuccess: 10}
	if err := c.checkPartialFailure(meta); err != nil {
		t.Errorf("expected no error, got %v", err)
	}
}

func TestCheckPartialFailure_AboveThreshold(t *testing.T) {
	c := &Coordinator{
		cfg: CoordinatorConfig{PartialFailureThreshold: 0.5},
	}
	meta := &QueryMeta{ShardsTotal: 10, ShardsSuccess: 6, ShardsFailed: 4}
	if err := c.checkPartialFailure(meta); err != nil {
		t.Errorf("60%% success should pass 50%% threshold, got %v", err)
	}
}

func TestCheckPartialFailure_BelowThreshold(t *testing.T) {
	c := &Coordinator{
		cfg: CoordinatorConfig{PartialFailureThreshold: 0.5},
	}
	meta := &QueryMeta{ShardsTotal: 10, ShardsSuccess: 3, ShardsFailed: 7}
	if err := c.checkPartialFailure(meta); err == nil {
		t.Error("30% success should fail 50% threshold, but got no error")
	}
}

func TestCheckPartialFailure_ZeroShards(t *testing.T) {
	c := &Coordinator{
		cfg: CoordinatorConfig{PartialFailureThreshold: 0.5},
	}
	meta := &QueryMeta{ShardsTotal: 0}
	if err := c.checkPartialFailure(meta); err != nil {
		t.Errorf("zero shards should not error, got %v", err)
	}
}

func TestCheckPartialFailure_ExactThreshold(t *testing.T) {
	c := &Coordinator{
		cfg: CoordinatorConfig{PartialFailureThreshold: 0.5},
	}
	meta := &QueryMeta{ShardsTotal: 10, ShardsSuccess: 5, ShardsFailed: 5}
	// 50% == threshold, should pass (>= not >).
	if err := c.checkPartialFailure(meta); err != nil {
		t.Errorf("exact threshold should pass, got %v", err)
	}
}

func TestCoordinatorConfig_Defaults(t *testing.T) {
	c := NewCoordinator(nil, nil, nil, CoordinatorConfig{}, nil)
	if c.cfg.ShardQueryTimeout != 30e9 { // 30s in ns
		t.Errorf("expected 30s default timeout, got %v", c.cfg.ShardQueryTimeout)
	}
	if c.cfg.PartialFailureThreshold != DefaultPartialFailureThreshold {
		t.Errorf("expected %f default threshold, got %f",
			DefaultPartialFailureThreshold, c.cfg.PartialFailureThreshold)
	}
}
