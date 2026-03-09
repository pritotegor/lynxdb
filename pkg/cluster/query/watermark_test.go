package query

import (
	"testing"
	"time"
)

func TestWatermarkTracker_EmptyReturnsZero(t *testing.T) {
	wt := NewWatermarkTracker(0, 0)
	if got := wt.GlobalWatermark(); got != 0 {
		t.Errorf("expected 0, got %d", got)
	}
}

func TestWatermarkTracker_SingleNode(t *testing.T) {
	wt := NewWatermarkTracker(0, 0) // defaults: 200ms skew, 5s stale

	mark := time.Now().UnixNano()
	wt.Update("node-1", mark)

	got := wt.GlobalWatermark()
	expected := mark - DefaultMaxSkew.Nanoseconds()
	if got != expected {
		t.Errorf("expected %d, got %d (diff=%dms)", expected, got,
			(expected-got)/int64(time.Millisecond))
	}
}

func TestWatermarkTracker_MultiNode_MinWins(t *testing.T) {
	wt := NewWatermarkTracker(0, 0)

	now := time.Now().UnixNano()
	slow := now - int64(5*time.Second) // 5s behind
	fast := now

	wt.Update("slow-node", slow)
	wt.Update("fast-node", fast)

	got := wt.GlobalWatermark()
	expected := slow - DefaultMaxSkew.Nanoseconds()
	if got != expected {
		t.Errorf("expected slow node watermark - skew, got diff=%dms",
			(expected-got)/int64(time.Millisecond))
	}
}

func TestWatermarkTracker_Remove(t *testing.T) {
	wt := NewWatermarkTracker(0, 0)

	now := time.Now().UnixNano()
	wt.Update("node-1", now-int64(10*time.Second))
	wt.Update("node-2", now)

	wt.Remove("node-1")

	// After removing the slow node, watermark should advance.
	got := wt.GlobalWatermark()
	expected := now - DefaultMaxSkew.Nanoseconds()
	if got != expected {
		t.Errorf("expected watermark at fast node, diff=%dms",
			(expected-got)/int64(time.Millisecond))
	}
}

func TestWatermarkTracker_NegativeClampedToZero(t *testing.T) {
	wt := NewWatermarkTracker(time.Hour, time.Hour) // huge skew

	wt.Update("node-1", int64(time.Millisecond)) // tiny watermark

	got := wt.GlobalWatermark()
	if got != 0 {
		t.Errorf("expected 0 (clamped), got %d", got)
	}
}
