package cluster

import (
	"testing"
	"time"
)

func TestSystemClock(t *testing.T) {
	clock := SystemClock{}
	before := time.Now()
	got := clock.Now()
	after := time.Now()

	if got.Before(before) || got.After(after) {
		t.Errorf("SystemClock.Now() = %v, not between %v and %v", got, before, after)
	}
}

func TestMockClock(t *testing.T) {
	base := time.Date(2026, 3, 9, 12, 0, 0, 0, time.UTC)
	clock := NewMockClock(base)

	if got := clock.Now(); !got.Equal(base) {
		t.Errorf("MockClock.Now() = %v, want %v", got, base)
	}

	clock.Advance(5 * time.Second)
	expected := base.Add(5 * time.Second)

	if got := clock.Now(); !got.Equal(expected) {
		t.Errorf("after Advance(5s): Now() = %v, want %v", got, expected)
	}

	newTime := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	clock.Set(newTime)

	if got := clock.Now(); !got.Equal(newTime) {
		t.Errorf("after Set(): Now() = %v, want %v", got, newTime)
	}
}

func TestMockClock_ImplementsInterface(t *testing.T) {
	var _ ClockProvider = SystemClock{}
	var _ ClockProvider = &MockClock{}
}
