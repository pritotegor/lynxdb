package query

import (
	"context"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"
)

func TestFlowController_AcquireRelease(t *testing.T) {
	fc := NewFlowController(2, slog.Default())

	ctx := context.Background()
	if err := fc.Acquire(ctx); err != nil {
		t.Fatalf("first acquire: %v", err)
	}
	if err := fc.Acquire(ctx); err != nil {
		t.Fatalf("second acquire: %v", err)
	}

	// Third acquire should block — use a short timeout to prove it.
	tctx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()

	err := fc.Acquire(tctx)
	if err == nil {
		t.Fatal("expected context deadline, got nil")
	}

	// Release one slot — next acquire should succeed.
	fc.Release()
	if err := fc.Acquire(ctx); err != nil {
		t.Fatalf("acquire after release: %v", err)
	}
}

func TestFlowController_CancelledContext(t *testing.T) {
	fc := NewFlowController(1, slog.Default())
	ctx := context.Background()
	if err := fc.Acquire(ctx); err != nil {
		t.Fatalf("first acquire: %v", err)
	}

	cancelCtx, cancel := context.WithCancel(ctx)
	cancel() // cancel immediately

	err := fc.Acquire(cancelCtx)
	if err == nil {
		t.Fatal("expected error from cancelled context")
	}
}

func TestFlowController_DefaultCapacity(t *testing.T) {
	fc := NewFlowController(0, slog.Default())
	// Should default to 50 — just verify we can acquire more than 1.
	ctx := context.Background()
	var acquired int32
	for i := 0; i < 10; i++ {
		if err := fc.Acquire(ctx); err != nil {
			t.Fatalf("acquire %d: %v", i, err)
		}
		atomic.AddInt32(&acquired, 1)
	}
	if acquired != 10 {
		t.Errorf("expected 10 acquired, got %d", acquired)
	}
}
