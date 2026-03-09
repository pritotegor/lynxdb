package ingest

import (
	"testing"
	"time"
)

func TestMetaLossDetector_InitialState(t *testing.T) {
	d := NewMetaLossDetector(100*time.Millisecond, testLogger())

	if d.IsMetaLoss() {
		t.Error("should not be in meta-loss immediately after creation")
	}
}

func TestMetaLossDetector_TransitionAfterTimeout(t *testing.T) {
	d := NewMetaLossDetector(50*time.Millisecond, testLogger())

	// Wait for timeout to elapse.
	time.Sleep(80 * time.Millisecond)
	d.check()

	if !d.IsMetaLoss() {
		t.Error("should be in meta-loss after timeout elapsed")
	}
}

func TestMetaLossDetector_RecoveryOnRenewal(t *testing.T) {
	d := NewMetaLossDetector(50*time.Millisecond, testLogger())

	// Enter meta-loss.
	time.Sleep(80 * time.Millisecond)
	d.check()

	if !d.IsMetaLoss() {
		t.Fatal("expected meta-loss")
	}

	// Recover.
	d.RecordLeaseRenewal()

	if d.IsMetaLoss() {
		t.Error("should recover after lease renewal")
	}
}

func TestMetaLossDetector_RenewalResetsTimer(t *testing.T) {
	d := NewMetaLossDetector(100*time.Millisecond, testLogger())

	// Wait 60ms, renew, wait another 60ms — should NOT enter meta-loss.
	time.Sleep(60 * time.Millisecond)
	d.RecordLeaseRenewal()
	time.Sleep(60 * time.Millisecond)
	d.check()

	if d.IsMetaLoss() {
		t.Error("should not be meta-loss — renewal reset the timer")
	}
}

func TestMetaLossDetector_DefaultTimeout(t *testing.T) {
	d := NewMetaLossDetector(0, testLogger())

	if d.timeout != defaultMetaLossTimeout {
		t.Errorf("expected default timeout %v, got %v", defaultMetaLossTimeout, d.timeout)
	}
}

func TestMetaLossDetector_IdempotentTransitions(t *testing.T) {
	d := NewMetaLossDetector(50*time.Millisecond, testLogger())

	time.Sleep(80 * time.Millisecond)

	// Multiple checks should not panic or misbehave.
	d.check()
	d.check()
	d.check()

	if !d.IsMetaLoss() {
		t.Error("should be in meta-loss")
	}

	// Multiple renewals should not panic.
	d.RecordLeaseRenewal()
	d.RecordLeaseRenewal()

	if d.IsMetaLoss() {
		t.Error("should be recovered")
	}
}
