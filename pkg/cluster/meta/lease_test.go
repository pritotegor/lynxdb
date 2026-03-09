package meta

import (
	"testing"
	"time"
)

type testClock struct {
	now time.Time
}

func (c *testClock) Now() time.Time { return c.now }

func TestShardLease_IsValid(t *testing.T) {
	now := time.Date(2026, 3, 9, 12, 0, 0, 0, time.UTC)
	clock := &testClock{now: now}

	lease := &ShardLease{
		HolderID:  "node-1",
		GrantedAt: now,
		ExpiresAt: now.Add(10 * time.Second),
		Epoch:     1,
	}

	// Still valid at grant time.
	if !lease.IsValid(clock, 50*time.Millisecond) {
		t.Error("lease should be valid at grant time")
	}

	// Still valid 9s later.
	clock.now = now.Add(9 * time.Second)
	if !lease.IsValid(clock, 50*time.Millisecond) {
		t.Error("lease should be valid at 9s")
	}

	// Still valid at expiration + within skew.
	clock.now = now.Add(10*time.Second + 30*time.Millisecond)
	if !lease.IsValid(clock, 50*time.Millisecond) {
		t.Error("lease should be valid within skew tolerance")
	}

	// Invalid beyond expiration + skew.
	clock.now = now.Add(10*time.Second + 100*time.Millisecond)
	if lease.IsValid(clock, 50*time.Millisecond) {
		t.Error("lease should be invalid past skew tolerance")
	}
}

func TestApplyGrantLease_IncreasesEpoch(t *testing.T) {
	state := NewMetaState()

	// First grant: epoch should be 1.
	payload1, _ := MarshalPayload(GrantLeasePayload{
		ShardID:       "shard-1",
		HolderID:      "node-1",
		LeaseDuration: 10 * time.Second,
	})
	if err := state.applyGrantLease(payload1); err != nil {
		t.Fatalf("applyGrantLease: %v", err)
	}
	if state.Leases["shard-1"].Epoch != 1 {
		t.Errorf("expected epoch 1, got %d", state.Leases["shard-1"].Epoch)
	}

	// Re-grant: epoch should increment to 2.
	payload2, _ := MarshalPayload(GrantLeasePayload{
		ShardID:       "shard-1",
		HolderID:      "node-2",
		LeaseDuration: 10 * time.Second,
	})
	if err := state.applyGrantLease(payload2); err != nil {
		t.Fatalf("applyGrantLease: %v", err)
	}
	if state.Leases["shard-1"].Epoch != 2 {
		t.Errorf("expected epoch 2, got %d", state.Leases["shard-1"].Epoch)
	}
	if state.Leases["shard-1"].HolderID != "node-2" {
		t.Errorf("expected holder node-2, got %s", state.Leases["shard-1"].HolderID)
	}
}

func TestApplyRenewLease_Validations(t *testing.T) {
	state := NewMetaState()

	// Grant a lease first.
	p, _ := MarshalPayload(GrantLeasePayload{
		ShardID:       "shard-1",
		HolderID:      "node-1",
		LeaseDuration: 10 * time.Second,
	})
	_ = state.applyGrantLease(p)

	tests := []struct {
		name    string
		payload RenewLeasePayload
		wantErr bool
	}{
		{
			name:    "valid renewal",
			payload: RenewLeasePayload{ShardID: "shard-1", HolderID: "node-1", Epoch: 1, LeaseDuration: 20 * time.Second},
			wantErr: false,
		},
		{
			name:    "no such shard",
			payload: RenewLeasePayload{ShardID: "shard-999", HolderID: "node-1", Epoch: 1},
			wantErr: true,
		},
		{
			name:    "wrong epoch",
			payload: RenewLeasePayload{ShardID: "shard-1", HolderID: "node-1", Epoch: 99},
			wantErr: true,
		},
		{
			name:    "wrong holder",
			payload: RenewLeasePayload{ShardID: "shard-1", HolderID: "node-2", Epoch: 1},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, _ := MarshalPayload(tt.payload)
			err := state.applyRenewLease(data)
			if (err != nil) != tt.wantErr {
				t.Errorf("wantErr=%v, got err=%v", tt.wantErr, err)
			}
		})
	}
}
