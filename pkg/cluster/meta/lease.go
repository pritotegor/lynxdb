package meta

import (
	"fmt"
	"time"

	"github.com/lynxbase/lynxdb/pkg/cluster/sharding"
)

// ShardLease represents a time-limited ownership claim on a shard.
// Only the lease holder may write to the shard.
type ShardLease struct {
	ShardID   sharding.ShardID `msgpack:"shard_id"`
	HolderID  sharding.NodeID  `msgpack:"holder_id"`
	GrantedAt time.Time        `msgpack:"granted_at"`
	ExpiresAt time.Time        `msgpack:"expires_at"`
	Epoch     uint64           `msgpack:"epoch"`
}

// IsValid checks whether the lease is still valid given the current time
// and the maximum allowed clock skew.
func (l *ShardLease) IsValid(clock ClockProvider, maxSkew time.Duration) bool {
	now := clock.Now()

	// Add skew tolerance: the lease is considered valid if the current time
	// is before ExpiresAt + maxSkew. This handles the case where the lease
	// holder's clock is slightly behind the validator's clock.
	return now.Before(l.ExpiresAt.Add(maxSkew))
}

// GrantLeasePayload is the payload for CmdGrantLease.
type GrantLeasePayload struct {
	ShardID       string          `msgpack:"shard_id"`
	HolderID      sharding.NodeID `msgpack:"holder_id"`
	LeaseDuration time.Duration   `msgpack:"lease_duration"`
}

// RenewLeasePayload is the payload for CmdRenewLease.
type RenewLeasePayload struct {
	ShardID       string          `msgpack:"shard_id"`
	HolderID      sharding.NodeID `msgpack:"holder_id"`
	Epoch         uint64          `msgpack:"epoch"`
	LeaseDuration time.Duration   `msgpack:"lease_duration"`
}

// applyGrantLease creates a new lease for a shard.
func (s *MetaState) applyGrantLease(payload []byte) error {
	var p GrantLeasePayload
	if err := UnmarshalPayload(payload, &p); err != nil {
		return fmt.Errorf("meta.applyGrantLease: %w", err)
	}

	now := time.Now()
	epoch := uint64(1)

	// If an existing lease exists, increment its epoch.
	if existing, ok := s.Leases[p.ShardID]; ok {
		epoch = existing.Epoch + 1
	}

	s.Leases[p.ShardID] = &ShardLease{
		HolderID:  p.HolderID,
		GrantedAt: now,
		ExpiresAt: now.Add(p.LeaseDuration),
		Epoch:     epoch,
	}
	s.Version++

	return nil
}

// applyRenewLease extends an existing lease.
// Returns error if the epoch doesn't match (stale renewal).
func (s *MetaState) applyRenewLease(payload []byte) error {
	var p RenewLeasePayload
	if err := UnmarshalPayload(payload, &p); err != nil {
		return fmt.Errorf("meta.applyRenewLease: %w", err)
	}

	existing, ok := s.Leases[p.ShardID]
	if !ok {
		return fmt.Errorf("meta.applyRenewLease: no lease for shard %q", p.ShardID)
	}
	if existing.Epoch != p.Epoch {
		return fmt.Errorf("meta.applyRenewLease: epoch mismatch (got %d, want %d)", p.Epoch, existing.Epoch)
	}
	if existing.HolderID != p.HolderID {
		return fmt.Errorf("meta.applyRenewLease: holder mismatch (got %q, want %q)", p.HolderID, existing.HolderID)
	}

	now := time.Now()
	existing.ExpiresAt = now.Add(p.LeaseDuration)
	s.Version++

	return nil
}
