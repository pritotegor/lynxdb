package meta

import (
	"fmt"
	"time"

	"github.com/lynxbase/lynxdb/pkg/cluster/sharding"
)

// CompactionTask represents an active compaction operation on a shard.
// Only one compaction task can be active per shard at a time.
type CompactionTask struct {
	ShardID        string          `msgpack:"shard_id"`
	AssignedNode   sharding.NodeID `msgpack:"assigned_node"`
	CatalogVersion uint64          `msgpack:"catalog_version"`
	InputParts     []string        `msgpack:"input_parts"`
	TargetLevel    int             `msgpack:"target_level"`
	CreatedAt      time.Time       `msgpack:"created_at"`
	ExpiresAt      time.Time       `msgpack:"expires_at"`
}

// GrantCompactionPayload is the payload for CmdGrantCompaction.
type GrantCompactionPayload struct {
	ShardID        string          `msgpack:"shard_id"`
	AssignedNode   sharding.NodeID `msgpack:"assigned_node"`
	CatalogVersion uint64          `msgpack:"catalog_version"`
	InputParts     []string        `msgpack:"input_parts"`
	TargetLevel    int             `msgpack:"target_level"`
	TTL            time.Duration   `msgpack:"ttl"`
}

// ReleaseCompactionPayload is the payload for CmdReleaseCompaction.
type ReleaseCompactionPayload struct {
	ShardID     string   `msgpack:"shard_id"`
	Success     bool     `msgpack:"success"`
	OutputParts []string `msgpack:"output_parts"`
}

// applyGrantCompaction assigns a compaction task.
// Returns error if a task already exists for the shard.
func (s *MetaState) applyGrantCompaction(payload []byte) error {
	var p GrantCompactionPayload
	if err := UnmarshalPayload(payload, &p); err != nil {
		return fmt.Errorf("meta.applyGrantCompaction: %w", err)
	}

	if _, exists := s.Compaction[p.ShardID]; exists {
		return fmt.Errorf("meta.applyGrantCompaction: compaction already active for shard %q", p.ShardID)
	}

	now := time.Now()
	s.Compaction[p.ShardID] = &CompactionTask{
		ShardID:        p.ShardID,
		AssignedNode:   p.AssignedNode,
		CatalogVersion: p.CatalogVersion,
		InputParts:     p.InputParts,
		TargetLevel:    p.TargetLevel,
		CreatedAt:      now,
		ExpiresAt:      now.Add(p.TTL),
	}
	s.Version++

	return nil
}

// applyReleaseCompaction removes a compaction task.
func (s *MetaState) applyReleaseCompaction(payload []byte) error {
	var p ReleaseCompactionPayload
	if err := UnmarshalPayload(payload, &p); err != nil {
		return fmt.Errorf("meta.applyReleaseCompaction: %w", err)
	}

	delete(s.Compaction, p.ShardID)
	s.Version++

	return nil
}

// ExpireCompactionTasks removes expired compaction tasks and returns them.
func (s *MetaState) ExpireCompactionTasks(clock ClockProvider) []*CompactionTask {
	now := clock.Now()
	var expired []*CompactionTask

	for id, task := range s.Compaction {
		if now.After(task.ExpiresAt) {
			expired = append(expired, task)
			delete(s.Compaction, id)
		}
	}

	if len(expired) > 0 {
		s.Version++
	}

	return expired
}
