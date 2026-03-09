package query

import (
	"github.com/lynxbase/lynxdb/pkg/spl2"
)

// JoinStrategy describes how a distributed join is executed.
type JoinStrategy int

const (
	// JoinBroadcast sends the smaller (right) side to all shards.
	// Used when the right side has a small result set (head N <= 10000).
	JoinBroadcast JoinStrategy = iota
	// JoinColocated executes the join on each shard independently.
	// Used when both sides share the shard key matching the join field.
	JoinColocated
	// JoinCoordinator materializes both sides on the coordinator.
	// Default fallback when no optimization applies.
	JoinCoordinator
)

// String returns a human-readable name for the join strategy.
func (s JoinStrategy) String() string {
	switch s {
	case JoinBroadcast:
		return "broadcast"
	case JoinColocated:
		return "colocated"
	case JoinCoordinator:
		return "coordinator"
	default:
		return "unknown"
	}
}

// broadcastThreshold is the maximum result count for the right side
// before we fall back from broadcast to coordinator join.
const broadcastThreshold = 10_000

// PlanDistributedJoin selects the optimal strategy for a distributed join.
//
// Strategy selection:
//   - Broadcast: if the right side has `head N` with N <= broadcastThreshold,
//     the right side results are small enough to broadcast to all shards.
//   - Colocated: if both sides share a shard key that matches the join field,
//     each shard can execute the join independently (not yet implemented).
//   - Coordinator: default fallback — both sides are materialized on the
//     coordinator node and joined there.
func PlanDistributedJoin(cmd *spl2.JoinCommand, _ *spl2.QueryHints) JoinStrategy {
	if cmd == nil || cmd.Subquery == nil {
		return JoinCoordinator
	}

	// Check if the right side (subquery) has a head limit small enough for broadcast.
	if limit := terminalHeadLimit(cmd.Subquery); limit > 0 && limit <= broadcastThreshold {
		return JoinBroadcast
	}

	// TODO: Check for colocated join when both sides share the shard key
	// matching the join field. This requires checking the FROM clauses
	// and shard configuration.

	return JoinCoordinator
}

// terminalHeadLimit returns the head limit of a query's last command,
// or 0 if the query doesn't end with a HeadCommand.
func terminalHeadLimit(q *spl2.Query) int {
	if q == nil || len(q.Commands) == 0 {
		return 0
	}

	last := q.Commands[len(q.Commands)-1]
	if head, ok := last.(*spl2.HeadCommand); ok {
		return head.Count
	}

	return 0
}
