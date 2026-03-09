package ingest

import (
	"context"
	"log/slog"
	"time"

	"github.com/lynxbase/lynxdb/pkg/cluster/rpc"
	clusterpb "github.com/lynxbase/lynxdb/pkg/cluster/rpc/proto"
	"github.com/lynxbase/lynxdb/pkg/cluster/sharding"
)

// notifyTimeout is the per-node timeout for fire-and-forget part notifications.
const notifyTimeout = 2 * time.Second

// PartNotifier sends fire-and-forget PartCommittedNotification RPCs to all
// query nodes when a new part is flushed and uploaded to S3. Query nodes
// use this notification to refresh their local segment cache or part catalog.
//
// Notifications are best-effort: failures are logged but never returned to
// the caller. Each notification is sent in its own goroutine with a 2-second
// timeout to avoid blocking the ingest hot path.
type PartNotifier struct {
	clientPool *rpc.ClientPool
	queryNodes func() []NodeAddress
	logger     *slog.Logger
}

// NodeAddress holds the gRPC address for a query node.
type NodeAddress struct {
	ID       sharding.NodeID
	GRPCAddr string
}

// NewPartNotifier creates a new PartNotifier.
// queryNodes is a function that returns the current set of query node addresses.
// It is called on every notification to get the latest membership.
func NewPartNotifier(pool *rpc.ClientPool, queryNodes func() []NodeAddress, logger *slog.Logger) *PartNotifier {
	return &PartNotifier{
		clientPool: pool,
		queryNodes: queryNodes,
		logger:     logger,
	}
}

// NotifyPartCommitted sends a PartCommittedNotification to all query nodes.
// Each send is fire-and-forget in a separate goroutine with a 2s timeout.
// Errors are logged but never returned.
func (n *PartNotifier) NotifyPartCommitted(ctx context.Context, notification *clusterpb.PartCommittedNotification) {
	nodes := n.queryNodes()
	if len(nodes) == 0 {
		return
	}

	for _, node := range nodes {
		node := node // capture loop variable
		go func() {
			sendCtx, cancel := context.WithTimeout(ctx, notifyTimeout)
			defer cancel()

			conn, err := n.clientPool.GetConn(sendCtx, node.GRPCAddr)
			if err != nil {
				n.logger.Debug("part notification: dial failed",
					"node", node.ID,
					"addr", node.GRPCAddr,
					"error", err)

				return
			}

			client := clusterpb.NewQueryServiceClient(conn)
			_, err = client.NotifyPartCommitted(sendCtx, notification)
			if err != nil {
				n.logger.Debug("part notification: RPC failed",
					"node", node.ID,
					"part_id", notification.PartId,
					"error", err)

				return
			}

			n.logger.Debug("part notification sent",
				"node", node.ID,
				"part_id", notification.PartId,
				"shard_id", notification.ShardId)
		}()
	}
}
