package cluster

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/hashicorp/raft"

	"github.com/lynxbase/lynxdb/pkg/cluster/meta"
	"github.com/lynxbase/lynxdb/pkg/cluster/rpc"
	clusterpb "github.com/lynxbase/lynxdb/pkg/cluster/rpc/proto"
	"github.com/lynxbase/lynxdb/pkg/config"
)

// Node orchestrates the cluster lifecycle for a single LynxDB process.
// It manages gRPC transport, Raft consensus (if meta role), and the
// local shard map cache.
type Node struct {
	cfg           config.ClusterConfig
	roles         RoleSet
	grpcServer    *rpc.Server
	clientPool    *rpc.ClientPool
	raft           *raft.Raft                           // non-nil only for meta nodes
	metaFSM        *meta.MetaFSM                        // non-nil only for meta nodes
	metaService    *meta.Service                        // non-nil only for meta nodes
	ingestService  clusterpb.IngestServiceServer        // non-nil only for ingest nodes
	shardMapCache  *ShardMapCache
	clock          ClockProvider
	logger         *slog.Logger
	dataDir        string
}

// NewNode creates a new cluster node from config. Call Start() to begin operations.
func NewNode(cfg config.ClusterConfig, dataDir string, logger *slog.Logger) (*Node, error) {
	roles, err := ParseRoles(cfg.Roles)
	if err != nil {
		return nil, fmt.Errorf("cluster.NewNode: %w", err)
	}

	n := &Node{
		cfg:           cfg,
		roles:         roles,
		clientPool:    rpc.NewClientPool(),
		shardMapCache: NewShardMapCache(),
		clock:         SystemClock{},
		logger:        logger.With("component", "cluster"),
		dataDir:       dataDir,
	}

	return n, nil
}

// Start initializes the cluster node:
//  1. Checks clock sync
//  2. Starts gRPC server
//  3. Sets up Raft (if meta role)
//  4. Joins cluster via Handshake
func (n *Node) Start(ctx context.Context) error {
	// Check clock synchronization (best-effort).
	if err := CheckClockSync(n.logger); err != nil {
		return fmt.Errorf("cluster.Node.Start: %w", err)
	}

	// Create and start gRPC server.
	grpcAddr := fmt.Sprintf("0.0.0.0:%d", n.cfg.GRPCPort)
	n.grpcServer = rpc.NewServer(grpcAddr, n.logger)

	// Set up Raft if this node has the meta role.
	if n.roles.Has(RoleMeta) {
		n.metaFSM = meta.NewMetaFSM(n.logger)

		r, err := meta.SetupRaft(n.cfg, n.metaFSM, n.dataDir, n.logger)
		if err != nil {
			return fmt.Errorf("cluster.Node.Start: %w", err)
		}
		n.raft = r

		n.metaService = meta.NewService(r, n.metaFSM, n.cfg, n.logger)
		clusterpb.RegisterMetaServiceServer(n.grpcServer.GRPCServer(), n.metaService)
	}

	// Start gRPC server in background.
	go func() {
		if err := n.grpcServer.Start(ctx); err != nil {
			n.logger.Error("gRPC server error", "error", err)
		}
	}()

	n.logger.Info("cluster node started",
		"node_id", n.cfg.NodeID,
		"roles", n.roles.String(),
		"grpc_addr", grpcAddr)

	return nil
}

// Stop gracefully shuts down the cluster node.
func (n *Node) Stop() error {
	n.logger.Info("stopping cluster node", "node_id", n.cfg.NodeID)

	// Shut down Raft first (if meta role).
	if n.raft != nil {
		if f := n.raft.Shutdown(); f.Error() != nil {
			n.logger.Error("raft shutdown error", "error", f.Error())
		}
	}

	// Stop gRPC server.
	if n.grpcServer != nil {
		n.grpcServer.Stop()
	}

	// Close client connections.
	if n.clientPool != nil {
		if err := n.clientPool.Close(); err != nil {
			n.logger.Error("client pool close error", "error", err)
		}
	}

	return nil
}

// ShardMapCache returns the local shard map cache.
func (n *Node) ShardMapCache() *ShardMapCache {
	return n.shardMapCache
}

// IsLeader reports whether this node is the current Raft leader.
// Always returns false for non-meta nodes.
func (n *Node) IsLeader() bool {
	if n.raft == nil {
		return false
	}

	return n.raft.State() == raft.Leader
}

// RegisterIngestService registers the cluster IngestService on the gRPC server
// and stores a reference for later wiring. Must be called after Start (which
// creates the gRPC server) but before any remote nodes connect.
//
// This is called by the engine's InitCluster method because the engine owns
// the localIngest callback — the node cannot construct the service alone.
// The service type is clusterpb.IngestServiceServer to avoid an import cycle
// between pkg/cluster and pkg/cluster/ingest.
func (n *Node) RegisterIngestService(svc clusterpb.IngestServiceServer) {
	n.ingestService = svc
	clusterpb.RegisterIngestServiceServer(n.grpcServer.GRPCServer(), svc)
	n.logger.Info("registered IngestService on gRPC server")
}

// IngestService returns the cluster IngestService, or nil if this node
// does not have the ingest role.
func (n *Node) IngestService() clusterpb.IngestServiceServer {
	return n.ingestService
}

// RegisterQueryService registers the cluster QueryService on the gRPC server
// and stores a reference for later wiring. Must be called after Start (which
// creates the gRPC server).
//
// This is called by the engine's InitClusterQuery method because the engine
// owns the local query engine — the node cannot construct the service alone.
func (n *Node) RegisterQueryService(svc clusterpb.QueryServiceServer) {
	clusterpb.RegisterQueryServiceServer(n.grpcServer.GRPCServer(), svc)
	n.logger.Info("registered QueryService on gRPC server")
}

// ClientPool returns the shared gRPC client connection pool.
func (n *Node) ClientPool() *rpc.ClientPool {
	return n.clientPool
}

// GRPCServer returns the gRPC server, or nil if Start has not been called.
func (n *Node) GRPCServer() *rpc.Server {
	return n.grpcServer
}
