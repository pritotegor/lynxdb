package meta

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/hashicorp/raft"
	"github.com/vmihailenco/msgpack/v5"

	"github.com/lynxbase/lynxdb/pkg/cluster/rpc"
	clusterpb "github.com/lynxbase/lynxdb/pkg/cluster/rpc/proto"
	"github.com/lynxbase/lynxdb/pkg/cluster/sharding"
	"github.com/lynxbase/lynxdb/pkg/config"
)

// Service implements the MetaServiceServer gRPC interface.
// It mediates between gRPC requests and the Raft consensus layer.
type Service struct {
	clusterpb.UnimplementedMetaServiceServer

	raft   *raft.Raft
	fsm    *MetaFSM
	logger *slog.Logger
	cfg    config.ClusterConfig
}

// NewService creates a new meta gRPC service.
func NewService(r *raft.Raft, fsm *MetaFSM, cfg config.ClusterConfig, logger *slog.Logger) *Service {
	return &Service{
		raft:   r,
		fsm:    fsm,
		logger: logger,
		cfg:    cfg,
	}
}

// Handshake handles a node joining the cluster.
func (s *Service) Handshake(ctx context.Context, req *clusterpb.HandshakeRequest) (*clusterpb.HandshakeResponse, error) {
	if err := rpc.CheckVersion(req.ProtocolVersion); err != nil {
		return nil, err
	}

	// Validate roles.
	if err := validateRoles(req.Roles); err != nil {
		return nil, fmt.Errorf("meta.Service.Handshake: %w", err)
	}

	// Register node via Raft.
	payload, err := MarshalPayload(RegisterNodePayload{
		Info: NodeInfo{
			ID:        sharding.NodeID(req.NodeId),
			Addr:      req.Addr,
			GRPCAddr:  req.GrpcAddr,
			Roles:     req.Roles,
			StartedAt: time.Now(),
			Version:   req.Version,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("meta.Service.Handshake: marshal: %w", err)
	}

	cmdData, err := EncodeCommand(&Command{Type: CmdRegisterNode, Data: payload})
	if err != nil {
		return nil, fmt.Errorf("meta.Service.Handshake: encode: %w", err)
	}

	future := s.raft.Apply(cmdData, 5*time.Second)
	if err := future.Error(); err != nil {
		// If not leader, return leader address for redirect.
		if leaderAddr, leaderID := s.raft.LeaderWithID(); leaderAddr != "" {
			return &clusterpb.HandshakeResponse{
				ProtocolVersion: rpc.ProtocolVersion,
				LeaderId:        string(leaderID),
				LeaderAddr:      string(leaderAddr),
			}, fmt.Errorf("not leader, redirect to %s", leaderAddr)
		}

		return nil, fmt.Errorf("meta.Service.Handshake: raft apply: %w", err)
	}

	// Return current shard map.
	state := s.fsm.State()
	smData, err := msgpack.Marshal(state.ShardMap)
	if err != nil {
		return nil, fmt.Errorf("meta.Service.Handshake: marshal shard map: %w", err)
	}

	return &clusterpb.HandshakeResponse{
		ProtocolVersion: rpc.ProtocolVersion,
		LeaderId:        string(s.cfg.NodeID),
		ShardMap:        smData,
		ShardMapEpoch:   state.ShardMap.Epoch,
	}, nil
}

// Heartbeat processes a node health report.
func (s *Service) Heartbeat(_ context.Context, req *clusterpb.HeartbeatRequest) (*clusterpb.HeartbeatResponse, error) {
	state := s.fsm.State()
	state.ProcessHeartbeat(sharding.NodeID(req.NodeId), NodeResourceReport{})

	_, leaderID := s.raft.LeaderWithID()

	return &clusterpb.HeartbeatResponse{
		Ok:       true,
		LeaderId: string(leaderID),
	}, nil
}

// WatchShardMap streams shard map updates to the caller.
func (s *Service) WatchShardMap(req *clusterpb.WatchShardMapRequest, stream clusterpb.MetaService_WatchShardMapServer) error {
	currentEpoch := req.CurrentEpoch
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-stream.Context().Done():
			return stream.Context().Err()
		case <-ticker.C:
			state := s.fsm.State()
			if state.ShardMap.Epoch > currentEpoch {
				smData, err := msgpack.Marshal(state.ShardMap)
				if err != nil {
					s.logger.Error("WatchShardMap: marshal failed", "error", err)

					continue
				}

				if err := stream.Send(&clusterpb.ShardMapUpdate{
					ShardMap: smData,
					Epoch:    state.ShardMap.Epoch,
				}); err != nil {
					return err
				}
				currentEpoch = state.ShardMap.Epoch
			}
		}
	}
}

// RenewLease extends a shard lease.
func (s *Service) RenewLease(_ context.Context, req *clusterpb.RenewLeaseRequest) (*clusterpb.RenewLeaseResponse, error) {
	payload, err := MarshalPayload(RenewLeasePayload{
		ShardID:       req.ShardId,
		HolderID:      sharding.NodeID(req.NodeId),
		Epoch:         req.Epoch,
		LeaseDuration: s.cfg.LeaseDuration.Duration(),
	})
	if err != nil {
		return nil, fmt.Errorf("meta.Service.RenewLease: marshal: %w", err)
	}

	cmdData, err := EncodeCommand(&Command{Type: CmdRenewLease, Data: payload})
	if err != nil {
		return nil, fmt.Errorf("meta.Service.RenewLease: encode: %w", err)
	}

	future := s.raft.Apply(cmdData, 5*time.Second)
	if err := future.Error(); err != nil {
		if leaderAddr, _ := s.raft.LeaderWithID(); leaderAddr != "" {
			return &clusterpb.RenewLeaseResponse{
				Granted:    false,
				LeaderAddr: string(leaderAddr),
			}, nil
		}

		return nil, fmt.Errorf("meta.Service.RenewLease: raft apply: %w", err)
	}

	// Check if the apply returned an error (e.g., epoch mismatch).
	if resp := future.Response(); resp != nil {
		if applyErr, ok := resp.(error); ok {
			return &clusterpb.RenewLeaseResponse{Granted: false}, applyErr
		}
	}

	state := s.fsm.State()
	lease := state.Leases[req.ShardId]

	var expiresNs int64
	if lease != nil {
		expiresNs = lease.ExpiresAt.UnixNano()
	}

	return &clusterpb.RenewLeaseResponse{
		Granted:          true,
		ExpiresAtUnixNs: expiresNs,
	}, nil
}

// RequestCompaction grants a compaction task.
func (s *Service) RequestCompaction(_ context.Context, req *clusterpb.RequestCompactionRequest) (*clusterpb.RequestCompactionResponse, error) {
	payload, err := MarshalPayload(GrantCompactionPayload{
		ShardID:        req.ShardId,
		AssignedNode:   sharding.NodeID(req.NodeId),
		CatalogVersion: req.CatalogVersion,
		InputParts:     req.InputParts,
		TargetLevel:    int(req.TargetLevel),
		TTL:            10 * time.Minute,
	})
	if err != nil {
		return nil, fmt.Errorf("meta.Service.RequestCompaction: marshal: %w", err)
	}

	cmdData, err := EncodeCommand(&Command{Type: CmdGrantCompaction, Data: payload})
	if err != nil {
		return nil, fmt.Errorf("meta.Service.RequestCompaction: encode: %w", err)
	}

	future := s.raft.Apply(cmdData, 5*time.Second)
	if err := future.Error(); err != nil {
		if leaderAddr, _ := s.raft.LeaderWithID(); leaderAddr != "" {
			return &clusterpb.RequestCompactionResponse{
				Granted:    false,
				LeaderAddr: string(leaderAddr),
			}, nil
		}

		return nil, fmt.Errorf("meta.Service.RequestCompaction: raft apply: %w", err)
	}

	if resp := future.Response(); resp != nil {
		if applyErr, ok := resp.(error); ok {
			return &clusterpb.RequestCompactionResponse{Granted: false}, applyErr
		}
	}

	return &clusterpb.RequestCompactionResponse{
		Granted: true,
		TaskId:  req.ShardId,
	}, nil
}

// ReleaseCompaction releases a compaction lock.
func (s *Service) ReleaseCompaction(_ context.Context, req *clusterpb.ReleaseCompactionRequest) (*clusterpb.ReleaseCompactionResponse, error) {
	payload, err := MarshalPayload(ReleaseCompactionPayload{
		ShardID:     req.TaskId,
		Success:     req.Success,
		OutputParts: req.OutputParts,
	})
	if err != nil {
		return nil, fmt.Errorf("meta.Service.ReleaseCompaction: marshal: %w", err)
	}

	cmdData, err := EncodeCommand(&Command{Type: CmdReleaseCompaction, Data: payload})
	if err != nil {
		return nil, fmt.Errorf("meta.Service.ReleaseCompaction: encode: %w", err)
	}

	future := s.raft.Apply(cmdData, 5*time.Second)
	if err := future.Error(); err != nil {
		return nil, fmt.Errorf("meta.Service.ReleaseCompaction: raft apply: %w", err)
	}

	return &clusterpb.ReleaseCompactionResponse{Ok: true}, nil
}

// ProposeDrain initiates draining a shard from a node.
func (s *Service) ProposeDrain(_ context.Context, req *clusterpb.ProposeDrainRequest) (*clusterpb.ProposeDrainResponse, error) {
	payload, err := MarshalPayload(ProposeDrainPayload{
		ShardID: req.ShardId,
		NodeID:  sharding.NodeID(req.NodeId),
	})
	if err != nil {
		return nil, fmt.Errorf("meta.Service.ProposeDrain: marshal: %w", err)
	}

	cmdData, err := EncodeCommand(&Command{Type: CmdProposeDrain, Data: payload})
	if err != nil {
		return nil, fmt.Errorf("meta.Service.ProposeDrain: encode: %w", err)
	}

	future := s.raft.Apply(cmdData, 5*time.Second)
	if err := future.Error(); err != nil {
		if leaderAddr, _ := s.raft.LeaderWithID(); leaderAddr != "" {
			return &clusterpb.ProposeDrainResponse{
				Ok:         false,
				LeaderAddr: string(leaderAddr),
			}, nil
		}

		return nil, fmt.Errorf("meta.Service.ProposeDrain: raft apply: %w", err)
	}

	if resp := future.Response(); resp != nil {
		if applyErr, ok := resp.(error); ok {
			return &clusterpb.ProposeDrainResponse{Ok: false}, applyErr
		}
	}

	return &clusterpb.ProposeDrainResponse{Ok: true}, nil
}

// CompleteDrain marks a shard drain as complete and assigns a new primary.
func (s *Service) CompleteDrain(_ context.Context, req *clusterpb.CompleteDrainRequest) (*clusterpb.CompleteDrainResponse, error) {
	payload, err := MarshalPayload(CompleteDrainPayload{
		ShardID:      req.ShardId,
		NodeID:       sharding.NodeID(req.NodeId),
		NewPrimaryID: sharding.NodeID(req.NewPrimaryId),
	})
	if err != nil {
		return nil, fmt.Errorf("meta.Service.CompleteDrain: marshal: %w", err)
	}

	cmdData, err := EncodeCommand(&Command{Type: CmdCompleteDrain, Data: payload})
	if err != nil {
		return nil, fmt.Errorf("meta.Service.CompleteDrain: encode: %w", err)
	}

	future := s.raft.Apply(cmdData, 5*time.Second)
	if err := future.Error(); err != nil {
		if leaderAddr, _ := s.raft.LeaderWithID(); leaderAddr != "" {
			return &clusterpb.CompleteDrainResponse{
				Ok:         false,
				LeaderAddr: string(leaderAddr),
			}, nil
		}

		return nil, fmt.Errorf("meta.Service.CompleteDrain: raft apply: %w", err)
	}

	if resp := future.Response(); resp != nil {
		if applyErr, ok := resp.(error); ok {
			return &clusterpb.CompleteDrainResponse{Ok: false}, applyErr
		}
	}

	return &clusterpb.CompleteDrainResponse{Ok: true}, nil
}

// validRoles is the set of recognized cluster role names.
var validRoles = map[string]bool{
	"meta":   true,
	"ingest": true,
	"query":  true,
}

// validateRoles checks that all role strings are recognized.
func validateRoles(roles []string) error {
	if len(roles) == 0 {
		return fmt.Errorf("no roles specified")
	}

	for _, r := range roles {
		if !validRoles[strings.ToLower(strings.TrimSpace(r))] {
			return fmt.Errorf("unknown role %q (valid: meta, ingest, query)", r)
		}
	}

	return nil
}
