package ingest

import (
	"context"
	"fmt"
	"io"
	"log/slog"

	"github.com/vmihailenco/msgpack/v5"

	clusterpb "github.com/lynxbase/lynxdb/pkg/cluster/rpc/proto"
	"github.com/lynxbase/lynxdb/pkg/cluster/sharding"
	"github.com/lynxbase/lynxdb/pkg/event"
)

// Service implements the IngestServiceServer gRPC interface.
// It handles incoming IngestBatch and ReplicateBatch RPCs from other
// cluster nodes.
type Service struct {
	clusterpb.UnimplementedIngestServiceServer

	// localIngest is called to ingest events into the local storage engine.
	localIngest func(context.Context, []*event.Event) error

	// shadowBatcher holds replicated events on replica nodes.
	shadowBatcher *ShadowBatcher

	// isrTracker records replication progress.
	isrTracker *ISRTracker

	// selfID is this node's identifier.
	selfID sharding.NodeID

	logger *slog.Logger
}

// NewService creates a new IngestService gRPC handler.
func NewService(
	selfID sharding.NodeID,
	localIngest func(context.Context, []*event.Event) error,
	shadow *ShadowBatcher,
	tracker *ISRTracker,
	logger *slog.Logger,
) *Service {
	return &Service{
		selfID:        selfID,
		localIngest:   localIngest,
		shadowBatcher: shadow,
		isrTracker:    tracker,
		logger:        logger,
	}
}

// IngestBatch handles a forwarded event batch from another node's router.
// It deserializes the msgpack events and ingests them locally.
func (s *Service) IngestBatch(ctx context.Context, req *clusterpb.IngestBatchRequest) (*clusterpb.IngestBatchResponse, error) {
	var events []*event.Event
	if err := msgpack.Unmarshal(req.Events, &events); err != nil {
		return nil, fmt.Errorf("ingest.Service.IngestBatch: unmarshal: %w", err)
	}

	if err := s.localIngest(ctx, events); err != nil {
		return nil, fmt.Errorf("ingest.Service.IngestBatch: local ingest: %w", err)
	}

	return &clusterpb.IngestBatchResponse{
		Ok:       true,
		BatchSeq: req.BatchSeq,
	}, nil
}

// ReplicateBatch handles incoming replication stream from the shard primary.
// Each entry is added to the shadow batcher (NOT flushed to disk). The ISR
// tracker is updated with the highest received batch_seq.
func (s *Service) ReplicateBatch(stream clusterpb.IngestService_ReplicateBatchServer) error {
	var lastSeq uint64
	var lastShardID string

	for {
		entry, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("ingest.Service.ReplicateBatch: recv: %w", err)
		}

		var events []*event.Event
		if err := msgpack.Unmarshal(entry.Events, &events); err != nil {
			s.logger.Error("ReplicateBatch: unmarshal failed",
				"shard_id", entry.ShardId,
				"batch_seq", entry.BatchSeq,
				"error", err)

			continue
		}

		s.shadowBatcher.Add(entry.ShardId, events, entry.BatchSeq)
		lastShardID = entry.ShardId
		lastSeq = entry.BatchSeq
	}

	// Update ISR tracker with the highest replicated seq.
	if lastShardID != "" {
		s.isrTracker.UpdateReplicatedSeq(lastShardID, s.selfID, lastSeq)
	}

	return stream.SendAndClose(&clusterpb.ReplicateBatchResponse{
		Ok:           true,
		LastBatchSeq: lastSeq,
	})
}
