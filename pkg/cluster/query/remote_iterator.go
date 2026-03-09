package query

import (
	"context"
	"fmt"
	"io"

	"github.com/vmihailenco/msgpack/v5"

	clusterpb "github.com/lynxbase/lynxdb/pkg/cluster/rpc/proto"
	"github.com/lynxbase/lynxdb/pkg/engine/pipeline"
	"github.com/lynxbase/lynxdb/pkg/event"
)

// RemoteShardIterator implements pipeline.Iterator by reading results from a
// gRPC ExecuteQuery stream. Each gRPC QueryResult message contains a single
// msgpack-encoded row. The iterator batches them into pipeline.Batch units.
type RemoteShardIterator struct {
	client    clusterpb.QueryServiceClient
	req       *clusterpb.ExecuteQueryRequest
	stream    clusterpb.QueryService_ExecuteQueryClient
	batchSize int
	done      bool
	schema    []pipeline.FieldInfo
}

// NewRemoteShardIterator creates a new iterator for a remote shard query.
func NewRemoteShardIterator(
	client clusterpb.QueryServiceClient,
	req *clusterpb.ExecuteQueryRequest,
	batchSize int,
) *RemoteShardIterator {
	if batchSize <= 0 {
		batchSize = pipeline.DefaultBatchSize
	}

	return &RemoteShardIterator{
		client:    client,
		req:       req,
		batchSize: batchSize,
	}
}

// Init opens the gRPC stream. Must be called before Next().
func (r *RemoteShardIterator) Init(ctx context.Context) error {
	stream, err := r.client.ExecuteQuery(ctx, r.req)
	if err != nil {
		return fmt.Errorf("RemoteShardIterator.Init: %w", err)
	}
	r.stream = stream

	return nil
}

// Next reads up to batchSize rows from the stream and returns them as a Batch.
// Returns (nil, nil) when the stream is exhausted.
func (r *RemoteShardIterator) Next(ctx context.Context) (*pipeline.Batch, error) {
	if r.done {
		return nil, nil
	}

	batch := pipeline.NewBatch(r.batchSize)
	for batch.Len < r.batchSize {
		msg, err := r.stream.Recv()
		if err != nil {
			if err == io.EOF {
				r.done = true

				break
			}

			return nil, fmt.Errorf("RemoteShardIterator.Next: %w", err)
		}

		if len(msg.Row) == 0 {
			continue
		}

		var row map[string]event.Value
		if err := msgpack.Unmarshal(msg.Row, &row); err != nil {
			return nil, fmt.Errorf("RemoteShardIterator.Next: decode row: %w", err)
		}

		batch.AddRow(row)
	}

	if batch.Len == 0 {
		return nil, nil
	}

	return batch, nil
}

// Close releases the gRPC stream.
func (r *RemoteShardIterator) Close() error {
	if r.stream != nil {
		// CloseSend signals the server we're done. Ignore errors since
		// the stream may already be closed from server side.
		_ = r.stream.CloseSend()
	}

	return nil
}

// Schema returns output column metadata. Since remote shards may return
// varying schemas, we return an empty list and rely on batch column discovery.
func (r *RemoteShardIterator) Schema() []pipeline.FieldInfo {
	return r.schema
}
