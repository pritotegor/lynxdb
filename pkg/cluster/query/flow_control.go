package query

import (
	"context"
	"log/slog"
)

// DefaultMaxConcurrentShards is the default limit on concurrent shard queries.
const DefaultMaxConcurrentShards = 50

// FlowController limits the number of concurrent shard queries using a
// channel-based semaphore. This prevents a single scatter-gather query
// from overwhelming the cluster with too many concurrent RPCs.
type FlowController struct {
	sem    chan struct{}
	logger *slog.Logger
}

// NewFlowController creates a flow controller with the given concurrency limit.
func NewFlowController(maxConcurrentShards int, logger *slog.Logger) *FlowController {
	if maxConcurrentShards <= 0 {
		maxConcurrentShards = DefaultMaxConcurrentShards
	}

	return &FlowController{
		sem:    make(chan struct{}, maxConcurrentShards),
		logger: logger,
	}
}

// Acquire blocks until a slot is available or the context is cancelled.
func (fc *FlowController) Acquire(ctx context.Context) error {
	select {
	case fc.sem <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Release returns a slot to the semaphore.
func (fc *FlowController) Release() {
	<-fc.sem
}
