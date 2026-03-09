package ingest

import (
	"context"
	"log/slog"
	"sync/atomic"
	"time"
)

// defaultMetaLossTimeout is the duration after the last successful lease
// renewal before the node assumes meta-loss and enters degraded mode.
const defaultMetaLossTimeout = 30 * time.Second

// defaultMetaLossCheckInterval is how often the background goroutine
// checks for meta-loss transitions.
const defaultMetaLossCheckInterval = 5 * time.Second

// MetaLossDetector monitors connectivity to the meta leader by tracking
// the time of the last successful lease renewal. When the timeout expires,
// the node enters "meta-loss" mode where:
//   - Writes are accepted without lease validation
//   - Parts are flagged with WriterEpoch=0 (indicating unvalidated ownership)
//   - Compaction coordination is skipped
//
// On recovery (meta becomes reachable again), the node reconciles by
// re-acquiring leases and re-validating ownership.
//
// Thread-safe.
type MetaLossDetector struct {
	lastRenewal atomic.Int64 // UnixNano of last successful lease renewal
	metaLost    atomic.Bool  // true when in meta-loss mode
	timeout     time.Duration
	logger      *slog.Logger
}

// NewMetaLossDetector creates a new MetaLossDetector with the given timeout.
// If timeout is 0, defaultMetaLossTimeout (30s) is used.
func NewMetaLossDetector(timeout time.Duration, logger *slog.Logger) *MetaLossDetector {
	if timeout == 0 {
		timeout = defaultMetaLossTimeout
	}

	d := &MetaLossDetector{
		timeout: timeout,
		logger:  logger,
	}
	// Initialize lastRenewal to now so we don't immediately enter meta-loss on startup.
	d.lastRenewal.Store(time.Now().UnixNano())

	return d
}

// RecordLeaseRenewal records a successful lease renewal from the meta leader.
// This resets the meta-loss timer. If the node was in meta-loss mode, it
// transitions back to normal operation.
func (d *MetaLossDetector) RecordLeaseRenewal() {
	d.lastRenewal.Store(time.Now().UnixNano())

	if d.metaLost.CompareAndSwap(true, false) {
		d.logger.Info("meta-loss recovered: lease renewal succeeded, resuming normal operation")
	}
}

// IsMetaLoss reports whether the node is currently in meta-loss mode.
func (d *MetaLossDetector) IsMetaLoss() bool {
	return d.metaLost.Load()
}

// check evaluates whether the timeout has elapsed since the last renewal
// and transitions the state if needed.
func (d *MetaLossDetector) check() {
	lastNano := d.lastRenewal.Load()
	lastTime := time.Unix(0, lastNano)
	elapsed := time.Since(lastTime)

	if elapsed >= d.timeout {
		if d.metaLost.CompareAndSwap(false, true) {
			d.logger.Warn("meta-loss detected: no lease renewal for timeout period, entering degraded mode",
				"timeout", d.timeout,
				"elapsed", elapsed)
		}
	}
}

// Start begins the background meta-loss detection loop.
// The loop runs every 5 seconds and checks whether the timeout has elapsed.
// It exits when the context is cancelled.
func (d *MetaLossDetector) Start(ctx context.Context) {
	ticker := time.NewTicker(defaultMetaLossCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			d.check()
		}
	}
}
