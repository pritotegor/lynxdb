package query

import (
	"sync"
	"time"

	"github.com/lynxbase/lynxdb/pkg/cluster/sharding"
)

// DefaultMaxSkew is the maximum allowed clock skew subtracted from the
// global watermark to ensure correctness of ordered event delivery.
const DefaultMaxSkew = 200 * time.Millisecond

// DefaultStaleTimeout is how long we wait for a node's watermark update
// before force-advancing it to avoid blocking global watermark progress.
const DefaultStaleTimeout = 5 * time.Second

// WatermarkTracker tracks per-node watermarks for distributed live tail.
// The global watermark is the minimum of all active node watermarks minus
// a configurable skew allowance. Events with timestamps <= GlobalWatermark
// are safe to deliver to the client.
type WatermarkTracker struct {
	mu           sync.Mutex
	nodeMarks    map[sharding.NodeID]nodeWatermark
	maxSkew      time.Duration
	staleTimeout time.Duration
}

type nodeWatermark struct {
	watermarkNs int64
	lastUpdate  time.Time
}

// NewWatermarkTracker creates a tracker with the given skew and staleness settings.
func NewWatermarkTracker(maxSkew, staleTimeout time.Duration) *WatermarkTracker {
	if maxSkew == 0 {
		maxSkew = DefaultMaxSkew
	}
	if staleTimeout == 0 {
		staleTimeout = DefaultStaleTimeout
	}

	return &WatermarkTracker{
		nodeMarks:    make(map[sharding.NodeID]nodeWatermark),
		maxSkew:      maxSkew,
		staleTimeout: staleTimeout,
	}
}

// Update records a watermark for the given node.
func (w *WatermarkTracker) Update(nodeID sharding.NodeID, watermarkNs int64) {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.nodeMarks[nodeID] = nodeWatermark{
		watermarkNs: watermarkNs,
		lastUpdate:  time.Now(),
	}
}

// GlobalWatermark returns the minimum watermark across all active nodes,
// minus the max skew allowance. Stale nodes (no update for > staleTimeout)
// are force-advanced to avoid blocking delivery.
func (w *WatermarkTracker) GlobalWatermark() int64 {
	w.mu.Lock()
	defer w.mu.Unlock()

	if len(w.nodeMarks) == 0 {
		return 0
	}

	now := time.Now()
	var minMark int64 = -1

	for nodeID, nm := range w.nodeMarks {
		mark := nm.watermarkNs

		// Force-advance stale nodes.
		if now.Sub(nm.lastUpdate) > w.staleTimeout {
			mark = now.Add(-w.maxSkew).UnixNano()
			w.nodeMarks[nodeID] = nodeWatermark{
				watermarkNs: mark,
				lastUpdate:  now,
			}
		}

		if minMark < 0 || mark < minMark {
			minMark = mark
		}
	}

	// Subtract max skew for safety.
	result := minMark - w.maxSkew.Nanoseconds()
	if result < 0 {
		result = 0
	}

	return result
}

// Remove removes a node from tracking (e.g., when its stream closes).
func (w *WatermarkTracker) Remove(nodeID sharding.NodeID) {
	w.mu.Lock()
	defer w.mu.Unlock()

	delete(w.nodeMarks, nodeID)
}
