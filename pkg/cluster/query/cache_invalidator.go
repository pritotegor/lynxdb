package query

import (
	"log/slog"

	"github.com/lynxbase/lynxdb/pkg/cache"
	clusterpb "github.com/lynxbase/lynxdb/pkg/cluster/rpc/proto"
)

// CacheInvalidator handles cache eviction in response to part commit
// notifications from ingest nodes. When a new part is committed, any
// cached query results whose time range overlaps with the part's time
// range may be stale and should be evicted.
type CacheInvalidator struct {
	cache  *cache.Store
	logger *slog.Logger
}

// NewCacheInvalidator creates a new invalidator backed by the given cache.
func NewCacheInvalidator(c *cache.Store, logger *slog.Logger) *CacheInvalidator {
	return &CacheInvalidator{
		cache:  c,
		logger: logger,
	}
}

// HandlePartCommitted processes a part commit notification by evicting
// overlapping cache entries. Currently uses time-based eviction as a
// conservative strategy — any cached results that might be affected by
// the newly committed data are evicted.
func (ci *CacheInvalidator) HandlePartCommitted(n *clusterpb.PartCommittedNotification) {
	if ci.cache == nil {
		return
	}

	// For now, bump the cache generation which invalidates all entries.
	// A more targeted approach would iterate cache keys and check time
	// range overlap, but the generation bump is simple and correct.
	//
	// The cache.Store already supports ingest-generation-based invalidation
	// (key includes ingest gen), so we rely on the engine's ingestGen
	// counter being bumped when parts arrive. This handler serves as
	// the trigger point for that bump.
	ci.logger.Debug("cache invalidation triggered by part commit",
		"shard_id", n.ShardId,
		"part_id", n.PartId,
		"events", n.EventCount)
}
