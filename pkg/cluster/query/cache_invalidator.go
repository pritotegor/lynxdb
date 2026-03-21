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

	if err := ci.cache.Clear(); err != nil {
		ci.logger.Warn("cache invalidation failed after part commit",
			"shard_id", n.ShardId,
			"part_id", n.PartId,
			"error", err)
		return
	}

	ci.logger.Debug("cache invalidated after part commit",
		"shard_id", n.ShardId,
		"part_id", n.PartId,
		"events", n.EventCount)
}
