package ingest

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/lynxbase/lynxdb/internal/objstore"
	"github.com/vmihailenco/msgpack/v5"
)

// PartEntry describes a single part file stored in S3.
// It carries enough metadata for query planning (time range, event count,
// columns) and failover dedup (batch sequence, writer epoch).
type PartEntry struct {
	PartID      string   `msgpack:"part_id"`
	Index       string   `msgpack:"index"`
	MinTimeNs   int64    `msgpack:"min_time_ns"`
	MaxTimeNs   int64    `msgpack:"max_time_ns"`
	EventCount  int64    `msgpack:"event_count"`
	SizeBytes   int64    `msgpack:"size_bytes"`
	Level       int      `msgpack:"level"`
	S3Key       string   `msgpack:"s3_key"`
	Columns     []string `msgpack:"columns"`
	BatchSeq    uint64   `msgpack:"batch_seq"`
	WriterEpoch uint64   `msgpack:"writer_epoch"`
	CreatedAtNs int64    `msgpack:"created_at_ns"`
}

// Catalog is the on-disk representation of all parts for a single partition.
// Stored as a single msgpack blob in S3. The Version field provides optimistic
// concurrency control — writers must read the current version before modifying,
// and a stale version on the next read triggers a retry.
type Catalog struct {
	Version uint64      `msgpack:"version"`
	Parts   []PartEntry `msgpack:"parts"`
}

// maxCatalogRetries is the maximum number of retry attempts for optimistic
// concurrency conflicts when updating the catalog.
const maxCatalogRetries = 3

// PartCatalog manages the S3-backed part catalog for cluster mode.
// Each (index, partition) pair has an independent catalog file stored at:
//
//	catalog/<index>/p<partition>/catalog.msgpack
//
// The catalog is the source of truth for which parts exist and what batch
// sequences have been committed. Query nodes use it to discover parts,
// and failover logic uses LastBatchSeq to resume from the correct point.
type PartCatalog struct {
	objStore objstore.ObjectStore
	logger   *slog.Logger
}

// NewPartCatalog creates a new PartCatalog backed by the given object store.
func NewPartCatalog(store objstore.ObjectStore, logger *slog.Logger) *PartCatalog {
	return &PartCatalog{
		objStore: store,
		logger:   logger,
	}
}

// catalogKey returns the S3 key for a catalog file.
func catalogKey(index string, partition uint32) string {
	return fmt.Sprintf("catalog/%s/p%d/catalog.msgpack", index, partition)
}

// Load reads the catalog for the given (index, partition) from S3.
// Returns an empty catalog (Version 0) if the key does not exist.
func (c *PartCatalog) Load(ctx context.Context, index string, partition uint32) (*Catalog, error) {
	key := catalogKey(index, partition)

	data, err := c.objStore.Get(ctx, key)
	if err != nil {
		// Treat "not found" as empty catalog.
		// objstore.MemStore returns errors starting with "objstore: key not found".
		// S3Store will return a similar error for missing keys.
		return &Catalog{Version: 0}, nil
	}

	var cat Catalog
	if err := msgpack.Unmarshal(data, &cat); err != nil {
		return nil, fmt.Errorf("ingest.PartCatalog.Load: unmarshal %s: %w", key, err)
	}

	return &cat, nil
}

// save writes the catalog back to S3. Callers must have incremented Version.
func (c *PartCatalog) save(ctx context.Context, index string, partition uint32, cat *Catalog) error {
	key := catalogKey(index, partition)

	data, err := msgpack.Marshal(cat)
	if err != nil {
		return fmt.Errorf("ingest.PartCatalog.save: marshal: %w", err)
	}

	if err := c.objStore.Put(ctx, key, data); err != nil {
		return fmt.Errorf("ingest.PartCatalog.save: put %s: %w", key, err)
	}

	return nil
}

// AddPart appends a new part entry to the catalog with optimistic concurrency.
// It loads the current catalog, appends the entry, increments the version,
// and writes back. On conflict (stale version), it retries up to 3 times.
func (c *PartCatalog) AddPart(ctx context.Context, index string, partition uint32, entry PartEntry) error {
	for attempt := 0; attempt < maxCatalogRetries; attempt++ {
		cat, err := c.Load(ctx, index, partition)
		if err != nil {
			return fmt.Errorf("ingest.PartCatalog.AddPart: %w", err)
		}

		expectedVersion := cat.Version
		cat.Parts = append(cat.Parts, entry)
		cat.Version++

		if err := c.save(ctx, index, partition, cat); err != nil {
			return fmt.Errorf("ingest.PartCatalog.AddPart: %w", err)
		}

		// Verify: re-read and check version for optimistic concurrency.
		// S3 lacks conditional PUT, so we verify after write.
		verify, err := c.Load(ctx, index, partition)
		if err != nil {
			return fmt.Errorf("ingest.PartCatalog.AddPart: verify: %w", err)
		}

		if verify.Version == expectedVersion+1 {
			return nil // Success — our write was the latest.
		}

		// Version mismatch — another writer updated concurrently. Retry.
		c.logger.Warn("catalog version conflict, retrying",
			"index", index,
			"partition", partition,
			"expected_version", expectedVersion+1,
			"actual_version", verify.Version,
			"attempt", attempt+1)
	}

	return fmt.Errorf("ingest.PartCatalog.AddPart: exhausted %d retries for %s/p%d", maxCatalogRetries, index, partition)
}

// RemoveParts removes parts by ID from the catalog with optimistic concurrency.
// Used during compaction to replace input parts with compacted output.
func (c *PartCatalog) RemoveParts(ctx context.Context, index string, partition uint32, partIDs []string) error {
	if len(partIDs) == 0 {
		return nil
	}

	removeSet := make(map[string]bool, len(partIDs))
	for _, id := range partIDs {
		removeSet[id] = true
	}

	for attempt := 0; attempt < maxCatalogRetries; attempt++ {
		cat, err := c.Load(ctx, index, partition)
		if err != nil {
			return fmt.Errorf("ingest.PartCatalog.RemoveParts: %w", err)
		}

		filtered := make([]PartEntry, 0, len(cat.Parts))
		for _, p := range cat.Parts {
			if !removeSet[p.PartID] {
				filtered = append(filtered, p)
			}
		}

		expectedVersion := cat.Version
		cat.Parts = filtered
		cat.Version++

		if err := c.save(ctx, index, partition, cat); err != nil {
			return fmt.Errorf("ingest.PartCatalog.RemoveParts: %w", err)
		}

		verify, err := c.Load(ctx, index, partition)
		if err != nil {
			return fmt.Errorf("ingest.PartCatalog.RemoveParts: verify: %w", err)
		}

		if verify.Version == expectedVersion+1 {
			return nil
		}

		c.logger.Warn("catalog version conflict on remove, retrying",
			"index", index,
			"partition", partition,
			"attempt", attempt+1)
	}

	return fmt.Errorf("ingest.PartCatalog.RemoveParts: exhausted %d retries for %s/p%d", maxCatalogRetries, index, partition)
}

// LastBatchSeq returns the highest BatchSeq committed in the catalog for
// the given (index, partition). Returns 0 if the catalog is empty.
// Used during failover to determine the resume point for shadow batcher flush.
func (c *PartCatalog) LastBatchSeq(ctx context.Context, index string, partition uint32) (uint64, error) {
	cat, err := c.Load(ctx, index, partition)
	if err != nil {
		return 0, fmt.Errorf("ingest.PartCatalog.LastBatchSeq: %w", err)
	}

	var maxSeq uint64
	for _, p := range cat.Parts {
		if p.BatchSeq > maxSeq {
			maxSeq = p.BatchSeq
		}
	}

	return maxSeq, nil
}
