package storage

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// SnapshotData holds the serialized state of a store for persistence.
type SnapshotData struct {
	CreatedAt int64             `json:"created_at"`
	Entries   map[string]string `json:"entries"`
	TTLs      map[string]int64  `json:"ttls,omitempty"`
}

// Snapshotter handles periodic and on-demand persistence of store state to disk.
type Snapshotter struct {
	mu       sync.Mutex
	dir      string
	filename string
	stop     chan struct{}
}

// NewSnapshotter creates a new Snapshotter that writes snapshots to the given directory.
func NewSnapshotter(dir string) (*Snapshotter, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("snapshotter: failed to create directory %q: %w", dir, err)
	}
	return &Snapshotter{
		dir:      dir,
		filename: "snapshot.json",
		stop:     make(chan struct{}),
	}, nil
}

// Save serializes the provided SnapshotData and writes it atomically to disk.
func (s *Snapshotter) Save(data SnapshotData) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	data.CreatedAt = time.Now().UnixMilli()

	bytes, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("snapshotter: failed to marshal snapshot: %w", err)
	}

	tmpPath := filepath.Join(s.dir, s.filename+".tmp")
	if err := os.WriteFile(tmpPath, bytes, 0644); err != nil {
		return fmt.Errorf("snapshotter: failed to write temp snapshot: %w", err)
	}

	destPath := filepath.Join(s.dir, s.filename)
	if err := os.Rename(tmpPath, destPath); err != nil {
		return fmt.Errorf("snapshotter: failed to rename snapshot: %w", err)
	}

	return nil
}

// Load reads and deserializes the snapshot from disk.
// Returns ErrNotFound if no snapshot exists yet.
func (s *Snapshotter) Load() (SnapshotData, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	path := filepath.Join(s.dir, s.filename)
	bytes, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return SnapshotData{}, ErrNotFound
	}
	if err != nil {
		return SnapshotData{}, fmt.Errorf("snapshotter: failed to read snapshot: %w", err)
	}

	var data SnapshotData
	if err := json.Unmarshal(bytes, &data); err != nil {
		return SnapshotData{}, fmt.Errorf("snapshotter: failed to unmarshal snapshot: %w", err)
	}

	return data, nil
}

// StartAutoSnapshot begins saving snapshots at the given interval.
// Call Stop to halt the background goroutine.
func (s *Snapshotter) StartAutoSnapshot(interval time.Duration, fn func() SnapshotData) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				_ = s.Save(fn())
			case <-s.stop:
				return
			}
		}
	}()
}

// Stop halts the auto-snapshot background goroutine.
func (s *Snapshotter) Stop() {
	close(s.stop)
}
