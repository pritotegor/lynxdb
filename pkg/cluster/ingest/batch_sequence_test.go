package ingest

import (
	"fmt"
	"sync"
	"testing"
)

func TestBatchSequencer_Next(t *testing.T) {
	s := NewBatchSequencer()

	// First call returns 1.
	if got := s.Next("shard-a"); got != 1 {
		t.Errorf("expected 1, got %d", got)
	}
	if got := s.Next("shard-a"); got != 2 {
		t.Errorf("expected 2, got %d", got)
	}

	// Independent counter for different shard.
	if got := s.Next("shard-b"); got != 1 {
		t.Errorf("expected 1 for shard-b, got %d", got)
	}
}

func TestBatchSequencer_Current(t *testing.T) {
	s := NewBatchSequencer()

	// No assignment yet.
	if got := s.Current("shard-x"); got != 0 {
		t.Errorf("expected 0 for unassigned shard, got %d", got)
	}

	s.Next("shard-x")
	s.Next("shard-x")
	s.Next("shard-x")

	if got := s.Current("shard-x"); got != 3 {
		t.Errorf("expected 3, got %d", got)
	}
}

func TestBatchSequencer_Set(t *testing.T) {
	s := NewBatchSequencer()
	s.Set("shard-a", 100)

	if got := s.Current("shard-a"); got != 100 {
		t.Errorf("expected 100, got %d", got)
	}

	// Next should continue from 101.
	if got := s.Next("shard-a"); got != 101 {
		t.Errorf("expected 101, got %d", got)
	}
}

func TestBatchSequencer_ConcurrentMonotonicity(t *testing.T) {
	s := NewBatchSequencer()
	const goroutines = 10
	const opsPerGoroutine = 1000
	shardID := "shard-concurrent"

	var wg sync.WaitGroup
	results := make([][]uint64, goroutines)

	for g := 0; g < goroutines; g++ {
		g := g
		results[g] = make([]uint64, opsPerGoroutine)
		wg.Add(1)

		go func() {
			defer wg.Done()
			for i := 0; i < opsPerGoroutine; i++ {
				results[g][i] = s.Next(shardID)
			}
		}()
	}

	wg.Wait()

	// Collect all values and verify uniqueness + no gaps.
	seen := make(map[uint64]bool)
	for g := 0; g < goroutines; g++ {
		for _, seq := range results[g] {
			if seen[seq] {
				t.Fatalf("duplicate sequence: %d", seq)
			}
			seen[seq] = true
		}
	}

	expectedTotal := uint64(goroutines * opsPerGoroutine)
	if got := s.Current(shardID); got != expectedTotal {
		t.Errorf("expected final seq %d, got %d", expectedTotal, got)
	}

	// Verify no gaps: all values from 1..expectedTotal should be present.
	for i := uint64(1); i <= expectedTotal; i++ {
		if !seen[i] {
			t.Fatalf("missing sequence %d", i)
		}
	}
}

func TestBatchSequencer_ConcurrentMultiShard(t *testing.T) {
	s := NewBatchSequencer()
	const shards = 5
	const opsPerShard = 500

	var wg sync.WaitGroup

	for shard := 0; shard < shards; shard++ {
		shardID := fmt.Sprintf("shard-%d", shard)
		wg.Add(1)

		go func() {
			defer wg.Done()
			for i := 0; i < opsPerShard; i++ {
				s.Next(shardID)
			}
		}()
	}

	wg.Wait()

	for shard := 0; shard < shards; shard++ {
		shardID := fmt.Sprintf("shard-%d", shard)
		if got := s.Current(shardID); got != uint64(opsPerShard) {
			t.Errorf("shard %s: expected %d, got %d", shardID, opsPerShard, got)
		}
	}
}
