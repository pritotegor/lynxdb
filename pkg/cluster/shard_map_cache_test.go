package cluster

import (
	"sync"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/cluster/sharding"
)

func TestShardMapCache_GetReturnsEmpty(t *testing.T) {
	c := NewShardMapCache()
	sm := c.Get()

	if sm == nil {
		t.Fatal("expected non-nil shard map from new cache")
	}
	if sm.Len() != 0 {
		t.Errorf("expected empty shard map, got %d entries", sm.Len())
	}
}

func TestShardMapCache_Update(t *testing.T) {
	c := NewShardMapCache()

	sm := sharding.NewShardMap()
	sm.Epoch = 42
	c.Update(sm)

	got := c.Get()
	if got.Epoch != 42 {
		t.Errorf("expected epoch 42, got %d", got.Epoch)
	}
}

func TestShardMapCache_ConcurrentReadWrite(t *testing.T) {
	c := NewShardMapCache()

	var wg sync.WaitGroup
	const writers = 10
	const readers = 100
	const iterations = 1000

	// Writers update the shard map with increasing epochs.
	for w := 0; w < writers; w++ {
		wg.Add(1)

		go func(id int) {
			defer wg.Done()

			for i := 0; i < iterations; i++ {
				sm := sharding.NewShardMap()
				sm.Epoch = uint64(id*iterations + i)
				c.Update(sm)
			}
		}(w)
	}

	// Readers continuously read the shard map.
	for r := 0; r < readers; r++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			for i := 0; i < iterations; i++ {
				sm := c.Get()
				if sm == nil {
					t.Error("Get() returned nil during concurrent access")

					return
				}
			}
		}()
	}

	wg.Wait()
}
