package query

import (
	"log/slog"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/cluster"
	"github.com/lynxbase/lynxdb/pkg/cluster/sharding"
)

func TestAffinityRouter_PrefersPrimary(t *testing.T) {
	cache := cluster.NewShardMapCache()
	ar := NewAffinityRouter(cache, slog.Default())

	got := ar.PickNode("primary", []sharding.NodeID{"replica-1", "replica-2"})
	if got != "primary" {
		t.Errorf("expected primary, got %s", got)
	}
}

func TestAffinityRouter_FallbackWhenOverloaded(t *testing.T) {
	cache := cluster.NewShardMapCache()
	ar := NewAffinityRouter(cache, slog.Default())

	// Overload the primary.
	for i := 0; i < defaultLoadThreshold+1; i++ {
		ar.IncrementLoad("primary")
	}

	got := ar.PickNode("primary", []sharding.NodeID{"replica-1"})
	if got != "replica-1" {
		t.Errorf("expected replica-1 (primary overloaded), got %s", got)
	}
}

func TestAffinityRouter_LeastLoadedReplica(t *testing.T) {
	cache := cluster.NewShardMapCache()
	ar := NewAffinityRouter(cache, slog.Default())

	// Overload primary.
	for i := 0; i < defaultLoadThreshold+1; i++ {
		ar.IncrementLoad("primary")
	}
	// Load replica-1 more than replica-2.
	for i := 0; i < 50; i++ {
		ar.IncrementLoad("replica-1")
	}
	for i := 0; i < 10; i++ {
		ar.IncrementLoad("replica-2")
	}

	got := ar.PickNode("primary", []sharding.NodeID{"replica-1", "replica-2"})
	if got != "replica-2" {
		t.Errorf("expected replica-2 (least loaded), got %s", got)
	}
}

func TestAffinityRouter_DecrementLoad(t *testing.T) {
	cache := cluster.NewShardMapCache()
	ar := NewAffinityRouter(cache, slog.Default())

	ar.IncrementLoad("node-1")
	ar.IncrementLoad("node-1")
	ar.DecrementLoad("node-1")

	load := ar.getLoad("node-1")
	if load != 1 {
		t.Errorf("expected load 1, got %d", load)
	}
}
