package ingest

import (
	"testing"
	"time"

	"github.com/lynxbase/lynxdb/pkg/event"
)

func makeEvents(n int) []*event.Event {
	events := make([]*event.Event, n)
	for i := range events {
		events[i] = event.NewEvent(time.Now(), "test event")
	}

	return events
}

func TestShadowBatcher_AddAndFlush(t *testing.T) {
	sb := NewShadowBatcher()

	sb.Add("shard-a", makeEvents(3), 1)
	sb.Add("shard-a", makeEvents(2), 2)
	sb.Add("shard-a", makeEvents(1), 3)

	// Flush after seq 1 — should get events from seq 2 and 3.
	events := sb.FlushShadow("shard-a", 1)
	if len(events) != 3 { // 2 + 1
		t.Errorf("expected 3 events, got %d", len(events))
	}

	// Buffer should be cleared after flush.
	events = sb.FlushShadow("shard-a", 0)
	if len(events) != 0 {
		t.Errorf("expected 0 events after second flush, got %d", len(events))
	}
}

func TestShadowBatcher_FlushAfterAll(t *testing.T) {
	sb := NewShadowBatcher()

	sb.Add("shard-a", makeEvents(5), 10)

	// Flush after seq 10 — nothing should be returned.
	events := sb.FlushShadow("shard-a", 10)
	if len(events) != 0 {
		t.Errorf("expected 0 events when afterSeq >= all batch_seqs, got %d", len(events))
	}
}

func TestShadowBatcher_FlushBeforeAll(t *testing.T) {
	sb := NewShadowBatcher()

	sb.Add("shard-a", makeEvents(3), 5)
	sb.Add("shard-a", makeEvents(2), 6)

	// Flush after seq 0 — should get everything.
	events := sb.FlushShadow("shard-a", 0)
	if len(events) != 5 {
		t.Errorf("expected 5 events, got %d", len(events))
	}
}

func TestShadowBatcher_UnknownShard(t *testing.T) {
	sb := NewShadowBatcher()

	events := sb.FlushShadow("unknown", 0)
	if events != nil {
		t.Errorf("expected nil for unknown shard, got %v", events)
	}
}

func TestShadowBatcher_IndependentShards(t *testing.T) {
	sb := NewShadowBatcher()

	sb.Add("shard-a", makeEvents(3), 1)
	sb.Add("shard-b", makeEvents(5), 1)

	eventsA := sb.FlushShadow("shard-a", 0)
	eventsB := sb.FlushShadow("shard-b", 0)

	if len(eventsA) != 3 {
		t.Errorf("shard-a: expected 3, got %d", len(eventsA))
	}
	if len(eventsB) != 5 {
		t.Errorf("shard-b: expected 5, got %d", len(eventsB))
	}
}

func TestShadowBatcher_BufferedCount(t *testing.T) {
	sb := NewShadowBatcher()

	if sb.BufferedCount("shard-a") != 0 {
		t.Error("expected 0 for empty shard")
	}

	sb.Add("shard-a", makeEvents(1), 1)
	sb.Add("shard-a", makeEvents(1), 2)

	if sb.BufferedCount("shard-a") != 2 {
		t.Errorf("expected 2, got %d", sb.BufferedCount("shard-a"))
	}
}

func TestShadowBatcher_Clear(t *testing.T) {
	sb := NewShadowBatcher()

	sb.Add("shard-a", makeEvents(5), 1)
	sb.Clear("shard-a")

	if sb.BufferedCount("shard-a") != 0 {
		t.Error("expected 0 after clear")
	}

	events := sb.FlushShadow("shard-a", 0)
	if events != nil {
		t.Error("expected nil after clear")
	}
}

func TestShadowBatcher_AddCopiesSlice(t *testing.T) {
	sb := NewShadowBatcher()

	events := makeEvents(3)
	sb.Add("shard-a", events, 1)

	// Mutate original slice.
	events[0] = nil

	// Flush should still have all events intact.
	flushed := sb.FlushShadow("shard-a", 0)
	for i, e := range flushed {
		if e == nil {
			t.Errorf("event %d is nil — Add should copy the slice", i)
		}
	}
}
