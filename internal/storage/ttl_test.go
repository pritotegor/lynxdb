package storage

import (
	"testing"
	"time"
)

func newTTLStore(t *testing.T) *TTLStore {
	t.Helper()
	s := NewStore()
	ts := NewTTLStore(s)
	t.Cleanup(ts.Stop)
	return ts
}

func TestTTL_SetAndGet(t *testing.T) {
	ts := newTTLStore(t)
	if err := ts.SetWithTTL("k", "v", 5*time.Second); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	val, err := ts.Get("k")
	if err != nil {
		t.Fatalf("expected value, got error: %v", err)
	}
	if val != "v" {
		t.Fatalf("expected 'v', got %q", val)
	}
}

func TestTTL_Expired(t *testing.T) {
	ts := newTTLStore(t)
	if err := ts.SetWithTTL("expiring", "data", 50*time.Millisecond); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	time.Sleep(100 * time.Millisecond)
	_, err := ts.Get("expiring")
	if err != ErrKeyNotFound {
		t.Fatalf("expected ErrKeyNotFound, got %v", err)
	}
}

func TestTTL_DeleteClearsTTL(t *testing.T) {
	ts := newTTLStore(t)
	_ = ts.SetWithTTL("del", "val", 10*time.Second)
	_ = ts.Delete("del")
	_, err := ts.Get("del")
	if err != ErrKeyNotFound {
		t.Fatalf("expected ErrKeyNotFound after delete, got %v", err)
	}
}

func TestTTL_TTLOf(t *testing.T) {
	ts := newTTLStore(t)
	_ = ts.SetWithTTL("ttlkey", "v", 5*time.Second)
	remaining := ts.TTLOf("ttlkey")
	if remaining <= 0 || remaining > 5*time.Second {
		t.Fatalf("unexpected TTL: %v", remaining)
	}
}

func TestTTL_TTLOf_NoExpiry(t *testing.T) {
	ts := newTTLStore(t)
	if d := ts.TTLOf("nokey"); d != 0 {
		t.Fatalf("expected 0, got %v", d)
	}
}

func TestTTL_EvictExpired(t *testing.T) {
	ts := newTTLStore(t)
	_ = ts.SetWithTTL("e1", "v1", 50*time.Millisecond)
	_ = ts.SetWithTTL("e2", "v2", 50*time.Millisecond)
	time.Sleep(1100 * time.Millisecond)
	_, err1 := ts.Get("e1")
	_, err2 := ts.Get("e2")
	if err1 != ErrKeyNotFound || err2 != ErrKeyNotFound {
		t.Fatal("expected both keys to be evicted")
	}
}
