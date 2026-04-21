package storage

import "testing"

func TestSet_And_Get(t *testing.T) {
	s := NewStore()
	if err := s.Set("hello", "world"); err != nil {
		t.Fatalf("Set failed: %v", err)
	}
	v, err := s.Get("hello")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if v != "world" {
		t.Fatalf("expected 'world', got %q", v)
	}
}

func TestGet_NotFound(t *testing.T) {
	s := NewStore()
	_, err := s.Get("missing")
	if err != ErrKeyNotFound {
		t.Fatalf("expected ErrKeyNotFound, got %v", err)
	}
}

func TestSet_EmptyKey(t *testing.T) {
	s := NewStore()
	if err := s.Set("", "v"); err != ErrEmptyKey {
		t.Fatalf("expected ErrEmptyKey, got %v", err)
	}
}

func TestDelete(t *testing.T) {
	s := NewStore()
	_ = s.Set("k", "v")
	if err := s.Delete("k"); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}
	_, err := s.Get("k")
	if err != ErrKeyNotFound {
		t.Fatalf("expected ErrKeyNotFound after delete, got %v", err)
	}
}

func TestDelete_NotFound(t *testing.T) {
	s := NewStore()
	if err := s.Delete("ghost"); err != ErrKeyNotFound {
		t.Fatalf("expected ErrKeyNotFound, got %v", err)
	}
}

func TestKeys(t *testing.T) {
	s := NewStore()
	_ = s.Set("a", "1")
	_ = s.Set("b", "2")
	keys := s.Keys()
	if len(keys) != 2 {
		t.Fatalf("expected 2 keys, got %d", len(keys))
	}
}

// TestSet_OverwriteValue verifies that setting a key twice updates the value
// rather than returning an error or keeping the old value.
func TestSet_OverwriteValue(t *testing.T) {
	s := NewStore()
	_ = s.Set("key", "original")
	if err := s.Set("key", "updated"); err != nil {
		t.Fatalf("Set (overwrite) failed: %v", err)
	}
	v, err := s.Get("key")
	if err != nil {
		t.Fatalf("Get after overwrite failed: %v", err)
	}
	if v != "updated" {
		t.Fatalf("expected 'updated', got %q", v)
	}
}

// TestKeys_EmptyStore verifies that Keys() returns an empty slice (not nil)
// when no entries have been added. Useful for callers that range over the result.
func TestKeys_EmptyStore(t *testing.T) {
	s := NewStore()
	keys := s.Keys()
	if len(keys) != 0 {
		t.Fatalf("expected 0 keys on empty store, got %d", len(keys))
	}
}

// TestSet_EmptyValue verifies that storing an empty string as a value is
// allowed — empty values are valid, only empty keys are rejected.
// Note: I ran into a bug in an earlier project where empty values were
// silently dropped; adding this to make sure that doesn't happen here.
func TestSet_EmptyValue(t *testing.T) {
	s := NewStore()
	if err := s.Set("emptyval", ""); err != nil {
		t.Fatalf("Set with empty value should succeed, got: %v", err)
	}
	v, err := s.Get("emptyval")
	if err != nil {
		t.Fatalf("Get after Set with empty value failed: %v", err)
	}
	if v != "" {
		t.Fatalf("expected empty string, got %q", v)
	}
}

// TestDelete_ThenSet verifies that a key can be re-added after being deleted.
// This caught an issue in a similar store I worked on where tombstone entries
// blocked subsequent writes to the same key.
func TestDelete_ThenSet(t *testing.T) {
	s := NewStore()
	_ = s.Set("reuse", "first")
	if err := s.Delete("reuse"); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}
	// Re-inserting after delete should work cleanly — no leftover state.
	if err := s.Set("reuse", "second"); err != nil {
		t.Fatalf("Set after Delete failed: %v", err)
	}
	v, err := s.Get("reuse")
	if err != nil {
		t.Fatalf("Get after Delete+Set failed: %v", err)
	}
	if v != "second" {
		t.Fatalf("expected 'second', got %q", v)
	}
}
