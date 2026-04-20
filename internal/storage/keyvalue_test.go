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
