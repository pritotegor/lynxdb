package storage

import (
	"testing"
)

func TestSet_And_Get(t *testing.T) {
	s := NewStore()
	err := s.Set("hello", []byte("world"))
	if err != nil {
		t.Fatalf("unexpected error on Set: %v", err)
	}
	val, err := s.Get("hello")
	if err != nil {
		t.Fatalf("unexpected error on Get: %v", err)
	}
	if string(val) != "world" {
		t.Errorf("expected 'world', got '%s'", val)
	}
}

func TestGet_NotFound(t *testing.T) {
	s := NewStore()
	_, err := s.Get("missing")
	if err != ErrKeyNotFound {
		t.Errorf("expected ErrKeyNotFound, got %v", err)
	}
}

func TestSet_EmptyKey(t *testing.T) {
	s := NewStore()
	err := s.Set("", []byte("value"))
	if err != ErrEmptyKey {
		t.Errorf("expected ErrEmptyKey, got %v", err)
	}
}

func TestDelete(t *testing.T) {
	s := NewStore()
	_ = s.Set("key", []byte("val"))
	_ = s.Delete("key")
	_, err := s.Get("key")
	if err != ErrKeyNotFound {
		t.Errorf("expected ErrKeyNotFound after delete, got %v", err)
	}
}

func TestKeys(t *testing.T) {
	s := NewStore()
	_ = s.Set("a", []byte("1"))
	_ = s.Set("b", []byte("2"))
	keys := s.Keys()
	if len(keys) != 2 {
		t.Errorf("expected 2 keys, got %d", len(keys))
	}
}
