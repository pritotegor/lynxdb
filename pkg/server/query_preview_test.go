package server

import (
	"testing"

	"github.com/lynxbase/lynxdb/pkg/event"
)

func TestSampleRowsForPreview_Empty(t *testing.T) {
	got := sampleRowsForPreview(nil, 50)
	if got == nil {
		t.Fatal("expected non-nil slice, got nil")
	}
	if len(got) != 0 {
		t.Fatalf("expected 0 rows, got %d", len(got))
	}
}

func TestSampleRowsForPreview_FewerThanMax(t *testing.T) {
	rows := makeTestRows(3)
	got := sampleRowsForPreview(rows, 50)
	if len(got) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(got))
	}
	// Should be the last 3 (all of them), in order.
	assertRowValue(t, got[0], "msg", "row-0")
	assertRowValue(t, got[1], "msg", "row-1")
	assertRowValue(t, got[2], "msg", "row-2")
}

func TestSampleRowsForPreview_ExactlyMax(t *testing.T) {
	rows := makeTestRows(5)
	got := sampleRowsForPreview(rows, 5)
	if len(got) != 5 {
		t.Fatalf("expected 5 rows, got %d", len(got))
	}
	assertRowValue(t, got[0], "msg", "row-0")
	assertRowValue(t, got[4], "msg", "row-4")
}

func TestSampleRowsForPreview_MoreThanMax(t *testing.T) {
	rows := makeTestRows(100)
	got := sampleRowsForPreview(rows, 10)
	if len(got) != 10 {
		t.Fatalf("expected 10 rows, got %d", len(got))
	}
	// Should be the LAST 10 rows (tail sampling).
	assertRowValue(t, got[0], "msg", "row-90")
	assertRowValue(t, got[9], "msg", "row-99")
}

func TestSampleRowsForPreview_MaxZero(t *testing.T) {
	rows := makeTestRows(5)
	got := sampleRowsForPreview(rows, 0)
	if len(got) != 0 {
		t.Fatalf("expected 0 rows for maxN=0, got %d", len(got))
	}
}

func TestSampleRowsForPreview_ConvertsEventValues(t *testing.T) {
	rows := []map[string]event.Value{
		{"num": event.IntValue(42), "str": event.StringValue("hello")},
	}
	got := sampleRowsForPreview(rows, 10)
	if len(got) != 1 {
		t.Fatalf("expected 1 row, got %d", len(got))
	}
	if got[0]["num"] != int64(42) {
		t.Errorf("expected num=42, got %v", got[0]["num"])
	}
	if got[0]["str"] != "hello" {
		t.Errorf("expected str=hello, got %v", got[0]["str"])
	}
}

func TestPreviewSnapshot_AtomicPointer(t *testing.T) {
	job := newSearchJob("level=error", ResultTypeEvents)

	// Initially nil.
	if snap := job.Preview.Load(); snap != nil {
		t.Fatal("expected nil preview on new job")
	}

	// Store a snapshot.
	job.Preview.Store(&PreviewSnapshot{
		Version: 1,
		Total:   100,
		Rows:    []map[string]interface{}{{"msg": "test"}},
	})

	snap := job.Preview.Load()
	if snap == nil {
		t.Fatal("expected non-nil preview after store")
	}
	if snap.Version != 1 {
		t.Fatalf("expected version=1, got %d", snap.Version)
	}
	if snap.Total != 100 {
		t.Fatalf("expected total=100, got %d", snap.Total)
	}
	if len(snap.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(snap.Rows))
	}

	// Overwrite with newer version.
	job.Preview.Store(&PreviewSnapshot{
		Version: 2,
		Total:   200,
		Rows:    []map[string]interface{}{{"msg": "updated"}},
	})

	snap = job.Preview.Load()
	if snap.Version != 2 {
		t.Fatalf("expected version=2, got %d", snap.Version)
	}
	if snap.Total != 200 {
		t.Fatalf("expected total=200, got %d", snap.Total)
	}
}

func TestPreviewSnapshot_ConcurrentAccess(t *testing.T) {
	job := newSearchJob("level=error", ResultTypeEvents)
	done := make(chan struct{})

	// Writer goroutine.
	go func() {
		defer close(done)
		for i := int64(0); i < 1000; i++ {
			job.Preview.Store(&PreviewSnapshot{
				Version: i,
				Total:   i * 10,
				Rows:    []map[string]interface{}{{"v": i}},
			})
		}
	}()

	// Reader goroutine — must not race.
	go func() {
		for i := 0; i < 1000; i++ {
			_ = job.Preview.Load()
		}
	}()

	<-done
}

// --- helpers ---

func makeTestRows(n int) []map[string]event.Value {
	rows := make([]map[string]event.Value, n)
	for i := 0; i < n; i++ {
		rows[i] = map[string]event.Value{
			"msg": event.StringValue("row-" + itoa(i)),
		}
	}
	return rows
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	digits := make([]byte, 0, 5)
	n := i
	for n > 0 {
		digits = append(digits, byte('0'+n%10))
		n /= 10
	}
	// Reverse.
	for l, r := 0, len(digits)-1; l < r; l, r = l+1, r-1 {
		digits[l], digits[r] = digits[r], digits[l]
	}
	return string(digits)
}

func assertRowValue(t *testing.T, row map[string]interface{}, key, want string) {
	t.Helper()
	got, ok := row[key]
	if !ok {
		t.Fatalf("row missing key %q", key)
	}
	if got != want {
		t.Errorf("row[%q] = %v, want %q", key, got, want)
	}
}
