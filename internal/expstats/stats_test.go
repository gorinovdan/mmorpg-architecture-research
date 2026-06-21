package expstats

import "testing"

func TestPercentile(t *testing.T) {
	values := []float64{40, 10, 30, 20}
	if got := Percentile(values, 50); got != 25 {
		t.Fatalf("p50 = %v, want 25", got)
	}
	if got := Percentile(values, 95); got != 38.5 {
		t.Fatalf("p95 = %v, want 38.5", got)
	}
}

func TestDuplicateCount(t *testing.T) {
	ids := []string{"a", "b", "a", "c", "b", "b"}
	if got := DuplicateCount(ids); got != 3 {
		t.Fatalf("duplicates = %d, want 3", got)
	}
}
