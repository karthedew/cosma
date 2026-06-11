package dataframe

import (
	"math"
	"testing"
)

func TestColumnAggregates(t *testing.T) {
	// Multi-chunk so reductions must fold across chunk boundaries.
	a := multiChunkInt64(t, "a", [][]int64{{1, 2}, {3, 4, 5}})
	df, err := New([]*Series{a})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	sum, err := df.Sum("a")
	if err != nil {
		t.Fatalf("Sum: %v", err)
	}
	if sum != int64(15) {
		t.Fatalf("Sum = %v, want 15", sum)
	}

	cnt, err := df.Count("a")
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if cnt != 5 {
		t.Fatalf("Count = %d, want 5", cnt)
	}

	mean, err := df.Mean("a")
	if err != nil {
		t.Fatalf("Mean: %v", err)
	}
	if math.Abs(mean-3.0) > 1e-9 {
		t.Fatalf("Mean = %v, want 3", mean)
	}

	min, err := df.Min("a")
	if err != nil {
		t.Fatalf("Min: %v", err)
	}
	if min != int64(1) {
		t.Fatalf("Min = %v, want 1", min)
	}

	max, err := df.Max("a")
	if err != nil {
		t.Fatalf("Max: %v", err)
	}
	if max != int64(5) {
		t.Fatalf("Max = %v, want 5", max)
	}

	// Unknown column is an error.
	if _, err := df.Sum("nope"); err == nil {
		t.Fatalf("expected error for unknown column")
	}
}
