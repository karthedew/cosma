package dataframe

import (
	"context"
	"reflect"
	"testing"

	"github.com/karthedew/cosma/expr"
)

func TestFilter(t *testing.T) {
	// Multi-chunk frame so Filter must evaluate the predicate and gather
	// surviving rows across chunk boundaries, and apply the same mask to
	// every column.
	a := multiChunkInt64(t, "a", [][]int64{{1, 2}, {3, 4, 5}})
	b := multiChunkInt64(t, "b", [][]int64{{10, 20}, {30, 40, 50}})
	df, err := New([]*Series{a, b})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	got, err := df.Filter(context.Background(), expr.Col("a").Gt(expr.Lit(int64(2))))
	if err != nil {
		t.Fatalf("Filter: %v", err)
	}
	if got.NumRows() != 3 {
		t.Fatalf("Filter NumRows = %d, want 3", got.NumRows())
	}
	if vals := int64Values(t, got, "a"); !reflect.DeepEqual(vals, []int64{3, 4, 5}) {
		t.Fatalf("a values = %v, want [3 4 5]", vals)
	}
	if vals := int64Values(t, got, "b"); !reflect.DeepEqual(vals, []int64{30, 40, 50}) {
		t.Fatalf("b values = %v, want [30 40 50]", vals)
	}

	// Original unchanged.
	if df.NumRows() != 5 {
		t.Fatalf("original NumRows = %d, want 5", df.NumRows())
	}
}

func TestFilterEmptyResult(t *testing.T) {
	a := multiChunkInt64(t, "a", [][]int64{{1, 2}, {3}})
	df, err := New([]*Series{a})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	got, err := df.Filter(context.Background(), expr.Col("a").Gt(expr.Lit(int64(100))))
	if err != nil {
		t.Fatalf("Filter: %v", err)
	}
	if got.NumRows() != 0 {
		t.Fatalf("Filter NumRows = %d, want 0", got.NumRows())
	}
	if got.NumCols() != 1 {
		t.Fatalf("Filter NumCols = %d, want 1", got.NumCols())
	}
}

func TestFilterPredicateType(t *testing.T) {
	a := multiChunkInt64(t, "a", [][]int64{{1, 2, 3}})
	df, err := New([]*Series{a})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	// A non-boolean predicate (bare column) must be rejected.
	if _, err := df.Filter(context.Background(), expr.Col("a")); err == nil {
		t.Fatalf("expected error for non-boolean predicate")
	}
}
