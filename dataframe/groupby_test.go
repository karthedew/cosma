package dataframe

import (
	"context"
	"math"
	"reflect"
	"testing"

	"github.com/karthedew/cosma/expr"
)

func TestGroupByAgg(t *testing.T) {
	// keys a,b,a,b paired with values 1,2,3,4 (multi-chunk values).
	k, err := NewSeries("k", []string{"a", "b", "a", "b"})
	if err != nil {
		t.Fatalf("NewSeries k: %v", err)
	}
	v := multiChunkInt64(t, "v", [][]int64{{1, 2}, {3, 4}})
	df, err := New([]*Series{k, v})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	got, err := df.GroupBy("k").Agg(context.Background(), expr.Col("v").Sum(), expr.Col("v").Count().As("n"), expr.Col("v").Mean())
	if err != nil {
		t.Fatalf("Agg: %v", err)
	}

	// Columns: key first, then aggregations in argument order.
	if cols := got.Columns(); !reflect.DeepEqual(cols, []string{"k", "v_sum", "n", "v_mean"}) {
		t.Fatalf("Columns = %v, want [k v_sum n v_mean]", cols)
	}
	if got.NumRows() != 2 {
		t.Fatalf("NumRows = %d, want 2", got.NumRows())
	}

	// Group order follows first appearance: a, then b.
	keys := stringValues(t, got, "k")
	if !reflect.DeepEqual(keys, []string{"a", "b"}) {
		t.Fatalf("keys = %v, want [a b]", keys)
	}
	// a: 1+3=4, b: 2+4=6
	if sums := int64Values(t, got, "v_sum"); !reflect.DeepEqual(sums, []int64{4, 6}) {
		t.Fatalf("v_sum = %v, want [4 6]", sums)
	}
	if ns := int64Values(t, got, "n"); !reflect.DeepEqual(ns, []int64{2, 2}) {
		t.Fatalf("n = %v, want [2 2]", ns)
	}
	means := float64Values(t, got, "v_mean")
	if math.Abs(means[0]-2.0) > 1e-9 || math.Abs(means[1]-3.0) > 1e-9 {
		t.Fatalf("v_mean = %v, want [2 3]", means)
	}
}

func TestGroupByErrors(t *testing.T) {
	v, err := NewSeries("v", []int64{1, 2, 3})
	if err != nil {
		t.Fatalf("NewSeries: %v", err)
	}
	df, err := New([]*Series{v})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if _, err := df.GroupBy("nope").Agg(context.Background(), expr.Col("v").Sum()); err == nil {
		t.Fatalf("expected error for unknown key column")
	}
	if _, err := df.GroupBy("v").Agg(context.Background(), expr.Col("nope").Sum()); err == nil {
		t.Fatalf("expected error for unknown agg column")
	}
}
