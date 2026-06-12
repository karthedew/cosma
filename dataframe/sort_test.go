package dataframe

import (
	"context"
	"reflect"
	"testing"

	"github.com/karthedew/cosma/expr"
)

func TestSortAscending(t *testing.T) {
	// a is multi-chunk and unsorted; b rides along to prove all columns are
	// reordered by the same permutation.
	a := multiChunkInt64(t, "a", [][]int64{{3, 1}, {2}})
	b, err := NewSeries("b", []string{"x", "y", "z"})
	if err != nil {
		t.Fatalf("NewSeries b: %v", err)
	}
	df, err := New([]*Series{a, b})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	got, err := df.Sort(context.Background(), expr.By("a"))
	if err != nil {
		t.Fatalf("Sort: %v", err)
	}
	if vals := int64Values(t, got, "a"); !reflect.DeepEqual(vals, []int64{1, 2, 3}) {
		t.Fatalf("a = %v, want [1 2 3]", vals)
	}
	if vals := stringValues(t, got, "b"); !reflect.DeepEqual(vals, []string{"y", "z", "x"}) {
		t.Fatalf("b = %v, want [y z x]", vals)
	}

	// Original unchanged.
	if vals := int64Values(t, df, "a"); !reflect.DeepEqual(vals, []int64{3, 1, 2}) {
		t.Fatalf("original a = %v, want [3 1 2]", vals)
	}
}

func TestSortDescending(t *testing.T) {
	a := multiChunkInt64(t, "a", [][]int64{{3, 1}, {2}})
	b, _ := NewSeries("b", []string{"x", "y", "z"})
	df, _ := New([]*Series{a, b})

	got, err := df.Sort(context.Background(), expr.By("a").Desc())
	if err != nil {
		t.Fatalf("Sort: %v", err)
	}
	if vals := int64Values(t, got, "a"); !reflect.DeepEqual(vals, []int64{3, 2, 1}) {
		t.Fatalf("a = %v, want [3 2 1]", vals)
	}
	if vals := stringValues(t, got, "b"); !reflect.DeepEqual(vals, []string{"x", "z", "y"}) {
		t.Fatalf("b = %v, want [x z y]", vals)
	}
}

func TestSortUnknownColumn(t *testing.T) {
	a := multiChunkInt64(t, "a", [][]int64{{1, 2, 3}})
	df, _ := New([]*Series{a})
	if _, err := df.Sort(context.Background(), expr.By("nope")); err == nil {
		t.Fatalf("expected error for unknown column")
	}
}
