package dataframe

import (
	"reflect"
	"testing"

	"github.com/karthedew/cosma/expr"
)

func TestWithColumnAdds(t *testing.T) {
	// Multi-chunk so the derived column must be evaluated per canonical
	// segment and reassembled to the frame's height.
	a := multiChunkInt64(t, "a", [][]int64{{1, 2}, {3, 4, 5}})
	df, err := New([]*Series{a})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	got, err := df.WithColumn(expr.Col("a").Add(expr.Lit(int64(10))).As("b"))
	if err != nil {
		t.Fatalf("WithColumn: %v", err)
	}
	if cols := got.Columns(); !reflect.DeepEqual(cols, []string{"a", "b"}) {
		t.Fatalf("Columns = %v, want [a b]", cols)
	}
	if vals := int64Values(t, got, "b"); !reflect.DeepEqual(vals, []int64{11, 12, 13, 14, 15}) {
		t.Fatalf("b values = %v, want [11 12 13 14 15]", vals)
	}
	// Original a is preserved and unchanged.
	if vals := int64Values(t, got, "a"); !reflect.DeepEqual(vals, []int64{1, 2, 3, 4, 5}) {
		t.Fatalf("a values = %v, want [1 2 3 4 5]", vals)
	}
	if df.NumCols() != 1 {
		t.Fatalf("original NumCols = %d, want 1", df.NumCols())
	}
}

func TestWithColumnReplaces(t *testing.T) {
	a := multiChunkInt64(t, "a", [][]int64{{1, 2, 3}})
	df, err := New([]*Series{a})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	// Replace a with a*a, keeping position.
	got, err := df.WithColumn(expr.Col("a").Mul(expr.Col("a")))
	if err != nil {
		t.Fatalf("WithColumn: %v", err)
	}
	if got.NumCols() != 1 {
		t.Fatalf("NumCols = %d, want 1", got.NumCols())
	}
	if vals := int64Values(t, got, "a"); !reflect.DeepEqual(vals, []int64{1, 4, 9}) {
		t.Fatalf("a values = %v, want [1 4 9]", vals)
	}
}
