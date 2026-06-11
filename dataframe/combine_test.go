package dataframe

import (
	"reflect"
	"testing"
)

func TestConcat(t *testing.T) {
	a := multiChunkInt64(t, "x", [][]int64{{1, 2}})
	b := multiChunkInt64(t, "x", [][]int64{{3, 4, 5}})
	dfa, err := New([]*Series{a})
	if err != nil {
		t.Fatalf("New a: %v", err)
	}
	dfb, err := New([]*Series{b})
	if err != nil {
		t.Fatalf("New b: %v", err)
	}

	got, err := Concat(dfa, dfb)
	if err != nil {
		t.Fatalf("Concat: %v", err)
	}
	if got.NumRows() != 5 {
		t.Fatalf("NumRows = %d, want 5", got.NumRows())
	}
	if vals := int64Values(t, got, "x"); !reflect.DeepEqual(vals, []int64{1, 2, 3, 4, 5}) {
		t.Fatalf("x = %v, want [1 2 3 4 5]", vals)
	}
	// Vertical concat preserves each input's chunks: 1 + 1 = 2 chunks.
	if got.NumChunks() != 2 {
		t.Fatalf("NumChunks = %d, want 2", got.NumChunks())
	}
	// Inputs unchanged.
	if dfa.NumRows() != 2 || dfb.NumRows() != 3 {
		t.Fatalf("inputs mutated: a=%d b=%d", dfa.NumRows(), dfb.NumRows())
	}
}

func TestConcatSchemaMismatch(t *testing.T) {
	a, _ := NewSeries("x", []int64{1, 2})
	b, _ := NewSeries("y", []int64{3, 4})
	dfa, _ := New([]*Series{a})
	dfb, _ := New([]*Series{b})

	if _, err := Concat(dfa, dfb); err == nil {
		t.Fatalf("expected error for mismatched column names")
	}

	c, _ := NewSeries("x", []string{"p", "q"})
	dfc, _ := New([]*Series{c})
	if _, err := Concat(dfa, dfc); err == nil {
		t.Fatalf("expected error for mismatched column types")
	}
}

func TestHStack(t *testing.T) {
	x, _ := NewSeries("x", []int64{1, 2, 3})
	y, _ := NewSeries("y", []int64{10, 20, 30})
	dfx, _ := New([]*Series{x})
	dfy, _ := New([]*Series{y})

	got, err := dfx.HStack(dfy)
	if err != nil {
		t.Fatalf("HStack: %v", err)
	}
	if cols := got.Columns(); !reflect.DeepEqual(cols, []string{"x", "y"}) {
		t.Fatalf("Columns = %v, want [x y]", cols)
	}
	if got.NumRows() != 3 {
		t.Fatalf("NumRows = %d, want 3", got.NumRows())
	}
	if vals := int64Values(t, got, "y"); !reflect.DeepEqual(vals, []int64{10, 20, 30}) {
		t.Fatalf("y = %v, want [10 20 30]", vals)
	}
}

func TestHStackErrors(t *testing.T) {
	x, _ := NewSeries("x", []int64{1, 2, 3})
	dup, _ := NewSeries("x", []int64{4, 5, 6})
	short, _ := NewSeries("z", []int64{7, 8})
	dfx, _ := New([]*Series{x})
	dfdup, _ := New([]*Series{dup})
	dfshort, _ := New([]*Series{short})

	if _, err := dfx.HStack(dfdup); err == nil {
		t.Fatalf("expected error for duplicate column name")
	}
	if _, err := dfx.HStack(dfshort); err == nil {
		t.Fatalf("expected error for height mismatch")
	}
}
