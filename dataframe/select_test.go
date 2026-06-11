package dataframe

import (
	"reflect"
	"testing"
)

func threeColDF(t *testing.T) *DataFrame {
	t.Helper()
	a, err := NewSeries("a", []int64{1, 2, 3})
	if err != nil {
		t.Fatalf("NewSeries a: %v", err)
	}
	b, err := NewSeries("b", []int64{10, 20, 30})
	if err != nil {
		t.Fatalf("NewSeries b: %v", err)
	}
	c, err := NewSeries("c", []string{"x", "y", "z"})
	if err != nil {
		t.Fatalf("NewSeries c: %v", err)
	}
	df, err := New([]*Series{a, b, c})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return df
}

func TestSelectReordersAndSubsets(t *testing.T) {
	df := threeColDF(t)

	got, err := df.Select("c", "a")
	if err != nil {
		t.Fatalf("Select: %v", err)
	}
	if cols := got.Columns(); !reflect.DeepEqual(cols, []string{"c", "a"}) {
		t.Fatalf("Columns = %v, want [c a]", cols)
	}
	if got.NumRows() != 3 {
		t.Fatalf("NumRows = %d, want 3", got.NumRows())
	}
	if vals := int64Values(t, got, "a"); !reflect.DeepEqual(vals, []int64{1, 2, 3}) {
		t.Fatalf("a values = %v, want [1 2 3]", vals)
	}

	// Original is unchanged.
	if cols := df.Columns(); !reflect.DeepEqual(cols, []string{"a", "b", "c"}) {
		t.Fatalf("original Columns = %v, want [a b c]", cols)
	}
}

func TestDrop(t *testing.T) {
	df := threeColDF(t)

	got, err := df.Drop("b")
	if err != nil {
		t.Fatalf("Drop: %v", err)
	}
	if cols := got.Columns(); !reflect.DeepEqual(cols, []string{"a", "c"}) {
		t.Fatalf("Columns = %v, want [a c]", cols)
	}

	if _, err := df.Drop("nope"); err == nil {
		t.Fatalf("expected error dropping unknown column")
	}

	// Original unchanged.
	if df.NumCols() != 3 {
		t.Fatalf("original NumCols = %d, want 3", df.NumCols())
	}
}

func TestRename(t *testing.T) {
	df := threeColDF(t)

	got, err := df.Rename("b", "B")
	if err != nil {
		t.Fatalf("Rename: %v", err)
	}
	if cols := got.Columns(); !reflect.DeepEqual(cols, []string{"a", "B", "c"}) {
		t.Fatalf("Columns = %v, want [a B c]", cols)
	}
	col, ok := got.Column("B")
	if !ok {
		t.Fatalf("renamed column B not found")
	}
	if col.Name() != "B" {
		t.Fatalf("Series name = %q, want B", col.Name())
	}
	if f := got.Schema().Fields()[1]; f.Name != "B" {
		t.Fatalf("schema field 1 name = %q, want B", f.Name)
	}

	if _, err := df.Rename("nope", "x"); err == nil {
		t.Fatalf("expected error renaming unknown column")
	}
	if _, err := df.Rename("b", "a"); err == nil {
		t.Fatalf("expected error renaming to existing column")
	}

	// Original unchanged.
	if cols := df.Columns(); !reflect.DeepEqual(cols, []string{"a", "b", "c"}) {
		t.Fatalf("original Columns = %v, want [a b c]", cols)
	}
}

func TestSelectErrors(t *testing.T) {
	df := threeColDF(t)
	if _, err := df.Select("a", "nope"); err == nil {
		t.Fatalf("expected error for unknown column")
	}
	if _, err := df.Select("a", "a"); err == nil {
		t.Fatalf("expected error for duplicate column")
	}
}
