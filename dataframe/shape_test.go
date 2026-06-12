package dataframe

import (
	"reflect"
	"testing"

	"github.com/apache/arrow/go/v18/arrow/array"
)

func int64Values(t *testing.T, df *DataFrame, name string) []int64 {
	t.Helper()
	col, ok := df.Column(name)
	if !ok {
		t.Fatalf("column %q not found", name)
	}
	var out []int64
	for _, chunk := range col.Chunked().Chunks() {
		arr, ok := chunk.(*array.Int64)
		if !ok {
			t.Fatalf("column %q chunk is %T, want *array.Int64", name, chunk)
		}
		for i := 0; i < arr.Len(); i++ {
			out = append(out, arr.Value(i))
		}
	}
	return out
}

func float64Values(t *testing.T, df *DataFrame, name string) []float64 {
	t.Helper()
	col, ok := df.Column(name)
	if !ok {
		t.Fatalf("column %q not found", name)
	}
	var out []float64
	for _, chunk := range col.Chunked().Chunks() {
		arr, ok := chunk.(*array.Float64)
		if !ok {
			t.Fatalf("column %q chunk is %T, want *array.Float64", name, chunk)
		}
		for i := 0; i < arr.Len(); i++ {
			out = append(out, arr.Value(i))
		}
	}
	return out
}

func stringValues(t *testing.T, df *DataFrame, name string) []string {
	t.Helper()
	col, ok := df.Column(name)
	if !ok {
		t.Fatalf("column %q not found", name)
	}
	var out []string
	for _, chunk := range col.Chunked().Chunks() {
		arr, ok := chunk.(*array.String)
		if !ok {
			t.Fatalf("column %q chunk is %T, want *array.String", name, chunk)
		}
		for i := 0; i < arr.Len(); i++ {
			out = append(out, arr.Value(i))
		}
	}
	return out
}

func threeRowDF(t *testing.T) *DataFrame {
	t.Helper()
	ids, err := NewSeries("ids", []int64{1, 2, 3})
	if err != nil {
		t.Fatalf("NewSeries ids: %v", err)
	}
	names, err := NewSeries("names", []string{"a", "b", "c"})
	if err != nil {
		t.Fatalf("NewSeries names: %v", err)
	}
	df, err := New([]*Series{ids, names})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return df
}

func TestDataFrameShape(t *testing.T) {
	df := threeRowDF(t)

	if got := df.NumRows(); got != 3 {
		t.Fatalf("NumRows = %d, want 3", got)
	}
	if got := df.NumCols(); got != 2 {
		t.Fatalf("NumCols = %d, want 2", got)
	}
	if got := df.Columns(); !reflect.DeepEqual(got, []string{"ids", "names"}) {
		t.Fatalf("Columns = %v, want [ids names]", got)
	}
}

func TestDataFrameHead(t *testing.T) {
	df := threeRowDF(t)

	head := df.Head(2)
	if got := head.NumRows(); got != 2 {
		t.Fatalf("Head(2).NumRows = %d, want 2", got)
	}
	if got := head.Columns(); !reflect.DeepEqual(got, []string{"ids", "names"}) {
		t.Fatalf("Head(2).Columns = %v, want [ids names]", got)
	}
	if got := int64Values(t, head, "ids"); !reflect.DeepEqual(got, []int64{1, 2}) {
		t.Fatalf("Head(2) ids = %v, want [1 2]", got)
	}

	// n greater than NumRows returns all rows.
	if got := df.Head(10).NumRows(); got != 3 {
		t.Fatalf("Head(10).NumRows = %d, want 3", got)
	}

	// n <= 0 returns an empty DataFrame that keeps the schema.
	empty := df.Head(0)
	if got := empty.NumRows(); got != 0 {
		t.Fatalf("Head(0).NumRows = %d, want 0", got)
	}
	if got := empty.NumCols(); got != 2 {
		t.Fatalf("Head(0).NumCols = %d, want 2", got)
	}

	// Original DataFrame is unchanged.
	if got := df.NumRows(); got != 3 {
		t.Fatalf("original NumRows = %d, want 3", got)
	}
}
