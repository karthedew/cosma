package dataframe

import (
	"reflect"
	"testing"
)

func TestLimit(t *testing.T) {
	// Multi-chunk frame so Limit must respect chunk boundaries.
	a := multiChunkInt64(t, "a", [][]int64{{1, 2}, {3, 4, 5}})
	df, err := New([]*Series{a})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	got := df.Limit(3)
	if got.NumRows() != 3 {
		t.Fatalf("Limit(3).NumRows = %d, want 3", got.NumRows())
	}
	if vals := int64Values(t, got, "a"); !reflect.DeepEqual(vals, []int64{1, 2, 3}) {
		t.Fatalf("Limit(3) values = %v, want [1 2 3]", vals)
	}

	if df.Limit(100).NumRows() != 5 {
		t.Fatalf("Limit past end should return all 5 rows")
	}
	if df.Limit(0).NumRows() != 0 {
		t.Fatalf("Limit(0) should be empty")
	}

	// Original unchanged.
	if df.NumRows() != 5 {
		t.Fatalf("original NumRows = %d, want 5", df.NumRows())
	}
}
