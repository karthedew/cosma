package dataframe

import (
	"reflect"
	"testing"

	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"
)

func TestRecordBatchIterMisaligned(t *testing.T) {
	// a cuts at row 2, b cuts at row 1: batches must align to both.
	a := multiChunkInt64(t, "a", [][]int64{{1, 2}, {3}})
	b := multiChunkInt64(t, "b", [][]int64{{10}, {20, 30}})
	df, err := New([]*Series{a, b})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	it, err := NewRecordBatchIter(df)
	if err != nil {
		t.Fatalf("NewRecordBatchIter: %v", err)
	}

	var batchRows []int64
	var aVals, bVals []int64
	for {
		rec, ok, err := it.Next()
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		if !ok {
			break
		}
		batchRows = append(batchRows, rec.NumRows())
		aVals = append(aVals, columnInt64(t, rec, 0)...)
		bVals = append(bVals, columnInt64(t, rec, 1)...)
		rec.Release()
	}

	if !reflect.DeepEqual(batchRows, []int64{1, 1, 1}) {
		t.Fatalf("batch rows = %v, want [1 1 1]", batchRows)
	}
	if !reflect.DeepEqual(aVals, []int64{1, 2, 3}) {
		t.Fatalf("a values = %v, want [1 2 3]", aVals)
	}
	if !reflect.DeepEqual(bVals, []int64{10, 20, 30}) {
		t.Fatalf("b values = %v, want [10 20 30]", bVals)
	}
}

func columnInt64(t *testing.T, rec arrow.Record, col int) []int64 {
	t.Helper()
	arr, ok := rec.Column(col).(*array.Int64)
	if !ok {
		t.Fatalf("column %d is %T, want *array.Int64", col, rec.Column(col))
	}
	out := make([]int64, arr.Len())
	for i := range out {
		out[i] = arr.Value(i)
	}
	return out
}

func TestRecordBatchIterErrors(t *testing.T) {
	if _, err := NewRecordBatchIter(nil); err == nil {
		t.Fatalf("expected error for nil dataframe")
	}

	s1, err := NewSeries("ids", []int32{1, 2})
	if err != nil {
		t.Fatalf("NewSeries ids: %v", err)
	}
	fn, err := New([]*Series{s1})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	it, err := NewRecordBatchIter(fn)
	if err != nil {
		t.Fatalf("NewRecordBatchIter: %v", err)
	}

	rec, ok, err := it.Next()
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	if !ok {
		t.Fatalf("expected ok=true")
	}
	if rec == nil {
		t.Fatalf("expected record")
	}
	if rec.NumRows() != 2 {
		t.Fatalf("expected 2 rows, got %d", rec.NumRows())
	}
	rec.Release()

	rec, ok, err = it.Next()
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	if ok {
		t.Fatalf("expected ok=false")
	}
	if rec != nil {
		t.Fatalf("expected nil record")
	}
}
