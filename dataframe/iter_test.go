package dataframe

import "testing"

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
	if err == nil {
		t.Fatalf("expected not implemented error")
	}
	if ok {
		t.Fatalf("expected ok=false")
	}
	if rec != nil {
		t.Fatalf("expected nil record")
	}
}
