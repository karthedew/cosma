package operator

import (
	"testing"

	"github.com/karthedew/cosma/dataframe"
)

func TestLimitOperator(t *testing.T) {
	ids, err := dataframe.NewSeries("ids", []int32{1, 2, 3})
	if err != nil {
		t.Fatalf("NewSeries ids: %v", err)
	}
	vals, err := dataframe.NewSeries("vals", []string{"a", "b", "c"})
	if err != nil {
		t.Fatalf("NewSeries vals: %v", err)
	}

	df, err := dataframe.New([]*dataframe.Series{ids, vals})
	if err != nil {
		t.Fatalf("New dataframe: %v", err)
	}
	iter, err := dataframe.NewRecordBatchIter(df)
	if err != nil {
		t.Fatalf("NewRecordBatchIter: %v", err)
	}
	rec, ok, err := iter.Next()
	if err != nil || !ok {
		t.Fatalf("Next: %v", err)
	}
	defer rec.Release()

	lim, err := NewLimit(rec.Schema(), 2)
	if err != nil {
		t.Fatalf("NewLimit: %v", err)
	}
	out, err := lim.Process(rec)
	if err != nil {
		t.Fatalf("Process: %v", err)
	}
	if out.NumRows() != 2 {
		out.Release()
		t.Fatalf("expected 2 rows, got %d", out.NumRows())
	}
	out.Release()

	out, err = lim.Process(rec)
	if err != nil {
		t.Fatalf("Process: %v", err)
	}
	if out != nil {
		out.Release()
		t.Fatalf("expected nil record after limit reached")
	}
}
