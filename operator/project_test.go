package operator

import (
	"testing"

	"github.com/karthedew/cosma/dataframe"
)

func TestProjectOperator(t *testing.T) {
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

	proj, err := NewProject(rec.Schema(), []int{1})
	if err != nil {
		t.Fatalf("NewProject: %v", err)
	}

	out, err := proj.Process(rec)
	if err != nil {
		t.Fatalf("Process: %v", err)
	}
	if out.NumCols() != 1 {
		out.Release()
		t.Fatalf("expected 1 column, got %d", out.NumCols())
	}
	if out.ColumnName(0) != "vals" {
		out.Release()
		t.Fatalf("expected column vals, got %q", out.ColumnName(0))
	}
	out.Release()
}
