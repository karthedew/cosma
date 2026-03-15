package stream

import (
	"testing"

	"github.com/karthedew/cosma/dataframe"
)

func TestDataFrameRecordReader(t *testing.T) {
	ids, err := dataframe.NewSeries("ids", []int32{10, 20, 30})
	if err != nil {
		t.Fatalf("NewSeries: %v", err)
	}
	vals, err := dataframe.NewSeries("vals", []string{"a", "b", "c"})
	if err != nil {
		t.Fatalf("NewSeries: %v", err)
	}

	df, err := dataframe.New([]*dataframe.Series{ids, vals})
	if err != nil {
		t.Fatalf("New dataframe: %v", err)
	}

	reader, err := NewDataFrameRecordReader(df)
	if err != nil {
		t.Fatalf("NewDataFrameRecordReader: %v", err)
	}
	defer reader.Release()

	if reader.Schema() == nil {
		t.Fatalf("expected schema")
	}

	if !reader.Next() {
		t.Fatalf("expected Next true")
	}

	rec := reader.Record()
	if rec == nil {
		t.Fatalf("expected record")
	}
	if rec.NumRows() != 3 {
		t.Fatalf("expected 3 rows, got %d", rec.NumRows())
	}

	if reader.Next() {
		t.Fatalf("expected Next false")
	}
	if reader.Err() != nil {
		t.Fatalf("unexpected reader error: %v", reader.Err())
	}
}
