package scan

import (
	"context"
	"os"
	"testing"

	"github.com/apache/arrow/go/v18/arrow"

	"github.com/karthedew/cosma/dataframe"
)

func TestScanParquetBatches(t *testing.T) {
	ids, err := dataframe.NewSeries("ids", []int32{1, 2, 3, 4})
	if err != nil {
		t.Fatalf("NewSeries ids: %v", err)
	}
	vals, err := dataframe.NewSeries("vals", []string{"a", "b", "c", "d"})
	if err != nil {
		t.Fatalf("NewSeries vals: %v", err)
	}
	df, err := dataframe.New([]*dataframe.Series{ids, vals})
	if err != nil {
		t.Fatalf("New dataframe: %v", err)
	}

	f, err := os.CreateTemp("", "cosma-parquet-*.parquet")
	if err != nil {
		t.Fatalf("CreateTemp: %v", err)
	}
	path := f.Name()
	if err := f.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	defer os.Remove(path)

	if err := dataframe.WriteParquet(df, path); err != nil {
		t.Fatalf("WriteParquet: %v", err)
	}

	reader, err := ScanParquet(context.Background(), path, WithParquetBatchSize(2))
	if err != nil {
		t.Fatalf("ScanParquet: %v", err)
	}
	defer reader.Release()

	rows := int64(0)
	for reader.Next() {
		rec := reader.Record()
		if rec == nil {
			t.Fatalf("expected record")
		}
		if rows == 0 {
			if reader.Schema().NumFields() != 2 {
				t.Fatalf("expected 2 fields")
			}
			if reader.Schema().Field(0).Name != "ids" {
				t.Fatalf("expected ids field")
			}
			if rec.Column(0).DataType().ID() != arrow.INT32 {
				t.Fatalf("expected int32 column, got %s", rec.Column(0).DataType())
			}
		}
		rows += rec.NumRows()
	}
	if err := reader.Err(); err != nil {
		t.Fatalf("reader error: %v", err)
	}
	if rows != 4 {
		t.Fatalf("expected 4 rows, got %d", rows)
	}
}
