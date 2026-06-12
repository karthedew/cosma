package scan

import (
	"context"
	"os"
	"testing"

	"github.com/apache/arrow/go/v18/arrow"
)

func TestScanCSVBatches(t *testing.T) {
	f, err := os.CreateTemp("", "cosma-csv-*.csv")
	if err != nil {
		t.Fatalf("CreateTemp: %v", err)
	}
	defer os.Remove(f.Name())
	if _, err := f.WriteString("ids,vals\n1,a\n2,b\n3,c\n"); err != nil {
		_ = f.Close()
		t.Fatalf("WriteString: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reader, err := ScanCSV(context.Background(), f.Name(), WithCSVChunkSize(2))
	if err != nil {
		t.Fatalf("ScanCSV: %v", err)
	}
	defer reader.Release()

	rows := int64(0)
	for reader.Next() {
		rec := reader.Record()
		if rec == nil {
			t.Fatalf("expected record")
		}
		if rows == 0 {
			schema := reader.Schema()
			if schema == nil {
				t.Fatalf("expected schema")
			}
			if schema.NumFields() != 2 {
				t.Fatalf("expected 2 fields")
			}
			if schema.Field(0).Name != "ids" {
				t.Fatalf("expected ids field")
			}
		}
		rows += rec.NumRows()
		arr := rec.Column(0)
		if arr.DataType().ID() != arrow.INT64 {
			t.Fatalf("expected int64 column, got %s", arr.DataType())
		}
	}
	if err := reader.Err(); err != nil {
		t.Fatalf("reader error: %v", err)
	}
	if rows != 3 {
		t.Fatalf("expected 3 rows, got %d", rows)
	}
}
