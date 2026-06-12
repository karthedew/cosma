package ingest

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
)

func writeTempCSV(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "data.csv")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("write csv: %v", err)
	}
	return path
}

func TestCSVReaderYieldsChunkedBatches(t *testing.T) {
	path := writeTempCSV(t, "id,name\n1,a\n2,b\n3,c\n")

	reader, err := CSV(path, CSVConfig{HasHeader: true, ChunkSize: 2})
	if err != nil {
		t.Fatalf("CSV: %v", err)
	}
	defer reader.Release()

	var rowsPerBatch []int64
	var ids []int64
	var firstField string
	for reader.Next() {
		rec := reader.Record()
		if firstField == "" {
			firstField = rec.Schema().Field(0).Name
		}
		rowsPerBatch = append(rowsPerBatch, rec.NumRows())
		idCol := rec.Column(0).(*array.Int64)
		for i := 0; i < idCol.Len(); i++ {
			ids = append(ids, idCol.Value(i))
		}
	}
	if err := reader.Err(); err != nil {
		t.Fatalf("reader err: %v", err)
	}

	if firstField != "id" {
		t.Fatalf("schema field 0 = %q, want id", firstField)
	}
	if len(rowsPerBatch) != 2 || rowsPerBatch[0] != 2 || rowsPerBatch[1] != 1 {
		t.Fatalf("rowsPerBatch = %v, want [2 1] (chunk size 2 over 3 rows)", rowsPerBatch)
	}
	if len(ids) != 3 || ids[0] != 1 || ids[2] != 3 {
		t.Fatalf("ids = %v, want [1 2 3]", ids)
	}
}

func TestCSVEmptyPathError(t *testing.T) {
	if _, err := CSV("", CSVConfig{}); err == nil {
		t.Fatalf("expected error for empty path")
	}
}
