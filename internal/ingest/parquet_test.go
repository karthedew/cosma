package ingest

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"
	"github.com/apache/arrow/go/v18/arrow/memory"
	"github.com/apache/arrow/go/v18/parquet/pqarrow"
)

func writeTempParquet(t *testing.T, ids []int64) string {
	t.Helper()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
	}, nil)

	b := array.NewInt64Builder(memory.DefaultAllocator)
	defer b.Release()
	b.AppendValues(ids, nil)
	arr := b.NewArray()
	defer arr.Release()

	col := arrow.NewColumnFromArr(schema.Field(0), arr)
	defer col.Release()
	table := array.NewTable(schema, []arrow.Column{col}, int64(len(ids)))
	defer table.Release()

	path := filepath.Join(t.TempDir(), "data.parquet")
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create parquet: %v", err)
	}
	defer f.Close()
	if err := pqarrow.WriteTable(table, f, 1024, nil, pqarrow.DefaultWriterProps()); err != nil {
		t.Fatalf("write parquet: %v", err)
	}
	return path
}

func TestParquetReaderYieldsRows(t *testing.T) {
	path := writeTempParquet(t, []int64{10, 20, 30})

	reader, err := Parquet(path, ParquetConfig{})
	if err != nil {
		t.Fatalf("Parquet: %v", err)
	}
	defer reader.Release()

	var ids []int64
	for reader.Next() {
		rec := reader.Record()
		idCol := rec.Column(0).(*array.Int64)
		for i := 0; i < idCol.Len(); i++ {
			ids = append(ids, idCol.Value(i))
		}
	}
	if err := reader.Err(); err != nil {
		t.Fatalf("reader err: %v", err)
	}

	if len(ids) != 3 || ids[0] != 10 || ids[2] != 30 {
		t.Fatalf("ids = %v, want [10 20 30]", ids)
	}
}

func TestParquetEmptyPathError(t *testing.T) {
	if _, err := Parquet("", ParquetConfig{}); err == nil {
		t.Fatalf("expected error for empty path")
	}
}
