package dataframe

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"
	"github.com/apache/arrow/go/v18/arrow/memory"
)

func TestCSVReadWriteRoundTrip(t *testing.T) {
	df := testDataFrame(t)

	path := filepath.Join(t.TempDir(), "data.csv")
	if err := WriteCSV(df, path); err != nil {
		t.Fatalf("WriteCSV: %v", err)
	}

	out, err := ReadCSV(path)
	if err != nil {
		t.Fatalf("ReadCSV: %v", err)
	}

	assertDataFrameEqual(t, df, out)
}

func TestParquetReadWriteRoundTrip(t *testing.T) {
	df := testDataFrame(t)

	path := filepath.Join(t.TempDir(), "data.parquet")
	if err := WriteParquet(df, path); err != nil {
		t.Fatalf("WriteParquet: %v", err)
	}

	out, err := ReadParquet(path)
	if err != nil {
		t.Fatalf("ReadParquet: %v", err)
	}

	assertDataFrameEqual(t, df, out)
}

func TestCSVReadMissingValues(t *testing.T) {
	path := filepath.Join(t.TempDir(), "missing.csv")
	content := []byte("ids,names\n1,alpha\n2,\n3,gamma\n")
	if err := os.WriteFile(path, content, 0600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	df, err := ReadCSV(path)
	if err != nil {
		t.Fatalf("ReadCSV: %v", err)
	}

	col, ok := df.Column("names")
	if !ok {
		t.Fatalf("expected names column")
	}

	arr := mustConcatChunked(t, "names", col.Chunked())
	defer arr.Release()
	if !arr.IsNull(1) {
		t.Fatalf("expected null at row 1")
	}
}

func TestParquetReadWriteNulls(t *testing.T) {
	ids, err := NewSeries("ids", []int64{1, 2, 3})
	if err != nil {
		t.Fatalf("ids series: %v", err)
	}
	nameBuilder := array.NewStringBuilder(memory.DefaultAllocator)
	nameBuilder.Append("alpha")
	nameBuilder.AppendNull()
	nameBuilder.Append("gamma")
	nameArr := nameBuilder.NewArray()
	nameBuilder.Release()
	defer nameArr.Release()

	names, err := NewSeriesFromArray("names", nameArr)
	if err != nil {
		t.Fatalf("names series: %v", err)
	}

	df, err := New([]*Series{ids, names})
	if err != nil {
		t.Fatalf("dataframe: %v", err)
	}

	path := filepath.Join(t.TempDir(), "nulls.parquet")
	if err := WriteParquet(df, path); err != nil {
		t.Fatalf("WriteParquet: %v", err)
	}

	out, err := ReadParquet(path)
	if err != nil {
		t.Fatalf("ReadParquet: %v", err)
	}

	col, ok := out.Column("names")
	if !ok {
		t.Fatalf("expected names column")
	}

	arr := mustConcatChunked(t, "names", col.Chunked())
	defer arr.Release()
	if !arr.IsNull(1) {
		t.Fatalf("expected null at row 1")
	}
}

func testDataFrame(t *testing.T) *DataFrame {
	t.Helper()

	ids, err := NewSeries("ids", []int64{1, 2, 3})
	if err != nil {
		t.Fatalf("ids series: %v", err)
	}
	names, err := NewSeries("names", []string{"alpha", "beta", "gamma"})
	if err != nil {
		t.Fatalf("names series: %v", err)
	}
	df, err := New([]*Series{ids, names})
	if err != nil {
		t.Fatalf("dataframe: %v", err)
	}
	return df
}

func assertDataFrameEqual(t *testing.T, left, right *DataFrame) {
	t.Helper()

	if left == nil || right == nil {
		t.Fatalf("expected dataframes, got left=%v right=%v", left, right)
	}

	leftSchema := left.Schema().Fields()
	rightSchema := right.Schema().Fields()
	if len(leftSchema) != len(rightSchema) {
		t.Fatalf("schema len mismatch: %d != %d", len(leftSchema), len(rightSchema))
	}

	for i := range leftSchema {
		if leftSchema[i].Name != rightSchema[i].Name {
			t.Fatalf("schema name mismatch at %d: %q != %q", i, leftSchema[i].Name, rightSchema[i].Name)
		}
		if leftSchema[i].Type != rightSchema[i].Type {
			t.Fatalf("schema type mismatch at %d: %q != %q", i, leftSchema[i].Type, rightSchema[i].Type)
		}
		if !arrow.TypeEqual(leftSchema[i].ArrowType, rightSchema[i].ArrowType) {
			t.Fatalf("schema arrow type mismatch at %d", i)
		}
	}

	if left.Height() != right.Height() {
		t.Fatalf("height mismatch: %d != %d", left.Height(), right.Height())
	}

	for _, field := range leftSchema {
		leftCol, ok := left.Column(field.Name)
		if !ok {
			t.Fatalf("missing left column %q", field.Name)
		}
		rightCol, ok := right.Column(field.Name)
		if !ok {
			t.Fatalf("missing right column %q", field.Name)
		}
		assertChunkedEqual(t, field.Name, leftCol.Chunked(), rightCol.Chunked())
	}
}

func assertChunkedEqual(t *testing.T, name string, left, right *arrow.Chunked) {
	t.Helper()

	if left == nil || right == nil {
		if left != right {
			t.Fatalf("column %q chunked nil mismatch", name)
		}
		return
	}
	if !arrow.TypeEqual(left.DataType(), right.DataType()) {
		t.Fatalf("column %q dtype mismatch", name)
	}

	leftArr := mustConcatChunked(t, name, left)
	defer leftArr.Release()

	rightArr := mustConcatChunked(t, name, right)
	defer rightArr.Release()

	if !array.Equal(leftArr, rightArr) {
		t.Fatalf("column %q data mismatch", name)
	}
}

func mustConcatChunked(t *testing.T, name string, chunked *arrow.Chunked) arrow.Array {
	t.Helper()

	if chunked == nil {
		t.Fatalf("column %q chunked is nil", name)
	}
	chunks := chunked.Chunks()
	if len(chunks) == 0 {
		arr := array.MakeArrayOfNull(memory.DefaultAllocator, chunked.DataType(), 0)
		return arr
	}
	arr, err := array.Concatenate(chunks, memory.DefaultAllocator)
	if err != nil {
		t.Fatalf("column %q concat: %v", name, err)
	}
	return arr
}
