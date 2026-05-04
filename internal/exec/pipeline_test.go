package exec

import (
	"context"
	"testing"

	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"
	"github.com/apache/arrow/go/v18/arrow/memory"

	"github.com/karthedew/cosma/dataframe"
	"github.com/karthedew/cosma/internal/stream"
	"github.com/karthedew/cosma/operator"
)

func TestPipelineFilterProjectLimit(t *testing.T) {
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

	reader, err := stream.NewDataFrameRecordReader(df)
	if err != nil {
		t.Fatalf("NewDataFrameRecordReader: %v", err)
	}
	defer reader.Release()

	filter, err := operator.NewFilter(context.Background(), reader.Schema(), func(rec arrow.Record) (arrow.Array, error) {
		col := rec.Column(0).(*array.Int32)
		builder := array.NewBooleanBuilder(memory.DefaultAllocator)
		defer builder.Release()
		for i := 0; i < col.Len(); i++ {
			builder.Append(col.Value(i) > 2)
		}
		return builder.NewArray(), nil
	}, nil)
	if err != nil {
		t.Fatalf("NewFilter: %v", err)
	}

	project, err := operator.NewProject(reader.Schema(), []int{1})
	if err != nil {
		t.Fatalf("NewProject: %v", err)
	}

	limit, err := operator.NewLimit(project.Schema(), 1)
	if err != nil {
		t.Fatalf("NewLimit: %v", err)
	}

	pipe, err := NewPipeline(context.Background(), reader, []operator.Operator{filter, project, limit})
	if err != nil {
		t.Fatalf("NewPipeline: %v", err)
	}
	defer pipe.Release()

	if !pipe.Next() {
		t.Fatalf("expected Next true")
	}
	rec := pipe.Record()
	if rec == nil {
		t.Fatalf("expected record")
	}
	if rec.NumRows() != 1 {
		t.Fatalf("expected 1 row, got %d", rec.NumRows())
	}
	if rec.NumCols() != 1 {
		t.Fatalf("expected 1 column, got %d", rec.NumCols())
	}
	if rec.ColumnName(0) != "vals" {
		t.Fatalf("expected vals column, got %q", rec.ColumnName(0))
	}

	if pipe.Next() {
		t.Fatalf("expected Next false")
	}
	if pipe.Err() != nil {
		t.Fatalf("unexpected error: %v", pipe.Err())
	}
}
