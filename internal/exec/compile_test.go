package exec

import (
	"context"
	"errors"
	"testing"

	"github.com/apache/arrow/go/v18/arrow/array"

	"github.com/karthedew/cosma/dataframe"
	"github.com/karthedew/cosma/internal/expr"
	"github.com/karthedew/cosma/internal/stream"
	"github.com/karthedew/cosma/operator"
	"github.com/karthedew/cosma/plan"
)

func TestCompileProjectLimit(t *testing.T) {
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

	reader, err := stream.NewDataFrameRecordReader(df)
	if err != nil {
		t.Fatalf("NewDataFrameRecordReader: %v", err)
	}
	defer reader.Release()

	pl, err := df.Lazy().Select("vals").Limit(1).Plan()
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}

	bound, err := plan.Bind(pl)
	if err != nil {
		t.Fatalf("Bind: %v", err)
	}

	src, ops, err := Compile(context.Background(), bound, reader, nil)
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	if src == nil {
		t.Fatalf("expected source")
	}
	if len(ops) != 2 {
		t.Fatalf("expected 2 ops, got %d", len(ops))
	}
	if _, ok := ops[0].(*operator.Project); !ok {
		t.Fatalf("expected Project op")
	}
	if _, ok := ops[1].(*operator.Limit); !ok {
		t.Fatalf("expected Limit op")
	}

	pipe, err := NewPipeline(context.Background(), src, ops)
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
}

func TestPipelineCancelledCtxStops(t *testing.T) {
	ids, err := dataframe.NewSeries("ids", []int32{1, 2, 3, 4})
	if err != nil {
		t.Fatalf("NewSeries: %v", err)
	}
	df, err := dataframe.New([]*dataframe.Series{ids})
	if err != nil {
		t.Fatalf("New dataframe: %v", err)
	}

	pl, err := df.Lazy().
		Filter(expr.BinaryNode{Op: expr.BinaryOpGt, Left: expr.ColumnNode{Name: "ids"}, Right: expr.LiteralNode{Value: 0}}).
		Plan()
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}
	bound, err := plan.Bind(pl)
	if err != nil {
		t.Fatalf("Bind: %v", err)
	}

	reader, err := stream.NewDataFrameRecordReader(df)
	if err != nil {
		t.Fatalf("NewDataFrameRecordReader: %v", err)
	}
	defer reader.Release()

	ctx, cancel := context.WithCancel(context.Background())
	src, ops, err := Compile(ctx, bound, reader, nil)
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	pipe, err := NewPipeline(ctx, src, ops)
	if err != nil {
		t.Fatalf("NewPipeline: %v", err)
	}
	defer pipe.Release()

	cancel()
	if pipe.Next() {
		t.Fatalf("expected Next false after cancel")
	}
	if !errors.Is(pipe.Err(), context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", pipe.Err())
	}
}

func TestCompileFilterProjectLimit(t *testing.T) {
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

	pl, err := df.Lazy().
		Filter(expr.BinaryNode{Op: expr.BinaryOpGt, Left: expr.ColumnNode{Name: "ids"}, Right: expr.LiteralNode{Value: 2}}).
		Select("ids").
		Limit(1).
		Plan()
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}

	bound, err := plan.Bind(pl)
	if err != nil {
		t.Fatalf("Bind: %v", err)
	}

	reader, err := stream.NewDataFrameRecordReader(df)
	if err != nil {
		t.Fatalf("NewDataFrameRecordReader: %v", err)
	}
	defer reader.Release()

	src, ops, err := Compile(context.Background(), bound, reader, nil)
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	pipe, err := NewPipeline(context.Background(), src, ops)
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
	col := rec.Column(0).(*array.Int32)
	if col.Value(0) != 3 {
		t.Fatalf("expected ids=3, got %d", col.Value(0))
	}
}
