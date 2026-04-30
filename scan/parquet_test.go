package scan

import (
	"context"
	"os"
	"testing"

	"github.com/apache/arrow/go/v18/arrow"

	"github.com/karthedew/cosma/dataframe"
	"github.com/karthedew/cosma/internal/exec"
	"github.com/karthedew/cosma/internal/expr"
	"github.com/karthedew/cosma/plan"
	"github.com/karthedew/cosma/schema"
)

func TestScanParquetPipeline(t *testing.T) {
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

	reader, err := ScanParquet(path, WithParquetBatchSize(2))
	if err != nil {
		t.Fatalf("ScanParquet: %v", err)
	}
	defer reader.Release()

	cosmaSchema := schema.New(
		schema.Field{Name: "ids", Type: schema.Int32, ArrowType: arrow.PrimitiveTypes.Int32},
		schema.Field{Name: "vals", Type: schema.Utf8, ArrowType: arrow.BinaryTypes.String},
	)

	root := plan.NewLimitNode(
		plan.NewProjectNode(
			plan.NewFilterNode(
				plan.NewScanNode(cosmaSchema, "parquet"),
				expr.Gt{Left: expr.ColumnNode{Name: "ids"}, Right: expr.LiteralNode{Value: 2}},
			),
			[]string{"ids"},
		),
		1,
	)

	pl := plan.NewLogicalPlan(root)
	bound, err := plan.Bind(pl)
	if err != nil {
		t.Fatalf("Bind: %v", err)
	}
	src, ops, err := exec.Compile(bound, reader)
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	pipe, err := exec.NewPipeline(context.Background(), src, ops)
	if err != nil {
		t.Fatalf("NewPipeline: %v", err)
	}
	defer pipe.Release()

	if !pipe.Next() {
		t.Fatalf("expected Next true")
	}
	rec := pipe.Record()
	if rec.NumCols() != 1 {
		t.Fatalf("expected 1 column, got %d", rec.NumCols())
	}
	if rec.ColumnName(0) != "ids" {
		t.Fatalf("expected ids column")
	}
}
