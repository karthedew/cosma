package scan

import (
	"context"
	"os"
	"testing"

	"github.com/apache/arrow/go/v18/arrow"

	"github.com/karthedew/cosma/internal/exec"
	"github.com/karthedew/cosma/internal/expr"
	"github.com/karthedew/cosma/plan"
	"github.com/karthedew/cosma/schema"
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

	reader, err := ScanCSV(f.Name(), WithCSVChunkSize(2))
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

func TestScanCSVFilterPipeline(t *testing.T) {
	f, err := os.CreateTemp("", "cosma-csv-*.csv")
	if err != nil {
		t.Fatalf("CreateTemp: %v", err)
	}
	defer os.Remove(f.Name())
	if _, err := f.WriteString("ids,vals\n1,a\n2,b\n3,c\n4,d\n"); err != nil {
		_ = f.Close()
		t.Fatalf("WriteString: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reader, err := ScanCSV(f.Name(), WithCSVChunkSize(2))
	if err != nil {
		t.Fatalf("ScanCSV: %v", err)
	}
	defer reader.Release()

	cosmaSchema := schema.New(
		schema.Field{Name: "ids", Type: schema.Int64, ArrowType: arrow.PrimitiveTypes.Int64},
		schema.Field{Name: "vals", Type: schema.Utf8, ArrowType: arrow.BinaryTypes.String},
	)

	root := plan.NewLimitNode(
		plan.NewProjectNode(
			plan.NewFilterNode(
				plan.NewScanNode(cosmaSchema, "csv"),
				expr.Gt{Left: expr.ColumnNode{Name: "ids"}, Right: expr.LiteralNode{Value: 2}},
			),
			[]string{"vals"},
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
	if rec.ColumnName(0) != "vals" {
		t.Fatalf("expected vals column")
	}
}
