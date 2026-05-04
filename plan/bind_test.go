package plan

import (
	"testing"

	"github.com/apache/arrow/go/v18/arrow"

	"github.com/karthedew/cosma/internal/expr"
	"github.com/karthedew/cosma/schema"
)

func TestBindProjectMissingColumn(t *testing.T) {
	s := schema.New(
		schema.Field{Name: "a", Type: schema.Int32, ArrowType: arrow.PrimitiveTypes.Int32},
	)
	root := NewProjectNode(NewScanNode(s, ScanSourceDataFrame), []string{"missing"})
	plan := NewLogicalPlan(root)
	if _, err := Bind(plan); err == nil {
		t.Fatalf("expected bind error")
	}
}

func TestBindFilterMissingColumn(t *testing.T) {
	s := schema.New(
		schema.Field{Name: "a", Type: schema.Int32, ArrowType: arrow.PrimitiveTypes.Int32},
	)
	root := NewFilterNode(NewScanNode(s, ScanSourceDataFrame), expr.BinaryNode{Op: expr.BinaryOpGt, Left: expr.ColumnNode{Name: "missing"}, Right: expr.LiteralNode{Value: 1}})
	plan := NewLogicalPlan(root)
	if _, err := Bind(plan); err == nil {
		t.Fatalf("expected bind error")
	}
}

func TestBindProjectSchema(t *testing.T) {
	s := schema.New(
		schema.Field{Name: "a", Type: schema.Int32, ArrowType: arrow.PrimitiveTypes.Int32},
		schema.Field{Name: "b", Type: schema.Utf8, ArrowType: arrow.BinaryTypes.String},
	)
	root := NewProjectNode(NewScanNode(s, ScanSourceDataFrame), []string{"b"})
	pl, err := Bind(NewLogicalPlan(root))
	if err != nil {
		t.Fatalf("Bind: %v", err)
	}
	proj, ok := pl.Root.(*ProjectNode)
	if !ok {
		t.Fatalf("expected ProjectNode root")
	}
	if proj.Schema().Len() != 1 {
		t.Fatalf("expected 1 field, got %d", proj.Schema().Len())
	}
}
