package plan

import (
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"

	"github.com/karthedew/cosma/schema"
)

func TestExplainLogical(t *testing.T) {
	s := schema.New(
		schema.Field{Name: "a", Type: schema.Int32, ArrowType: arrow.PrimitiveTypes.Int32},
		schema.Field{Name: "b", Type: schema.Utf8, ArrowType: arrow.BinaryTypes.String},
	)
	root := NewLimitNode(
		NewProjectNode(
			NewScanNode(s, ScanSourceDataFrame),
			[]string{"b"},
		),
		5,
	)
	pl := NewLogicalPlan(root)
	got := ExplainLogical(pl)
	want := "Limit(n=5)\n  Project(columns=[b])\n    Scan(source=dataframe)\n"
	if got != want {
		t.Fatalf("unexpected explain output:\n%s", got)
	}
}

// TestOptimizerPushdownExplain verifies that after optimizing and lowering a
// Limit(5) plan the physical Explain output contains "5" as the PushedLimit
// annotation on the scan node, confirming limit pushdown is visible in Explain.
func TestOptimizerPushdownExplain(t *testing.T) {
	s := schema.New(
		schema.Field{Name: "x", Type: schema.Int64, ArrowType: arrow.PrimitiveTypes.Int64},
	)
	logical := NewLogicalPlan(
		NewLimitNode(NewScanNode(s, ScanSourceDataFrame), 5),
	)

	bound, err := Bind(logical)
	if err != nil {
		t.Fatalf("Bind: %v", err)
	}
	optimized, err := Optimize(bound)
	if err != nil {
		t.Fatalf("Optimize: %v", err)
	}
	physical, err := Lower(optimized)
	if err != nil {
		t.Fatalf("Lower: %v", err)
	}

	explain := physical.Explain()
	if !strings.Contains(explain, "5") {
		t.Fatalf("expected Explain output to contain \"5\" (PushedLimit), got:\n%s", explain)
	}
	// Specifically check the PushedLimit annotation produced by describePhysicalNode.
	if !strings.Contains(explain, "PushedLimit=5") {
		t.Fatalf("expected PushedLimit=5 in Explain output, got:\n%s", explain)
	}
}
