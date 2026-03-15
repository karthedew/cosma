package plan

import (
	"testing"

	"github.com/apache/arrow/go/v18/arrow"

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
