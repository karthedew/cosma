package plan_test

import (
	"reflect"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"

	"github.com/karthedew/cosma/expr"
	"github.com/karthedew/cosma/plan"
	"github.com/karthedew/cosma/schema"
)

// abcSchema is a three-column int64 source for column-pushdown tests.
func abcSchema() *schema.Schema {
	return schema.New(
		schema.Field{Name: "a", Type: schema.Int64, ArrowType: arrow.PrimitiveTypes.Int64},
		schema.Field{Name: "b", Type: schema.Int64, ArrowType: arrow.PrimitiveTypes.Int64},
		schema.Field{Name: "c", Type: schema.Int64, ArrowType: arrow.PrimitiveTypes.Int64},
	)
}

// findScan returns the first ScanNode in an optimized logical tree.
func findScan(n plan.LogicalNode) *plan.ScanNode {
	if s, ok := n.(*plan.ScanNode); ok {
		return s
	}
	for _, c := range n.Children() {
		if s := findScan(c); s != nil {
			return s
		}
	}
	return nil
}

// optimizedScan binds and optimizes a plan rooted at root and returns the scan.
func optimizedScan(t *testing.T, root plan.LogicalNode) *plan.ScanNode {
	t.Helper()
	bound, err := plan.Bind(plan.NewLogicalPlan(root))
	if err != nil {
		t.Fatalf("Bind: %v", err)
	}
	opt, err := plan.Optimize(bound)
	if err != nil {
		t.Fatalf("Optimize: %v", err)
	}
	s := findScan(opt.Root)
	if s == nil {
		t.Fatal("no scan in optimized plan")
	}
	return s
}

// TestPushColumnsIncludesFilterColumn: a filter column outside the projection is
// still pushed to the scan, so a streaming scan does not prune a column the
// surviving Filter needs.
func TestPushColumnsIncludesFilterColumn(t *testing.T) {
	// Project(a) -> Filter(b > 0) -> Scan{a,b,c}
	scan := plan.NewScanNode(abcSchema(), plan.ScanSourceDataFrame)
	root := plan.NewProjectNode(
		plan.NewFilterNode(scan, expr.Col("b").Gt(expr.Lit(int64(0)))),
		[]string{"a"},
	)
	got := optimizedScan(t, root).PushedColumns
	want := []string{"a", "b"} // schema order
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("PushedColumns = %v, want %v", got, want)
	}
}

// TestPushColumnsIncludesSortKey: a sort key outside the projection is pushed.
func TestPushColumnsIncludesSortKey(t *testing.T) {
	// Project(a) -> Sort(by b) -> Scan{a,b,c}
	scan := plan.NewScanNode(abcSchema(), plan.ScanSourceDataFrame)
	root := plan.NewProjectNode(
		plan.NewSortNode(scan, []expr.SortKey{expr.By("b")}),
		[]string{"a"},
	)
	got := optimizedScan(t, root).PushedColumns
	want := []string{"a", "b"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("PushedColumns = %v, want %v", got, want)
	}
}

// TestPushColumnsWithColumnInputs: a WithColumn whose expression reads a source
// column outside the projection still gets that input pushed, so the WithColumn
// can evaluate against a streaming scan that would otherwise have pruned it.
func TestPushColumnsWithColumnInputs(t *testing.T) {
	// Project(a) -> WithColumn(derived = b + 1) -> Scan{a,b,c}. The projection
	// keeps only a, but the WithColumn reads b, so b must reach the scan.
	scan := plan.NewScanNode(abcSchema(), plan.ScanSourceDataFrame)
	root := plan.NewProjectNode(
		plan.NewWithColumnNode(scan, expr.Col("b").Add(expr.Lit(int64(1))).As("derived")),
		[]string{"a"},
	)
	got := optimizedScan(t, root).PushedColumns
	want := []string{"a", "b"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("PushedColumns = %v, want %v", got, want)
	}
}

// TestPushColumnsNestedProject: a nested project's columns are included so the
// inner projection does not request a column pruned at the scan.
func TestPushColumnsNestedProject(t *testing.T) {
	// Project(a) -> Project(a,b) -> Filter(c > 0) -> Scan{a,b,c}
	scan := plan.NewScanNode(abcSchema(), plan.ScanSourceDataFrame)
	root := plan.NewProjectNode(
		plan.NewProjectNode(
			plan.NewFilterNode(scan, expr.Col("c").Gt(expr.Lit(int64(0)))),
			[]string{"a", "b"},
		),
		[]string{"a"},
	)
	got := optimizedScan(t, root).PushedColumns
	want := []string{"a", "b", "c"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("PushedColumns = %v, want %v", got, want)
	}
}

// TestPushColumnsPrunesWhenSafe: a projection with no column-referencing
// operators between it and the scan still prunes to exactly its columns.
func TestPushColumnsPrunesWhenSafe(t *testing.T) {
	// Project(a) -> Limit -> Scan{a,b,c}
	scan := plan.NewScanNode(abcSchema(), plan.ScanSourceDataFrame)
	root := plan.NewProjectNode(plan.NewLimitNode(scan, 10), []string{"a"})
	got := optimizedScan(t, root).PushedColumns
	want := []string{"a"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("PushedColumns = %v, want %v", got, want)
	}
}
