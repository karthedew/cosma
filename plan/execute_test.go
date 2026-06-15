package plan

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/karthedew/cosma/expr"
)

// buildPhysical binds and lowers a logical root, returning the physical plan.
func buildPhysical(t *testing.T, root LogicalNode) *PhysicalPlan {
	t.Helper()
	bound, err := Bind(NewLogicalPlan(root))
	require.NoError(t, err)
	phys, err := Lower(bound)
	require.NoError(t, err)
	return phys
}

// TestPhysicalPlanExecuteGuards covers the nil-plan and nil-executor guards.
func TestPhysicalPlanExecuteGuards(t *testing.T) {
	t.Parallel()

	_, err := (*PhysicalPlan)(nil).Execute(context.Background(), &fakeExecutor{})
	require.Error(t, err)

	_, err = (&PhysicalPlan{Root: nil}).Execute(context.Background(), &fakeExecutor{})
	require.Error(t, err)

	phys := buildPhysical(t, NewScanNode(abcSchemaI(), ScanSourceDataFrame))
	_, err = phys.Execute(context.Background(), nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "executor is nil")
}

// TestExecuteDispatchesEveryOperator drives Execute over a plan containing every
// operator and asserts the executor saw each method, in dependency order
// (children before parents).
func TestExecuteDispatchesEveryOperator(t *testing.T) {
	t.Parallel()

	s := abcSchemaI()
	left := NewScanNode(s, ScanSourceDataFrame)
	right := NewScanNode(s, ScanSourceDataFrame)

	root := NewLimitNode(
		NewSortNode(
			NewProjectNode(
				NewFilterNode(
					NewWithColumnNode(
						NewAggregateNode(
							NewJoinNode(left, right, "a", "inner"),
							[]string{"a"},
							[]expr.AggNode{expr.Col("b").Sum().As("sb")},
						),
						expr.Col("a").Add(expr.Lit(int64(1))).As("a2"),
					),
					expr.Col("a").Gt(expr.Lit(int64(0))),
				),
				[]string{"a"},
			),
			[]expr.SortKey{expr.By("a")},
		),
		4,
	)

	exec := &fakeExecutor{}
	phys := buildPhysical(t, root)
	out, err := phys.Execute(context.Background(), exec)
	require.NoError(t, err)
	require.NotNil(t, out)

	// Two scans (join left+right), then join, aggregate, withcolumn, filter,
	// project, sort, limit. Scan order is left then right.
	want := []string{"Scan", "Scan", "Join", "Aggregate", "WithColumn", "Filter", "Project", "Sort", "Limit"}
	assert.Equal(t, want, exec.ops())
}

// TestExecuteErrorPropagation table-drives each operator's error return, both
// from the operator itself and (for unary nodes) from the upstream scan.
func TestExecuteErrorPropagation(t *testing.T) {
	t.Parallel()

	s := abcSchemaI()
	scan := func() *ScanNode { return NewScanNode(s, ScanSourceDataFrame) }

	tests := []struct {
		name string
		root func() LogicalNode
		exec *fakeExecutor
	}{
		{"scan", func() LogicalNode { return scan() }, &fakeExecutor{failScan: true}},
		{"filter", func() LogicalNode { return NewFilterNode(scan(), expr.Col("a").Gt(expr.Lit(int64(0)))) }, &fakeExecutor{failFilter: true}},
		{"filter upstream", func() LogicalNode { return NewFilterNode(scan(), expr.Col("a").Gt(expr.Lit(int64(0)))) }, &fakeExecutor{failScan: true}},
		{"project", func() LogicalNode { return NewProjectNode(scan(), []string{"a"}) }, &fakeExecutor{failProject: true}},
		{"project upstream", func() LogicalNode { return NewProjectNode(scan(), []string{"a"}) }, &fakeExecutor{failScan: true}},
		{"limit", func() LogicalNode { return NewLimitNode(scan(), 5) }, &fakeExecutor{failLimit: true}},
		{"sort", func() LogicalNode { return NewSortNode(scan(), []expr.SortKey{expr.By("a")}) }, &fakeExecutor{failSort: true}},
		{"sort upstream", func() LogicalNode { return NewSortNode(scan(), []expr.SortKey{expr.By("a")}) }, &fakeExecutor{failScan: true}},
		{"aggregate", func() LogicalNode {
			return NewAggregateNode(scan(), []string{"a"}, []expr.AggNode{expr.Col("b").Sum()})
		}, &fakeExecutor{failAggregate: true}},
		{"withcolumn", func() LogicalNode {
			return NewWithColumnNode(scan(), expr.Col("a").Add(expr.Lit(int64(1))).As("d"))
		}, &fakeExecutor{failWithColumn: true}},
		{"withcolumn upstream", func() LogicalNode {
			return NewWithColumnNode(scan(), expr.Col("a").Add(expr.Lit(int64(1))).As("d"))
		}, &fakeExecutor{failScan: true}},
		{"join", func() LogicalNode { return NewJoinNode(scan(), scan(), "a", "inner") }, &fakeExecutor{failJoin: true}},
		{"join left", func() LogicalNode { return NewJoinNode(scan(), scan(), "a", "inner") }, &fakeExecutor{failScan: true}},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			phys := buildPhysical(t, tt.root())
			_, err := phys.Execute(context.Background(), tt.exec)
			require.Error(t, err)
		})
	}
}

// TestJoinRightErrorPropagation forces the right side of a join to fail after
// the left succeeds, covering PhysJoin's second error branch.
func TestJoinRightErrorPropagation(t *testing.T) {
	t.Parallel()

	// A join whose left is a scan and right is a filter; fail the filter so the
	// left scan has already succeeded when the right errors.
	s := abcSchemaI()
	left := NewScanNode(s, ScanSourceDataFrame)
	right := NewFilterNode(NewScanNode(s, ScanSourceDataFrame), expr.Col("a").Gt(expr.Lit(int64(0))))
	root := NewJoinNode(left, right, "a", "inner")

	phys := buildPhysical(t, root)
	_, err := phys.Execute(context.Background(), &fakeExecutor{failFilter: true})
	require.Error(t, err)
}
