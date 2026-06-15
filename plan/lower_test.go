package plan

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/karthedew/cosma/expr"
)

// TestLowerNilPlan covers Lower's empty-plan guard.
func TestLowerNilPlan(t *testing.T) {
	t.Parallel()

	_, err := Lower(nil)
	require.Error(t, err)

	_, err = Lower(&LogicalPlan{Root: nil})
	require.Error(t, err)
}

// TestLowerEveryNode binds then lowers a plan containing every node kind and
// asserts each logical node maps to its expected physical type, preserving the
// scalar fields (columns, limit N, join keys, group keys).
func TestLowerEveryNode(t *testing.T) {
	t.Parallel()

	s := abcSchemaI()
	left := NewScanNode(s, ScanSourceDataFrame)
	right := NewScanNode(s, ScanSourceDataFrame)

	logical := NewLimitNode(
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

	bound, err := Bind(NewLogicalPlan(logical))
	require.NoError(t, err)
	phys, err := Lower(bound)
	require.NoError(t, err)

	limit, ok := phys.Root.(*PhysLimit)
	require.True(t, ok, "root should be PhysLimit")
	assert.Equal(t, int64(4), limit.N)

	sort, ok := limit.Input.(*PhysSort)
	require.True(t, ok, "limit input should be PhysSort")
	require.Len(t, sort.Keys, 1)
	assert.Equal(t, "a", sort.Keys[0].Column)

	proj, ok := sort.Input.(*PhysProject)
	require.True(t, ok, "sort input should be PhysProject")
	assert.Equal(t, []string{"a"}, proj.Columns)

	filter, ok := proj.Input.(*PhysFilter)
	require.True(t, ok, "project input should be PhysFilter")
	assert.NotNil(t, filter.Predicate.Node)

	withcol, ok := filter.Input.(*PhysWithColumn)
	require.True(t, ok, "filter input should be PhysWithColumn")
	assert.NotNil(t, withcol.Expr.Node)

	agg, ok := withcol.Input.(*PhysAggregate)
	require.True(t, ok, "withcolumn input should be PhysAggregate")
	assert.Equal(t, []string{"a"}, agg.GroupKeys)
	require.Len(t, agg.Aggs, 1)

	join, ok := agg.Input.(*PhysJoin)
	require.True(t, ok, "aggregate input should be PhysJoin")
	assert.Equal(t, "a", join.On)
	assert.Equal(t, "inner", join.How)

	_, ok = join.Left.(*PhysScan)
	assert.True(t, ok, "join left should be PhysScan")
	_, ok = join.Right.(*PhysScan)
	assert.True(t, ok, "join right should be PhysScan")
}

// TestLowerUnsupportedNode exercises lowerNode's default branch via an unknown
// logical node type.
func TestLowerUnsupportedNode(t *testing.T) {
	t.Parallel()

	_, err := Lower(NewLogicalPlan(brokenNode{}))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cannot lower")
}

// TestLowerChildErrorPropagation covers the recursive error returns in lowerNode
// for each parent kind by giving it an unlowerable (broken) child. Join is
// checked for both the left and right child branches.
func TestLowerChildErrorPropagation(t *testing.T) {
	t.Parallel()

	broken := brokenNode{}
	tests := []struct {
		name string
		root LogicalNode
	}{
		{"filter", &FilterNode{Input: broken, Predicate: expr.Col("a").Gt(expr.Lit(int64(0)))}},
		{"project", &ProjectNode{Input: broken, Columns: []string{"a"}}},
		{"limit", &LimitNode{Input: broken, N: 1}},
		{"sort", &SortNode{Input: broken, Keys: []expr.SortKey{expr.By("a")}}},
		{"aggregate", &AggregateNode{Input: broken, GroupKeys: []string{"a"}, Aggs: []expr.AggNode{expr.Col("b").Sum()}}},
		{"withcolumn", &WithColumnNode{Input: broken, Expr: expr.Col("a")}},
		{"join left", &JoinNode{Left: broken, Right: &ScanNode{schema: abcSchemaI()}, On: "a", How: "inner"}},
		{"join right", &JoinNode{Left: &ScanNode{schema: abcSchemaI()}, Right: broken, On: "a", How: "inner"}},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := Lower(NewLogicalPlan(tt.root))
			require.Error(t, err)
		})
	}
}

// TestLowerScanPreservesNode confirms PhysScan keeps a pointer to the lowered
// ScanNode (including its pushdown annotations) rather than copying.
func TestLowerScanPreservesNode(t *testing.T) {
	t.Parallel()

	bound, err := Bind(NewLogicalPlan(NewScanNode(abcSchemaI(), ScanSourceDataFrame)))
	require.NoError(t, err)
	phys, err := Lower(bound)
	require.NoError(t, err)

	scan := phys.Root.(*PhysScan)
	assert.Same(t, bound.Root.(*ScanNode), scan.Node)
}
