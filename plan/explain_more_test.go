package plan

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/karthedew/cosma/expr"
)

// TestExplainLogicalEmpty covers the nil-plan / nil-root guard.
func TestExplainLogicalEmpty(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "<empty plan>", ExplainLogical(nil))
	assert.Equal(t, "<empty plan>", ExplainLogical(&LogicalPlan{Root: nil}))
}

// TestExplainPhysicalEmpty covers the nil-plan / nil-root guard.
func TestExplainPhysicalEmpty(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "<empty plan>", (*PhysicalPlan)(nil).Explain())
	assert.Equal(t, "<empty plan>", (&PhysicalPlan{Root: nil}).Explain())
}

// TestExplainLogicalAllNodes binds a plan covering every describable node and
// asserts each node's detail string appears in the logical explain output.
func TestExplainLogicalAllNodes(t *testing.T) {
	t.Parallel()

	s := abcSchemaI()
	left := NewScanNode(s, ScanSourceDataFrame)
	right := NewScanNode(s, ScanSourceDataFrame)

	root := NewLimitNode(
		NewSortNode(
			NewFilterNode(
				NewProjectNode(
					NewWithColumnNode(
						NewAggregateNode(
							NewJoinNode(left, right, "a", "inner"),
							[]string{"a"},
							[]expr.AggNode{expr.Col("b").Sum().As("sb")},
						),
						expr.Col("a").Add(expr.Lit(int64(1))).As("a2"),
					),
					[]string{"a"},
				),
				expr.Col("a").Gt(expr.Lit(int64(0))),
			),
			[]expr.SortKey{expr.By("a")},
		),
		4,
	)

	bound, err := Bind(NewLogicalPlan(root))
	require.NoError(t, err)
	out := ExplainLogical(bound)

	for _, sub := range []string{
		"Limit(n=4)",
		"Sort(keys=",
		"Filter(predicate=",
		"Project(columns=[a])",
		"WithColumn(expr=",
		"Aggregate(groupKeys=[a] aggs=",
		"Join(on=a how=inner)",
		"Scan(source=dataframe)",
	} {
		assert.Containsf(t, out, sub, "explain missing %q in:\n%s", sub, out)
	}
}

// TestExplainLogicalIndentation checks that children are indented two spaces per
// depth level.
func TestExplainLogicalIndentation(t *testing.T) {
	t.Parallel()

	root := NewFilterNode(NewScanNode(abcSchemaI(), ScanSourceDataFrame), expr.Col("a").Gt(expr.Lit(int64(0))))
	bound, err := Bind(NewLogicalPlan(root))
	require.NoError(t, err)
	out := ExplainLogical(bound)
	lines := strings.Split(strings.TrimRight(out, "\n"), "\n")
	require.Len(t, lines, 2)
	assert.False(t, strings.HasPrefix(lines[0], " "), "root not indented")
	assert.True(t, strings.HasPrefix(lines[1], "  "), "child indented two spaces")
}

// TestDescribeNodeScanEmptySource: a scan with an empty source string yields no
// detail (the source!="" guard), so Explain prints a bare "Scan".
func TestDescribeNodeScanEmptySource(t *testing.T) {
	t.Parallel()

	scan := NewScanNode(abcSchemaI(), "")
	out := ExplainLogical(NewLogicalPlan(scan))
	assert.Equal(t, "Scan\n", out)
}

// TestExplainPhysicalAllNodes lowers a full plan and asserts each physical
// node's detail appears in the physical explain output.
func TestExplainPhysicalAllNodes(t *testing.T) {
	t.Parallel()

	s := abcSchemaI()
	left := NewScanNode(s, ScanSourceDataFrame)
	right := NewScanNode(s, ScanSourceDataFrame)

	root := NewLimitNode(
		NewSortNode(
			NewFilterNode(
				NewProjectNode(
					NewWithColumnNode(
						NewAggregateNode(
							NewJoinNode(left, right, "a", "inner"),
							[]string{"a"},
							[]expr.AggNode{expr.Col("b").Sum().As("sb")},
						),
						expr.Col("a").Add(expr.Lit(int64(1))).As("a2"),
					),
					[]string{"a"},
				),
				expr.Col("a").Gt(expr.Lit(int64(0))),
			),
			[]expr.SortKey{expr.By("a")},
		),
		4,
	)

	bound, err := Bind(NewLogicalPlan(root))
	require.NoError(t, err)
	opt, err := Optimize(bound)
	require.NoError(t, err)
	phys, err := Lower(opt)
	require.NoError(t, err)
	out := phys.Explain()

	for _, sub := range []string{
		"PhysLimit(n=4)",
		"PhysSort(keys=",
		"PhysFilter(predicate=",
		"PhysProject(columns=[a])",
		"PhysWithColumn(expr=",
		"PhysAggregate(groupKeys=[a] aggs=",
		"PhysJoin(on=a how=inner)",
		"PhysScan(source=dataframe",
	} {
		assert.Containsf(t, out, sub, "explain missing %q in:\n%s", sub, out)
	}
}

// TestExplainPhysicalScanPushedColumnsAndFilters: a Project+Filter chain over a
// scan records PushedColumns and PushedFilters annotations visible in Explain.
func TestExplainPhysicalScanAnnotations(t *testing.T) {
	t.Parallel()

	root := NewProjectNode(
		NewFilterNode(NewScanNode(abcSchemaI(), ScanSourceDataFrame), expr.Col("b").Gt(expr.Lit(int64(0)))),
		[]string{"a"},
	)
	bound, err := Bind(NewLogicalPlan(root))
	require.NoError(t, err)
	opt, err := Optimize(bound)
	require.NoError(t, err)
	phys, err := Lower(opt)
	require.NoError(t, err)
	out := phys.Explain()

	assert.Contains(t, out, "PushedColumns=[a b]")
	assert.Contains(t, out, "PushedFilters=1")
}
