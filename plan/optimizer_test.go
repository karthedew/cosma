package plan

import (
	"reflect"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/karthedew/cosma/expr"
	"github.com/karthedew/cosma/schema"
)

// optScan binds, optimizes, and returns the (first) scan from a logical root.
func optScan(t *testing.T, root LogicalNode) *ScanNode {
	t.Helper()
	bound, err := Bind(NewLogicalPlan(root))
	require.NoError(t, err)
	opt, err := Optimize(bound)
	require.NoError(t, err)
	return findScanI(opt.Root)
}

func findScanI(n LogicalNode) *ScanNode {
	if s, ok := n.(*ScanNode); ok {
		return s
	}
	for _, c := range n.Children() {
		if s := findScanI(c); s != nil {
			return s
		}
	}
	return nil
}

// TestOptimizeNilPlan covers the empty-plan guard.
func TestOptimizeNilPlan(t *testing.T) {
	t.Parallel()

	_, err := Optimize(nil)
	require.Error(t, err)
	_, err = Optimize(&LogicalPlan{Root: nil})
	require.Error(t, err)
}

// TestPushFiltersRecordsPredicate: a single filter chain records its predicate
// on the scan's PushedFilters.
func TestPushFiltersRecordsPredicate(t *testing.T) {
	t.Parallel()

	root := NewFilterNode(NewScanNode(abcSchemaI(), ScanSourceDataFrame), expr.Col("a").Gt(expr.Lit(int64(0))))
	scan := optScan(t, root)
	assert.Len(t, scan.PushedFilters, 1)
}

// TestPushFiltersMultiple: two stacked filters both reach the scan.
func TestPushFiltersMultiple(t *testing.T) {
	t.Parallel()

	root := NewFilterNode(
		NewFilterNode(NewScanNode(abcSchemaI(), ScanSourceDataFrame), expr.Col("a").Gt(expr.Lit(int64(0)))),
		expr.Col("b").Lt(expr.Lit(int64(10))),
	)
	scan := optScan(t, root)
	assert.Len(t, scan.PushedFilters, 2)
}

// TestPushFiltersChainBroken: a filter above an Aggregate has no linear scan
// below it (scanBelow returns nil), so nothing is pushed onto the inner scan.
func TestPushFiltersChainBroken(t *testing.T) {
	t.Parallel()

	// Filter(a>0) -> Aggregate -> Scan. The aggregate breaks the chain, so the
	// outer filter is NOT pushed; the inner scan keeps zero pushed filters.
	root := NewFilterNode(
		NewAggregateNode(NewScanNode(abcSchemaI(), ScanSourceDataFrame), []string{"a"}, []expr.AggNode{expr.Col("b").Sum().As("sb")}),
		expr.Col("a").Gt(expr.Lit(int64(0))),
	)
	scan := optScan(t, root)
	assert.Empty(t, scan.PushedFilters)
}

// TestPushColumnsChainBrokenByAggregate: a Project above an Aggregate cannot see
// a scan in a linear chain, so PushedColumns is left nil (no pushdown).
func TestPushColumnsChainBrokenByAggregate(t *testing.T) {
	t.Parallel()

	root := NewProjectNode(
		NewAggregateNode(NewScanNode(abcSchemaI(), ScanSourceDataFrame), []string{"a"}, []expr.AggNode{expr.Col("b").Sum().As("sb")}),
		[]string{"a"},
	)
	scan := optScan(t, root)
	assert.Nil(t, scan.PushedColumns)
}

// TestPushColumnsChainBrokenByJoin: a Project over a Join has no single scan
// below it, so no columns are pushed onto either scan.
func TestPushColumnsChainBrokenByJoin(t *testing.T) {
	t.Parallel()

	left := NewScanNode(abcSchemaI(), ScanSourceDataFrame)
	right := NewScanNode(abcSchemaI(), ScanSourceDataFrame)
	root := NewProjectNode(NewJoinNode(left, right, "a", "inner"), []string{"a"})

	bound, err := Bind(NewLogicalPlan(root))
	require.NoError(t, err)
	opt, err := Optimize(bound)
	require.NoError(t, err)

	join := opt.Root.(*ProjectNode).Input.(*JoinNode)
	assert.Nil(t, join.Left.(*ScanNode).PushedColumns)
	assert.Nil(t, join.Right.(*ScanNode).PushedColumns)
}

// TestPushColumnsFirstProjectWins: the outer Project's set is computed first, so
// a second (inner) Project's pass is skipped because PushedColumns is already
// set. The union from the first pass (a, plus the inner project's b) governs.
func TestPushColumnsFirstProjectWins(t *testing.T) {
	t.Parallel()

	root := NewProjectNode(
		NewProjectNode(NewScanNode(abcSchemaI(), ScanSourceDataFrame), []string{"a", "b"}),
		[]string{"a"},
	)
	scan := optScan(t, root)
	// Outer pass collects a (outer) + a,b (inner project) = {a,b} in schema order.
	assert.Equal(t, []string{"a", "b"}, scan.PushedColumns)
}

// TestPushColumnsNoMatchYieldsNil: a Project that references only a derived
// column (not present in the scan schema) yields an empty intersection, leaving
// PushedColumns nil. The tree is constructed pre-bound (schemas set by hand) so
// the projection of a non-source column is not rejected by Bind's validation;
// this isolates intersectSchemaColumns' empty-result branch.
func TestPushColumnsNoMatchYieldsNil(t *testing.T) {
	t.Parallel()

	s := abcSchemaI()
	scan := &ScanNode{schema: s, source: ScanSourceDataFrame, PushedLimit: -1}
	// The derived expression references no source column (a pure literal), so the
	// only "needed" name is the projected "d", which is not in the scan schema.
	wc := &WithColumnNode{Input: scan, Expr: expr.Lit(int64(1)).As("d"), schema: s}
	root := &ProjectNode{Input: wc, Columns: []string{"d"}, schema: s}

	opt, err := Optimize(NewLogicalPlan(root))
	require.NoError(t, err)
	assert.Nil(t, findScanI(opt.Root).PushedColumns)
}

// TestPushLimitMinAcrossNested: two nested limits push the smaller value.
func TestPushLimitMinAcrossNested(t *testing.T) {
	t.Parallel()

	// Limit(3) -> Limit(10) -> Scan. The smaller bound (3) is pushed.
	root := NewLimitNode(NewLimitNode(NewScanNode(abcSchemaI(), ScanSourceDataFrame), 10), 3)
	scan := optScan(t, root)
	assert.Equal(t, int64(3), scan.PushedLimit)
}

// TestPushLimitAcrossRowPreserving: a limit pushes through Project and
// WithColumn (row-preserving) down to the scan.
func TestPushLimitAcrossRowPreserving(t *testing.T) {
	t.Parallel()

	root := NewLimitNode(
		NewProjectNode(
			NewWithColumnNode(NewScanNode(abcSchemaI(), ScanSourceDataFrame), expr.Col("a").Add(expr.Lit(int64(1))).As("d")),
			[]string{"a"},
		),
		5,
	)
	scan := optScan(t, root)
	assert.Equal(t, int64(5), scan.PushedLimit)
}

// TestPushLimitStopsAtFilter: a limit above a filter must NOT be pushed to the
// scan (pushing before a filter drops the wrong rows). PushedLimit stays -1.
func TestPushLimitStopsAtFilter(t *testing.T) {
	t.Parallel()

	root := NewLimitNode(
		NewFilterNode(NewScanNode(abcSchemaI(), ScanSourceDataFrame), expr.Col("a").Gt(expr.Lit(int64(0)))),
		5,
	)
	scan := optScan(t, root)
	assert.Equal(t, int64(-1), scan.PushedLimit)
}

// TestPushLimitStopsAtSort: a limit above a sort must NOT be pushed (pushing
// before a sort keeps the wrong rows).
func TestPushLimitStopsAtSort(t *testing.T) {
	t.Parallel()

	root := NewLimitNode(
		NewSortNode(NewScanNode(abcSchemaI(), ScanSourceDataFrame), []expr.SortKey{expr.By("a")}),
		5,
	)
	scan := optScan(t, root)
	assert.Equal(t, int64(-1), scan.PushedLimit)
}

// TestOptimizeDoesNotMutateInput confirms cloneNode isolates the optimizer: the
// original plan's scan keeps its defaults after Optimize annotates the clone.
func TestOptimizeDoesNotMutateInput(t *testing.T) {
	t.Parallel()

	src := NewScanNode(abcSchemaI(), ScanSourceDataFrame)
	root := NewFilterNode(NewLimitNode(src, 5), expr.Col("a").Gt(expr.Lit(int64(0))))

	bound, err := Bind(NewLogicalPlan(root))
	require.NoError(t, err)
	boundScan := findScanI(bound.Root)

	_, err = Optimize(bound)
	require.NoError(t, err)

	assert.Empty(t, boundScan.PushedFilters, "input scan filters untouched")
	assert.Nil(t, boundScan.PushedColumns, "input scan columns untouched")
	// Limit sits above a filter, so even on the clone it is not pushed; the input
	// scan likewise keeps the default.
	assert.Equal(t, int64(-1), boundScan.PushedLimit)
}

// TestCloneNodePreservesStructure round-trips every node kind through cloneNode
// (via Optimize) and confirms the tree shape and scalar fields survive.
func TestCloneNodePreservesStructure(t *testing.T) {
	t.Parallel()

	s := abcSchemaI()
	left := NewScanNode(s, ScanSourceDataFrame)
	right := NewScanNode(s, ScanSourceDataFrame)
	root := NewSortNode(
		NewAggregateNode(
			NewJoinNode(left, right, "a", "inner"),
			[]string{"a"},
			[]expr.AggNode{expr.Col("b").Sum().As("sb")},
		),
		[]expr.SortKey{expr.By("a")},
	)

	bound, err := Bind(NewLogicalPlan(root))
	require.NoError(t, err)
	opt, err := Optimize(bound)
	require.NoError(t, err)

	sort, ok := opt.Root.(*SortNode)
	require.True(t, ok)
	agg, ok := sort.Input.(*AggregateNode)
	require.True(t, ok)
	assert.Equal(t, []string{"a"}, agg.GroupKeys)
	join, ok := agg.Input.(*JoinNode)
	require.True(t, ok)
	assert.Equal(t, "inner", join.How)
	assert.NotSame(t, left, join.Left, "clone produces fresh nodes")
}

// TestScanBelowReturnsNilOnBranch indirectly checks scanBelow's branch handling
// by confirming a filter directly above a join records nothing on either scan.
func TestPushFiltersAboveJoinNotPushed(t *testing.T) {
	t.Parallel()

	left := NewScanNode(abcSchemaI(), ScanSourceDataFrame)
	right := NewScanNode(abcSchemaI(), ScanSourceDataFrame)
	root := NewFilterNode(NewJoinNode(left, right, "a", "inner"), expr.Col("a").Gt(expr.Lit(int64(0))))

	bound, err := Bind(NewLogicalPlan(root))
	require.NoError(t, err)
	opt, err := Optimize(bound)
	require.NoError(t, err)

	join := opt.Root.(*FilterNode).Input.(*JoinNode)
	assert.Empty(t, join.Left.(*ScanNode).PushedFilters)
	assert.Empty(t, join.Right.(*ScanNode).PushedFilters)
}

// TestPushColumnsIntersectsSchema confirms only columns present in the scan
// schema survive: a project naming a real column plus a derived one keeps just
// the real one.
func TestPushColumnsIntersectsSchema(t *testing.T) {
	t.Parallel()

	wide := schema.New(
		schema.Field{Name: "a", Type: schema.Int64, ArrowType: arrow.PrimitiveTypes.Int64},
		schema.Field{Name: "b", Type: schema.Int64, ArrowType: arrow.PrimitiveTypes.Int64},
	)
	// Pre-bound tree: the Project names a real column ("a") and a derived one
	// ("d"); only the real column survives the schema intersection.
	scan := &ScanNode{schema: wide, source: ScanSourceDataFrame, PushedLimit: -1}
	wc := &WithColumnNode{Input: scan, Expr: expr.Col("a").Add(expr.Lit(int64(1))).As("d"), schema: wide}
	root := &ProjectNode{Input: wc, Columns: []string{"a", "d"}, schema: wide}

	opt, err := Optimize(NewLogicalPlan(root))
	require.NoError(t, err)
	got := findScanI(opt.Root).PushedColumns
	want := []string{"a"} // "d" is derived, not in the scan schema
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("PushedColumns = %v, want %v", got, want)
	}
}
