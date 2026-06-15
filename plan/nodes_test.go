package plan

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/karthedew/cosma/expr"
)

// TestLogicalNodeNamesAndChildren walks every logical node type and asserts its
// Name() and Children() wiring. Children are checked by identity (pointer) so a
// node that returns the wrong input is caught.
func TestLogicalNodeNamesAndChildren(t *testing.T) {
	t.Parallel()

	s := abcSchemaI()
	scan := NewScanNode(s, ScanSourceDataFrame)
	right := NewScanNode(s, ScanSourceDataFrame)

	filter := NewFilterNode(scan, expr.Col("a").Gt(expr.Lit(int64(0))))
	project := NewProjectNode(scan, []string{"a", "b"})
	limit := NewLimitNode(scan, 7)
	withcol := NewWithColumnNode(scan, expr.Col("a").Add(expr.Lit(int64(1))).As("d"))
	sort := NewSortNode(scan, []expr.SortKey{expr.By("a")})
	agg := NewAggregateNode(scan, []string{"a"}, []expr.AggNode{expr.Col("b").Sum().As("sb")})
	join := NewJoinNode(scan, right, "a", "inner")

	tests := []struct {
		name     string
		node     LogicalNode
		wantName string
		children []LogicalNode
	}{
		{"scan", scan, "Scan", nil},
		{"filter", filter, "Filter", []LogicalNode{scan}},
		{"project", project, "Project", []LogicalNode{scan}},
		{"limit", limit, "Limit", []LogicalNode{scan}},
		{"withcolumn", withcol, "WithColumn", []LogicalNode{scan}},
		{"sort", sort, "Sort", []LogicalNode{scan}},
		{"aggregate", agg, "Aggregate", []LogicalNode{scan}},
		{"join", join, "Join", []LogicalNode{scan, right}},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.wantName, tt.node.Name())
			require.Len(t, tt.node.Children(), len(tt.children))
			for i, c := range tt.children {
				assert.Same(t, c, tt.node.Children()[i])
			}
		})
	}
}

// TestScanNodeAccessors checks the scan-only accessors and constructor defaults.
func TestScanNodeAccessors(t *testing.T) {
	t.Parallel()

	s := abcSchemaI()
	scan := NewScanNode(s, ScanSourceFile)
	assert.Equal(t, "Scan", scan.Name())
	assert.Same(t, s, scan.Schema())
	assert.Equal(t, ScanSourceFile, scan.Source())
	assert.Nil(t, scan.Children())
	// PushedLimit defaults to -1 ("no limit pushed").
	assert.Equal(t, int64(-1), scan.PushedLimit)
}

// TestLogicalNodeSchemaAfterBind confirms each node's Schema() accessor returns
// the schema populated by Bind (it is nil before binding for non-scan nodes).
func TestLogicalNodeSchemaAfterBind(t *testing.T) {
	t.Parallel()

	s := abcSchemaI()

	// Pre-bind: non-scan nodes carry no schema.
	pre := NewFilterNode(NewScanNode(s, ScanSourceDataFrame), expr.Col("a").Gt(expr.Lit(int64(0))))
	assert.Nil(t, pre.Schema(), "filter schema is nil before bind")

	bound, err := Bind(NewLogicalPlan(pre))
	require.NoError(t, err)
	assert.NotNil(t, bound.Root.Schema(), "filter schema is set after bind")
}

// TestPhysicalNodeNamesAndChildren mirrors the logical test for physical nodes,
// asserting Name() and the Children() wiring of each operator.
func TestPhysicalNodeNamesAndChildren(t *testing.T) {
	t.Parallel()

	scan := &PhysScan{Node: NewScanNode(abcSchemaI(), ScanSourceDataFrame)}
	right := &PhysScan{Node: NewScanNode(abcSchemaI(), ScanSourceDataFrame)}

	filter := &PhysFilter{Input: scan, Predicate: expr.Col("a").Gt(expr.Lit(int64(0)))}
	project := &PhysProject{Input: scan, Columns: []string{"a"}}
	limit := &PhysLimit{Input: scan, N: 3}
	sort := &PhysSort{Input: scan, Keys: []expr.SortKey{expr.By("a")}}
	agg := &PhysAggregate{Input: scan, GroupKeys: []string{"a"}, Aggs: []expr.AggNode{expr.Col("b").Sum()}}
	withcol := &PhysWithColumn{Input: scan, Expr: expr.Col("a").Add(expr.Lit(int64(1)))}
	join := &PhysJoin{Left: scan, Right: right, On: "a", How: "inner"}

	tests := []struct {
		name     string
		node     PhysicalNode
		wantName string
		children []PhysicalNode
	}{
		{"scan", scan, "PhysScan", nil},
		{"filter", filter, "PhysFilter", []PhysicalNode{scan}},
		{"project", project, "PhysProject", []PhysicalNode{scan}},
		{"limit", limit, "PhysLimit", []PhysicalNode{scan}},
		{"sort", sort, "PhysSort", []PhysicalNode{scan}},
		{"aggregate", agg, "PhysAggregate", []PhysicalNode{scan}},
		{"withcolumn", withcol, "PhysWithColumn", []PhysicalNode{scan}},
		{"join", join, "PhysJoin", []PhysicalNode{scan, right}},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.wantName, tt.node.Name())
			require.Len(t, tt.node.Children(), len(tt.children))
			for i, c := range tt.children {
				assert.Same(t, c, tt.node.Children()[i])
			}
			// Schema() accessor: nil here since nodes were built without lowering,
			// but the call must not panic.
			assert.Nil(t, tt.node.Schema())
		})
	}
}

// TestPhysicalNodeSchema confirms Schema() returns the arrow schema stored on
// the node (set during Lower from the logical schema).
func TestPhysicalNodeSchema(t *testing.T) {
	t.Parallel()

	logical := NewProjectNode(NewScanNode(abcSchemaI(), ScanSourceDataFrame), []string{"a"})
	bound, err := Bind(NewLogicalPlan(logical))
	require.NoError(t, err)
	phys, err := Lower(bound)
	require.NoError(t, err)

	require.NotNil(t, phys.Root.Schema())
	// Project narrows to one column, so the arrow schema should carry one field.
	assert.Equal(t, 1, phys.Root.Schema().NumFields())
}
