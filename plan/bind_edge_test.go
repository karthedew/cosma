package plan

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/karthedew/cosma/expr"
	"github.com/karthedew/cosma/schema"
)

// brokenNode is an unknown LogicalNode type used to exercise the default branch
// of bindNode / lowerNode.
type brokenNode struct{}

func (brokenNode) Name() string            { return "Broken" }
func (brokenNode) Schema() *schema.Schema  { return nil }
func (brokenNode) Children() []LogicalNode { return nil }

// TestBindNilPlan covers Bind's empty-plan guard for both a nil plan and a plan
// with a nil root.
func TestBindNilPlan(t *testing.T) {
	t.Parallel()

	_, err := Bind(nil)
	require.Error(t, err)

	_, err = Bind(&LogicalPlan{Root: nil})
	require.Error(t, err)
}

// TestBindErrors table-drives the per-node error returns: nil inputs, nil
// predicates/expressions, empty column/key sets, missing columns, and bad join
// modes. Each case asserts Bind fails with a message mentioning the cause.
func TestBindErrors(t *testing.T) {
	t.Parallel()

	s := abcSchemaI()
	scan := func() *ScanNode { return NewScanNode(s, ScanSourceDataFrame) }

	tests := []struct {
		name    string
		root    LogicalNode
		wantSub string
	}{
		{"scan nil schema", NewScanNode(nil, ScanSourceDataFrame), "scan schema is nil"},
		{"filter nil input", &FilterNode{Input: nil, Predicate: expr.Col("a").Gt(expr.Lit(int64(0)))}, "filter input is nil"},
		{"filter nil predicate", &FilterNode{Input: scan(), Predicate: expr.Expr{}}, "filter predicate is nil"},
		{"filter missing column", NewFilterNode(scan(), expr.Col("zzz").Gt(expr.Lit(int64(0)))), "not in schema"},
		{"project nil input", &ProjectNode{Input: nil, Columns: []string{"a"}}, "project input is nil"},
		{"project empty columns", NewProjectNode(scan(), nil), "project columns are empty"},
		{"project empty name", NewProjectNode(scan(), []string{""}), "project column name is empty"},
		{"project duplicate", NewProjectNode(scan(), []string{"a", "a"}), "duplicate project column"},
		{"project missing column", NewProjectNode(scan(), []string{"zzz"}), "not in schema"},
		{"limit nil input", &LimitNode{Input: nil, N: 1}, "limit input is nil"},
		{"limit negative", NewLimitNode(scan(), -5), "limit must be >= 0"},
		{"withcolumn nil input", &WithColumnNode{Input: nil, Expr: expr.Col("a")}, "withcolumn input is nil"},
		{"withcolumn nil expr", &WithColumnNode{Input: scan(), Expr: expr.Expr{}}, "withcolumn expression is nil"},
		{"sort nil input", &SortNode{Input: nil, Keys: []expr.SortKey{expr.By("a")}}, "sort input is nil"},
		{"sort empty keys", NewSortNode(scan(), nil), "sort keys are empty"},
		{"sort missing column", NewSortNode(scan(), []expr.SortKey{expr.By("zzz")}), "not in schema"},
		{"aggregate nil input", &AggregateNode{Input: nil, Aggs: []expr.AggNode{expr.Col("a").Sum()}}, "aggregate input is nil"},
		{"aggregate empty aggs", NewAggregateNode(scan(), []string{"a"}, nil), "aggregate aggs are empty"},
		{"aggregate missing key", NewAggregateNode(scan(), []string{"zzz"}, []expr.AggNode{expr.Col("a").Sum()}), "not in schema"},
		{"join nil left", &JoinNode{Left: nil, Right: scan(), On: "a", How: "inner"}, "join input is nil"},
		{"join nil right", &JoinNode{Left: scan(), Right: nil, On: "a", How: "inner"}, "join input is nil"},
		{"join empty key", NewJoinNode(scan(), scan(), "", "inner"), "join key is empty"},
		{"join bad how", NewJoinNode(scan(), scan(), "a", "cross"), "unsupported how"},
		{"join missing left key", NewJoinNode(NewScanNode(schema.New(schema.Field{Name: "x", Type: schema.Int64, ArrowType: arrow.PrimitiveTypes.Int64}), ScanSourceDataFrame), scan(), "a", "inner"), "not in left schema"},
		{"join missing right key", NewJoinNode(scan(), NewScanNode(schema.New(schema.Field{Name: "x", Type: schema.Int64, ArrowType: arrow.PrimitiveTypes.Int64}), ScanSourceDataFrame), "a", "inner"), "not in right schema"},
		{"unsupported node", brokenNode{}, "unsupported logical node"},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := Bind(NewLogicalPlan(tt.root))
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantSub)
		})
	}
}

// TestBindErrorPropagatesFromChild confirms an error deep in the tree surfaces
// through the recursive bindNode walk (filter over a project with a bad column).
func TestBindErrorPropagatesFromChild(t *testing.T) {
	t.Parallel()

	root := NewFilterNode(
		NewProjectNode(NewScanNode(abcSchemaI(), ScanSourceDataFrame), []string{"zzz"}),
		expr.Col("a").Gt(expr.Lit(int64(0))),
	)
	_, err := Bind(NewLogicalPlan(root))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not in schema")
}

// TestBindSuccessEveryNode binds a plan containing every node kind and asserts
// no error and that each bound node carries a non-nil schema.
func TestBindSuccessEveryNode(t *testing.T) {
	t.Parallel()

	s := abcSchemaI()
	left := NewScanNode(s, ScanSourceDataFrame)
	right := NewScanNode(s, ScanSourceDataFrame)

	// Aggregate(a -> sum b) over a Join, then sort, limit, project, withcolumn.
	root := NewLimitNode(
		NewSortNode(
			NewProjectNode(
				NewWithColumnNode(
					NewAggregateNode(
						NewJoinNode(left, right, "a", "left"),
						[]string{"a"},
						[]expr.AggNode{expr.Col("b").Sum().As("sb")},
					),
					expr.Col("a").Add(expr.Lit(int64(1))).As("a2"),
				),
				[]string{"a"},
			),
			[]expr.SortKey{expr.By("a")},
		),
		10,
	)

	bound, err := Bind(NewLogicalPlan(root))
	require.NoError(t, err)

	var check func(n LogicalNode)
	check = func(n LogicalNode) {
		assert.NotNilf(t, n.Schema(), "%s schema should be non-nil after bind", n.Name())
		for _, c := range n.Children() {
			check(c)
		}
	}
	check(bound.Root)
}

// TestBindCloneIndependence verifies Bind returns a fresh ScanNode rather than
// aliasing the input (so later optimizer mutation cannot leak back). It mutates
// the bound scan's PushedLimit and checks the source scan is untouched.
func TestBindCloneIndependence(t *testing.T) {
	t.Parallel()

	src := NewScanNode(abcSchemaI(), ScanSourceDataFrame)
	bound, err := Bind(NewLogicalPlan(src))
	require.NoError(t, err)

	boundScan := bound.Root.(*ScanNode)
	assert.NotSame(t, src, boundScan, "bind should return a new scan node")
	boundScan.PushedLimit = 99
	assert.Equal(t, int64(-1), src.PushedLimit, "source scan must be unchanged")
}
