package plan

import (
	"context"
	"fmt"

	"github.com/apache/arrow/go/v18/arrow"

	"github.com/karthedew/cosma/expr"
	"github.com/karthedew/cosma/schema"
)

// PhysicalNode is one executable operator. Execute runs the node against the
// injected Executor and returns the resulting DataFrame as an opaque handle.
type PhysicalNode interface {
	Name() string
	Schema() *arrow.Schema
	Children() []PhysicalNode
	Execute(ctx context.Context, exec Executor) (any, error)
}

type PhysicalPlan struct {
	Root PhysicalNode
}

func NewPhysicalPlan(root PhysicalNode) *PhysicalPlan {
	return &PhysicalPlan{Root: root}
}

// Execute runs the physical plan against exec and returns the resulting
// DataFrame handle (a *dataframe.DataFrame).
func (p *PhysicalPlan) Execute(ctx context.Context, exec Executor) (any, error) {
	if p == nil || p.Root == nil {
		return nil, fmt.Errorf("physical plan is empty")
	}
	if exec == nil {
		return nil, fmt.Errorf("executor is nil")
	}
	return p.Root.Execute(ctx, exec)
}

// arrowSchema converts a cosma schema to arrow, returning nil on failure. It is
// best-effort metadata for Explain and callers that introspect the plan; node
// correctness does not depend on it.
func arrowSchema(s *schema.Schema) *arrow.Schema {
	if s == nil {
		return nil
	}
	a, err := schema.ToArrow(s)
	if err != nil {
		return nil
	}
	return a
}

// PhysScan reads the source DataFrame, honoring any pushdown annotations.
type PhysScan struct {
	Node   *ScanNode
	schema *arrow.Schema
}

func (n *PhysScan) Name() string             { return "PhysScan" }
func (n *PhysScan) Schema() *arrow.Schema    { return n.schema }
func (n *PhysScan) Children() []PhysicalNode { return nil }
func (n *PhysScan) Execute(ctx context.Context, exec Executor) (any, error) {
	return exec.Scan(ctx, n.Node)
}

type PhysFilter struct {
	Input     PhysicalNode
	Predicate expr.Expr
	schema    *arrow.Schema
}

func (n *PhysFilter) Name() string             { return "PhysFilter" }
func (n *PhysFilter) Schema() *arrow.Schema    { return n.schema }
func (n *PhysFilter) Children() []PhysicalNode { return []PhysicalNode{n.Input} }
func (n *PhysFilter) Execute(ctx context.Context, exec Executor) (any, error) {
	in, err := n.Input.Execute(ctx, exec)
	if err != nil {
		return nil, err
	}
	return exec.Filter(ctx, in, n.Predicate)
}

type PhysProject struct {
	Input   PhysicalNode
	Columns []string
	schema  *arrow.Schema
}

func (n *PhysProject) Name() string             { return "PhysProject" }
func (n *PhysProject) Schema() *arrow.Schema    { return n.schema }
func (n *PhysProject) Children() []PhysicalNode { return []PhysicalNode{n.Input} }
func (n *PhysProject) Execute(ctx context.Context, exec Executor) (any, error) {
	in, err := n.Input.Execute(ctx, exec)
	if err != nil {
		return nil, err
	}
	return exec.Project(in, n.Columns)
}

type PhysLimit struct {
	Input  PhysicalNode
	N      int64
	schema *arrow.Schema
}

func (n *PhysLimit) Name() string             { return "PhysLimit" }
func (n *PhysLimit) Schema() *arrow.Schema    { return n.schema }
func (n *PhysLimit) Children() []PhysicalNode { return []PhysicalNode{n.Input} }
func (n *PhysLimit) Execute(ctx context.Context, exec Executor) (any, error) {
	in, err := n.Input.Execute(ctx, exec)
	if err != nil {
		return nil, err
	}
	return exec.Limit(in, n.N)
}

type PhysSort struct {
	Input  PhysicalNode
	Keys   []expr.SortKey
	schema *arrow.Schema
}

func (n *PhysSort) Name() string             { return "PhysSort" }
func (n *PhysSort) Schema() *arrow.Schema    { return n.schema }
func (n *PhysSort) Children() []PhysicalNode { return []PhysicalNode{n.Input} }
func (n *PhysSort) Execute(ctx context.Context, exec Executor) (any, error) {
	in, err := n.Input.Execute(ctx, exec)
	if err != nil {
		return nil, err
	}
	return exec.Sort(ctx, in, n.Keys)
}

type PhysAggregate struct {
	Input     PhysicalNode
	GroupKeys []string
	Aggs      []expr.AggNode
	schema    *arrow.Schema
}

func (n *PhysAggregate) Name() string             { return "PhysAggregate" }
func (n *PhysAggregate) Schema() *arrow.Schema    { return n.schema }
func (n *PhysAggregate) Children() []PhysicalNode { return []PhysicalNode{n.Input} }
func (n *PhysAggregate) Execute(ctx context.Context, exec Executor) (any, error) {
	in, err := n.Input.Execute(ctx, exec)
	if err != nil {
		return nil, err
	}
	return exec.Aggregate(ctx, in, n.GroupKeys, n.Aggs)
}

type PhysWithColumn struct {
	Input  PhysicalNode
	Expr   expr.Expr
	schema *arrow.Schema
}

func (n *PhysWithColumn) Name() string             { return "PhysWithColumn" }
func (n *PhysWithColumn) Schema() *arrow.Schema    { return n.schema }
func (n *PhysWithColumn) Children() []PhysicalNode { return []PhysicalNode{n.Input} }
func (n *PhysWithColumn) Execute(ctx context.Context, exec Executor) (any, error) {
	in, err := n.Input.Execute(ctx, exec)
	if err != nil {
		return nil, err
	}
	return exec.WithColumn(in, n.Expr)
}

type PhysJoin struct {
	Left   PhysicalNode
	Right  PhysicalNode
	On     string
	How    string
	schema *arrow.Schema
}

func (n *PhysJoin) Name() string             { return "PhysJoin" }
func (n *PhysJoin) Schema() *arrow.Schema    { return n.schema }
func (n *PhysJoin) Children() []PhysicalNode { return []PhysicalNode{n.Left, n.Right} }
func (n *PhysJoin) Execute(ctx context.Context, exec Executor) (any, error) {
	left, err := n.Left.Execute(ctx, exec)
	if err != nil {
		return nil, err
	}
	right, err := n.Right.Execute(ctx, exec)
	if err != nil {
		return nil, err
	}
	return exec.Join(ctx, left, right, n.On, n.How)
}
