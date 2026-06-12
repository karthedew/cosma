package plan

import (
	"github.com/karthedew/cosma/expr"
	"github.com/karthedew/cosma/schema"
)

const (
	// ScanSourceDataFrame is an in-memory DataFrame scan. The ScanNode.Handle
	// is the *dataframe.DataFrame to materialize.
	ScanSourceDataFrame = "dataframe"
	// ScanSourceFile is a file-backed scan that the executor opens as a
	// streaming RecordReader rather than materializing the whole file. The
	// ScanNode.Handle carries a file-scan descriptor the executor understands
	// (e.g. path + reader options).
	ScanSourceFile = "file"
)

type LogicalNode interface {
	Name() string
	Schema() *schema.Schema
	Children() []LogicalNode
}

type LogicalPlan struct {
	Root LogicalNode
}

func NewLogicalPlan(root LogicalNode) *LogicalPlan {
	return &LogicalPlan{Root: root}
}

type ScanNode struct {
	schema *schema.Schema
	source string

	// Handle is the opaque source object the executor scans (e.g. a
	// *dataframe.DataFrame). It is typed as any to keep plan free of a
	// dataframe import.
	Handle any

	// Pushdown annotations produced by Optimize. They are advisory: the
	// executor honors whichever it understands and the surviving logical
	// nodes above the scan still produce a correct result if it does not.
	PushedFilters []expr.Expr
	PushedColumns []string
	PushedLimit   int64 // <0 means "no limit pushed"
}

func NewScanNode(schema *schema.Schema, source string) *ScanNode {
	return &ScanNode{schema: schema, source: source, PushedLimit: -1}
}

func (s *ScanNode) Name() string            { return "Scan" }
func (s *ScanNode) Schema() *schema.Schema  { return s.schema }
func (s *ScanNode) Children() []LogicalNode { return nil }
func (s *ScanNode) Source() string          { return s.source }

type FilterNode struct {
	Input     LogicalNode
	Predicate expr.Expr
	schema    *schema.Schema
}

func NewFilterNode(input LogicalNode, predicate expr.Expr) *FilterNode {
	return &FilterNode{Input: input, Predicate: predicate}
}

func (f *FilterNode) Name() string            { return "Filter" }
func (f *FilterNode) Schema() *schema.Schema  { return f.schema }
func (f *FilterNode) Children() []LogicalNode { return []LogicalNode{f.Input} }

type ProjectNode struct {
	Input   LogicalNode
	Columns []string
	schema  *schema.Schema
}

func NewProjectNode(input LogicalNode, columns []string) *ProjectNode {
	return &ProjectNode{Input: input, Columns: append([]string(nil), columns...)}
}

func (p *ProjectNode) Name() string            { return "Project" }
func (p *ProjectNode) Schema() *schema.Schema  { return p.schema }
func (p *ProjectNode) Children() []LogicalNode { return []LogicalNode{p.Input} }

type LimitNode struct {
	Input  LogicalNode
	N      int64
	schema *schema.Schema
}

func NewLimitNode(input LogicalNode, n int64) *LimitNode {
	return &LimitNode{Input: input, N: n}
}

func (l *LimitNode) Name() string            { return "Limit" }
func (l *LimitNode) Schema() *schema.Schema  { return l.schema }
func (l *LimitNode) Children() []LogicalNode { return []LogicalNode{l.Input} }

type WithColumnNode struct {
	Input  LogicalNode
	Expr   expr.Expr
	schema *schema.Schema
}

func NewWithColumnNode(input LogicalNode, e expr.Expr) *WithColumnNode {
	return &WithColumnNode{Input: input, Expr: e}
}

func (w *WithColumnNode) Name() string            { return "WithColumn" }
func (w *WithColumnNode) Schema() *schema.Schema  { return w.schema }
func (w *WithColumnNode) Children() []LogicalNode { return []LogicalNode{w.Input} }

type SortNode struct {
	Input  LogicalNode
	Keys   []expr.SortKey
	schema *schema.Schema
}

func NewSortNode(input LogicalNode, keys []expr.SortKey) *SortNode {
	return &SortNode{Input: input, Keys: append([]expr.SortKey(nil), keys...)}
}

func (s *SortNode) Name() string            { return "Sort" }
func (s *SortNode) Schema() *schema.Schema  { return s.schema }
func (s *SortNode) Children() []LogicalNode { return []LogicalNode{s.Input} }

// AggregateNode is a grouped aggregation. It is named AggregateNode (not
// AggNode) to avoid colliding with expr.AggNode, the column-level aggregate it
// carries in Aggs.
type AggregateNode struct {
	Input     LogicalNode
	GroupKeys []string
	Aggs      []expr.AggNode
	schema    *schema.Schema
}

func NewAggregateNode(input LogicalNode, groupKeys []string, aggs []expr.AggNode) *AggregateNode {
	return &AggregateNode{
		Input:     input,
		GroupKeys: append([]string(nil), groupKeys...),
		Aggs:      append([]expr.AggNode(nil), aggs...),
	}
}

func (a *AggregateNode) Name() string            { return "Aggregate" }
func (a *AggregateNode) Schema() *schema.Schema  { return a.schema }
func (a *AggregateNode) Children() []LogicalNode { return []LogicalNode{a.Input} }

type JoinNode struct {
	Left   LogicalNode
	Right  LogicalNode
	On     string
	How    string
	schema *schema.Schema
}

func NewJoinNode(left, right LogicalNode, on, how string) *JoinNode {
	return &JoinNode{Left: left, Right: right, On: on, How: how}
}

func (j *JoinNode) Name() string            { return "Join" }
func (j *JoinNode) Schema() *schema.Schema  { return j.schema }
func (j *JoinNode) Children() []LogicalNode { return []LogicalNode{j.Left, j.Right} }
