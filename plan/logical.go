package plan

import (
	"github.com/karthedew/cosma/internal/expr"
	"github.com/karthedew/cosma/schema"
)

const (
	ScanSourceDataFrame = "dataframe"
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
}

func NewScanNode(schema *schema.Schema, source string) *ScanNode {
	return &ScanNode{schema: schema, source: source}
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
