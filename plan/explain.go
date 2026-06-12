package plan

import (
	"fmt"
	"strings"
)

func ExplainLogical(plan *LogicalPlan) string {
	if plan == nil || plan.Root == nil {
		return "<empty plan>"
	}

	var b strings.Builder
	explainLogicalNode(&b, plan.Root, 0)
	return b.String()
}

func explainLogicalNode(b *strings.Builder, node LogicalNode, depth int) {
	if node == nil {
		return
	}
	for i := 0; i < depth; i++ {
		b.WriteString("  ")
	}
	name := node.Name()
	if detail := describeNode(node); detail != "" {
		name = fmt.Sprintf("%s(%s)", name, detail)
	}
	b.WriteString(name)
	b.WriteString("\n")
	for _, child := range node.Children() {
		explainLogicalNode(b, child, depth+1)
	}
}

func describeNode(node LogicalNode) string {
	switch n := node.(type) {
	case *ScanNode:
		if n.source != "" {
			return fmt.Sprintf("source=%s", n.source)
		}
	case *FilterNode:
		if n.Predicate.Node != nil {
			return fmt.Sprintf("predicate=%s", n.Predicate)
		}
	case *ProjectNode:
		if len(n.Columns) > 0 {
			return fmt.Sprintf("columns=%v", n.Columns)
		}
	case *LimitNode:
		return fmt.Sprintf("n=%d", n.N)
	case *SortNode:
		return fmt.Sprintf("keys=%v", n.Keys)
	case *AggregateNode:
		return fmt.Sprintf("groupKeys=%v aggs=%v", n.GroupKeys, n.Aggs)
	case *WithColumnNode:
		return fmt.Sprintf("expr=%s", n.Expr)
	case *JoinNode:
		return fmt.Sprintf("on=%s how=%s", n.On, n.How)
	}
	return ""
}

// Explain renders the physical plan tree with operator names and any scan-level
// pushdown annotations recorded by the optimizer.
func (p *PhysicalPlan) Explain() string {
	if p == nil || p.Root == nil {
		return "<empty plan>"
	}
	var b strings.Builder
	explainPhysicalNode(&b, p.Root, 0)
	return b.String()
}

func explainPhysicalNode(b *strings.Builder, node PhysicalNode, depth int) {
	if node == nil {
		return
	}
	for i := 0; i < depth; i++ {
		b.WriteString("  ")
	}
	name := node.Name()
	if detail := describePhysicalNode(node); detail != "" {
		name = fmt.Sprintf("%s(%s)", name, detail)
	}
	b.WriteString(name)
	b.WriteString("\n")
	for _, child := range node.Children() {
		explainPhysicalNode(b, child, depth+1)
	}
}

func describePhysicalNode(node PhysicalNode) string {
	switch n := node.(type) {
	case *PhysScan:
		parts := []string{fmt.Sprintf("source=%s", n.Node.source)}
		if len(n.Node.PushedColumns) > 0 {
			parts = append(parts, fmt.Sprintf("PushedColumns=%v", n.Node.PushedColumns))
		}
		if len(n.Node.PushedFilters) > 0 {
			parts = append(parts, fmt.Sprintf("PushedFilters=%d", len(n.Node.PushedFilters)))
		}
		if n.Node.PushedLimit >= 0 {
			parts = append(parts, fmt.Sprintf("PushedLimit=%d (limit pushed)", n.Node.PushedLimit))
		}
		return strings.Join(parts, " ")
	case *PhysFilter:
		return fmt.Sprintf("predicate=%s", n.Predicate)
	case *PhysProject:
		return fmt.Sprintf("columns=%v", n.Columns)
	case *PhysLimit:
		return fmt.Sprintf("n=%d", n.N)
	case *PhysSort:
		return fmt.Sprintf("keys=%v", n.Keys)
	case *PhysAggregate:
		return fmt.Sprintf("groupKeys=%v aggs=%v", n.GroupKeys, n.Aggs)
	case *PhysWithColumn:
		return fmt.Sprintf("expr=%s", n.Expr)
	case *PhysJoin:
		return fmt.Sprintf("on=%s how=%s", n.On, n.How)
	}
	return ""
}
