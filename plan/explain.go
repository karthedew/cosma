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
		if n.Predicate != nil {
			return fmt.Sprintf("predicate=%s", n.Predicate)
		}
	case *ProjectNode:
		if len(n.Columns) > 0 {
			return fmt.Sprintf("columns=%v", n.Columns)
		}
	case *LimitNode:
		return fmt.Sprintf("n=%d", n.N)
	}
	return ""
}
