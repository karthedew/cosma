package plan

import (
	"fmt"

	"github.com/karthedew/cosma/expr"
)

// Optimize runs the logical optimizer passes and returns a new logical plan.
// The passes annotate the ScanNode with pushdown hints (PushedFilters,
// PushedColumns, PushedLimit) that the physical executor may honor during scan.
//
// The annotations are advisory: the operator nodes above the scan (Filter,
// Project, Limit) are preserved, so the plan stays correct whether or not the
// executor acts on a hint. Today the executor honors PushedLimit during scan;
// the filter and column hints are recorded for Explain and future scan sources
// that can prune at the source (e.g. Parquet column/row-group pruning).
func Optimize(p *LogicalPlan) (*LogicalPlan, error) {
	if p == nil || p.Root == nil {
		return nil, fmt.Errorf("logical plan is empty")
	}
	root := cloneNode(p.Root)
	pushFilters(root)
	pushColumns(root)
	pushLimit(root)
	return NewLogicalPlan(root), nil
}

// scanBelow walks the single-input chain below node and returns the ScanNode at
// its base, or nil if the chain branches (e.g. a Join) before reaching a scan.
func scanBelow(node LogicalNode) *ScanNode {
	for {
		switch n := node.(type) {
		case *ScanNode:
			return n
		case *FilterNode:
			node = n.Input
		case *ProjectNode:
			node = n.Input
		case *LimitNode:
			node = n.Input
		case *SortNode:
			node = n.Input
		case *WithColumnNode:
			node = n.Input
		default:
			return nil
		}
	}
}

// pushFilters records every FilterNode predicate that sits in a linear chain
// above a scan onto that scan's PushedFilters.
func pushFilters(node LogicalNode) {
	walk(node, func(n LogicalNode) {
		if f, ok := n.(*FilterNode); ok {
			if s := scanBelow(f); s != nil {
				s.PushedFilters = append(s.PushedFilters, f.Predicate)
			}
		}
	})
}

// pushColumns records the narrowest ProjectNode column set above each scan.
func pushColumns(node LogicalNode) {
	walk(node, func(n LogicalNode) {
		if p, ok := n.(*ProjectNode); ok {
			if s := scanBelow(p); s != nil && s.PushedColumns == nil {
				s.PushedColumns = append([]string(nil), p.Columns...)
			}
		}
	})
}

// limitScanBelow walks the chain below a limit and returns the ScanNode at its
// base, traversing only operators that preserve both row count and row order
// (Project, WithColumn). Unlike scanBelow it must stop at Filter and Sort: a
// limit hard-applied at the scan before a filter drops the wrong rows, and
// before a sort keeps the wrong rows. The executor honors PushedLimit by
// truncating during the scan, so pushing across those nodes is unsound, not
// merely unhelpful.
func limitScanBelow(node LogicalNode) *ScanNode {
	for {
		switch n := node.(type) {
		case *ScanNode:
			return n
		case *ProjectNode:
			node = n.Input
		case *WithColumnNode:
			node = n.Input
		case *LimitNode:
			// A nested limit keeps a prefix too; the min logic in pushLimit
			// makes pushing across it sound.
			node = n.Input
		default:
			return nil
		}
	}
}

// pushLimit records the smallest LimitNode value above each scan, looking
// through row-preserving operators only (see limitScanBelow).
func pushLimit(node LogicalNode) {
	walk(node, func(n LogicalNode) {
		if l, ok := n.(*LimitNode); ok {
			if s := limitScanBelow(l); s != nil {
				if s.PushedLimit < 0 || l.N < s.PushedLimit {
					s.PushedLimit = l.N
				}
			}
		}
	})
}

func walk(node LogicalNode, fn func(LogicalNode)) {
	if node == nil {
		return
	}
	fn(node)
	for _, c := range node.Children() {
		walk(c, fn)
	}
}

// cloneNode deep-copies the logical tree so Optimize does not mutate the input
// plan's ScanNode annotations.
func cloneNode(node LogicalNode) LogicalNode {
	switch n := node.(type) {
	case *ScanNode:
		return &ScanNode{
			schema:        n.schema,
			source:        n.source,
			Handle:        n.Handle,
			PushedFilters: append([]expr.Expr(nil), n.PushedFilters...),
			PushedColumns: append([]string(nil), n.PushedColumns...),
			PushedLimit:   n.PushedLimit,
		}
	case *FilterNode:
		return &FilterNode{Input: cloneNode(n.Input), Predicate: n.Predicate, schema: n.schema}
	case *ProjectNode:
		return &ProjectNode{Input: cloneNode(n.Input), Columns: append([]string(nil), n.Columns...), schema: n.schema}
	case *LimitNode:
		return &LimitNode{Input: cloneNode(n.Input), N: n.N, schema: n.schema}
	case *SortNode:
		return &SortNode{Input: cloneNode(n.Input), Keys: append([]expr.SortKey(nil), n.Keys...), schema: n.schema}
	case *AggregateNode:
		return &AggregateNode{
			Input:     cloneNode(n.Input),
			GroupKeys: append([]string(nil), n.GroupKeys...),
			Aggs:      append([]expr.AggNode(nil), n.Aggs...),
			schema:    n.schema,
		}
	case *WithColumnNode:
		return &WithColumnNode{Input: cloneNode(n.Input), Expr: n.Expr, schema: n.schema}
	case *JoinNode:
		return &JoinNode{Left: cloneNode(n.Left), Right: cloneNode(n.Right), On: n.On, How: n.How, schema: n.schema}
	default:
		return node
	}
}
