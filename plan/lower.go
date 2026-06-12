package plan

import "fmt"

// Lower translates an (optimized) logical plan into an executable physical plan.
func Lower(p *LogicalPlan) (*PhysicalPlan, error) {
	if p == nil || p.Root == nil {
		return nil, fmt.Errorf("logical plan is empty")
	}
	root, err := lowerNode(p.Root)
	if err != nil {
		return nil, err
	}
	return NewPhysicalPlan(root), nil
}

func lowerNode(node LogicalNode) (PhysicalNode, error) {
	switch n := node.(type) {
	case *ScanNode:
		return &PhysScan{Node: n, schema: arrowSchema(n.schema)}, nil
	case *FilterNode:
		in, err := lowerNode(n.Input)
		if err != nil {
			return nil, err
		}
		return &PhysFilter{Input: in, Predicate: n.Predicate, schema: arrowSchema(n.schema)}, nil
	case *ProjectNode:
		in, err := lowerNode(n.Input)
		if err != nil {
			return nil, err
		}
		return &PhysProject{Input: in, Columns: n.Columns, schema: arrowSchema(n.schema)}, nil
	case *LimitNode:
		in, err := lowerNode(n.Input)
		if err != nil {
			return nil, err
		}
		return &PhysLimit{Input: in, N: n.N, schema: arrowSchema(n.schema)}, nil
	case *SortNode:
		in, err := lowerNode(n.Input)
		if err != nil {
			return nil, err
		}
		return &PhysSort{Input: in, Keys: n.Keys, schema: arrowSchema(n.schema)}, nil
	case *AggregateNode:
		in, err := lowerNode(n.Input)
		if err != nil {
			return nil, err
		}
		return &PhysAggregate{Input: in, GroupKeys: n.GroupKeys, Aggs: n.Aggs, schema: arrowSchema(n.schema)}, nil
	case *WithColumnNode:
		in, err := lowerNode(n.Input)
		if err != nil {
			return nil, err
		}
		return &PhysWithColumn{Input: in, Expr: n.Expr, schema: arrowSchema(n.schema)}, nil
	case *JoinNode:
		left, err := lowerNode(n.Left)
		if err != nil {
			return nil, err
		}
		right, err := lowerNode(n.Right)
		if err != nil {
			return nil, err
		}
		return &PhysJoin{Left: left, Right: right, On: n.On, How: n.How, schema: arrowSchema(n.schema)}, nil
	default:
		return nil, fmt.Errorf("cannot lower logical node %T", node)
	}
}
