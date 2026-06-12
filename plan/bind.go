package plan

import (
	"fmt"

	"github.com/karthedew/cosma/expr"
	"github.com/karthedew/cosma/schema"
)

func Bind(plan *LogicalPlan) (*LogicalPlan, error) {
	if plan == nil || plan.Root == nil {
		return nil, fmt.Errorf("logical plan is empty")
	}

	root, err := bindNode(plan.Root)
	if err != nil {
		return nil, err
	}

	return NewLogicalPlan(root), nil
}

func bindNode(node LogicalNode) (LogicalNode, error) {
	if node == nil {
		return nil, fmt.Errorf("logical node is nil")
	}

	switch n := node.(type) {
	case *ScanNode:
		if n.schema == nil {
			return nil, fmt.Errorf("scan schema is nil")
		}
		return &ScanNode{
			schema:        n.schema,
			source:        n.source,
			Handle:        n.Handle,
			PushedFilters: n.PushedFilters,
			PushedColumns: n.PushedColumns,
			PushedLimit:   n.PushedLimit,
		}, nil
	case *FilterNode:
		if n.Input == nil {
			return nil, fmt.Errorf("filter input is nil")
		}
		if n.Predicate.Node == nil {
			return nil, fmt.Errorf("filter predicate is nil")
		}
		input, err := bindNode(n.Input)
		if err != nil {
			return nil, err
		}
		s := input.Schema()
		if s == nil {
			return nil, fmt.Errorf("filter input schema is nil")
		}
		cols := collectColumns(n.Predicate)
		for _, name := range cols {
			if _, ok := s.Field(name); !ok {
				return nil, fmt.Errorf("filter column %q not in schema", name)
			}
		}
		return &FilterNode{Input: input, Predicate: n.Predicate, schema: s}, nil
	case *ProjectNode:
		if n.Input == nil {
			return nil, fmt.Errorf("project input is nil")
		}
		if len(n.Columns) == 0 {
			return nil, fmt.Errorf("project columns are empty")
		}
		input, err := bindNode(n.Input)
		if err != nil {
			return nil, err
		}
		s := input.Schema()
		if s == nil {
			return nil, fmt.Errorf("project input schema is nil")
		}
		fields := make([]schema.Field, len(n.Columns))
		seen := make(map[string]struct{}, len(n.Columns))
		for i, name := range n.Columns {
			if name == "" {
				return nil, fmt.Errorf("project column name is empty")
			}
			if _, ok := seen[name]; ok {
				return nil, fmt.Errorf("duplicate project column %q", name)
			}
			field, ok := s.Field(name)
			if !ok {
				return nil, fmt.Errorf("project column %q not in schema", name)
			}
			seen[name] = struct{}{}
			fields[i] = field
		}
		projSchema := schema.New(fields...)
		return &ProjectNode{Input: input, Columns: append([]string(nil), n.Columns...), schema: projSchema}, nil
	case *LimitNode:
		if n.Input == nil {
			return nil, fmt.Errorf("limit input is nil")
		}
		if n.N < 0 {
			return nil, fmt.Errorf("limit must be >= 0")
		}
		input, err := bindNode(n.Input)
		if err != nil {
			return nil, err
		}
		s := input.Schema()
		if s == nil {
			return nil, fmt.Errorf("limit input schema is nil")
		}
		return &LimitNode{Input: input, N: n.N, schema: s}, nil
	case *WithColumnNode:
		if n.Input == nil {
			return nil, fmt.Errorf("withcolumn input is nil")
		}
		if n.Expr.Node == nil {
			return nil, fmt.Errorf("withcolumn expression is nil")
		}
		input, err := bindNode(n.Input)
		if err != nil {
			return nil, err
		}
		s := input.Schema()
		if s == nil {
			return nil, fmt.Errorf("withcolumn input schema is nil")
		}
		// The output schema (which column is added/replaced and its dtype) is
		// determined at execution; carry the input schema for explain/lowering.
		return &WithColumnNode{Input: input, Expr: n.Expr, schema: s}, nil
	case *SortNode:
		if n.Input == nil {
			return nil, fmt.Errorf("sort input is nil")
		}
		if len(n.Keys) == 0 {
			return nil, fmt.Errorf("sort keys are empty")
		}
		input, err := bindNode(n.Input)
		if err != nil {
			return nil, err
		}
		s := input.Schema()
		if s == nil {
			return nil, fmt.Errorf("sort input schema is nil")
		}
		for _, k := range n.Keys {
			if _, ok := s.Field(k.Column); !ok {
				return nil, fmt.Errorf("sort column %q not in schema", k.Column)
			}
		}
		return &SortNode{Input: input, Keys: append([]expr.SortKey(nil), n.Keys...), schema: s}, nil
	case *AggregateNode:
		if n.Input == nil {
			return nil, fmt.Errorf("aggregate input is nil")
		}
		if len(n.Aggs) == 0 {
			return nil, fmt.Errorf("aggregate aggs are empty")
		}
		input, err := bindNode(n.Input)
		if err != nil {
			return nil, err
		}
		s := input.Schema()
		if s == nil {
			return nil, fmt.Errorf("aggregate input schema is nil")
		}
		for _, k := range n.GroupKeys {
			if _, ok := s.Field(k); !ok {
				return nil, fmt.Errorf("aggregate group key %q not in schema", k)
			}
		}
		// Output schema is determined at execution; carry input schema.
		return &AggregateNode{
			Input:     input,
			GroupKeys: append([]string(nil), n.GroupKeys...),
			Aggs:      append([]expr.AggNode(nil), n.Aggs...),
			schema:    s,
		}, nil
	case *JoinNode:
		if n.Left == nil || n.Right == nil {
			return nil, fmt.Errorf("join input is nil")
		}
		if n.On == "" {
			return nil, fmt.Errorf("join key is empty")
		}
		switch n.How {
		case "inner", "left":
		default:
			return nil, fmt.Errorf("join: unsupported how %q", n.How)
		}
		left, err := bindNode(n.Left)
		if err != nil {
			return nil, err
		}
		right, err := bindNode(n.Right)
		if err != nil {
			return nil, err
		}
		ls := left.Schema()
		rs := right.Schema()
		if ls == nil || rs == nil {
			return nil, fmt.Errorf("join input schema is nil")
		}
		if _, ok := ls.Field(n.On); !ok {
			return nil, fmt.Errorf("join key %q not in left schema", n.On)
		}
		if _, ok := rs.Field(n.On); !ok {
			return nil, fmt.Errorf("join key %q not in right schema", n.On)
		}
		// Output schema is determined at execution; carry left schema.
		return &JoinNode{Left: left, Right: right, On: n.On, How: n.How, schema: ls}, nil
	default:
		return nil, fmt.Errorf("unsupported logical node %T", node)
	}
}

func collectColumns(e expr.Expr) []string {
	seen := make(map[string]struct{})
	_ = expr.Walk(e.Node, func(node expr.ExprNode) error {
		if c, ok := node.(expr.ColumnNode); ok {
			seen[c.Name] = struct{}{}
		}
		return nil
	})

	out := make([]string, 0, len(seen))
	for name := range seen {
		out = append(out, name)
	}
	return out
}
