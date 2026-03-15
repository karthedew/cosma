package plan

import (
	"fmt"

	"github.com/karthedew/cosma/internal/expr"
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
		return &ScanNode{schema: n.schema, source: n.source}, nil
	case *FilterNode:
		if n.Input == nil {
			return nil, fmt.Errorf("filter input is nil")
		}
		if n.Predicate == nil {
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
	default:
		return nil, fmt.Errorf("unsupported logical node %T", node)
	}
}

func collectColumns(e expr.Expr) []string {
	seen := make(map[string]struct{})
	var walk func(expr.Expr)
	walk = func(node expr.Expr) {
		switch v := node.(type) {
		case nil:
			return
		case expr.Col:
			seen[v.Name] = struct{}{}
		case *expr.Col:
			if v != nil {
				seen[v.Name] = struct{}{}
			}
		case expr.Lit, *expr.Lit:
			return
		case expr.Eq:
			walk(v.Left)
			walk(v.Right)
		case *expr.Eq:
			if v != nil {
				walk(v.Left)
				walk(v.Right)
			}
		case expr.Gt:
			walk(v.Left)
			walk(v.Right)
		case *expr.Gt:
			if v != nil {
				walk(v.Left)
				walk(v.Right)
			}
		}
	}
	walk(e)

	out := make([]string, 0, len(seen))
	for name := range seen {
		out = append(out, name)
	}
	return out
}
