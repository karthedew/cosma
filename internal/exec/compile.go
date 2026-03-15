package exec

import (
	"fmt"

	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"

	"github.com/karthedew/cosma/internal/expr"
	"github.com/karthedew/cosma/operator"
	"github.com/karthedew/cosma/plan"
	"github.com/karthedew/cosma/schema"
)

func Compile(plan *plan.LogicalPlan, source array.RecordReader) (array.RecordReader, []operator.Operator, error) {
	if plan == nil || plan.Root == nil {
		return nil, nil, fmt.Errorf("logical plan is empty")
	}
	if source == nil {
		return nil, nil, fmt.Errorf("source reader is nil")
	}

	src, ops, _, err := compileNode(plan.Root, source)
	if err != nil {
		return nil, nil, err
	}
	if src == nil {
		return nil, nil, fmt.Errorf("compiled source is nil")
	}
	return src, ops, nil
}

func compileNode(node plan.LogicalNode, source array.RecordReader) (array.RecordReader, []operator.Operator, *schema.Schema, error) {
	switch n := node.(type) {
	case *plan.ScanNode:
		if n.Schema() == nil {
			return nil, nil, nil, fmt.Errorf("scan schema is nil")
		}
		return source, nil, n.Schema(), nil
	case *plan.ProjectNode:
		src, ops, currentSchema, err := compileNode(n.Input, source)
		if err != nil {
			return nil, nil, nil, err
		}
		indices, err := resolveIndices(currentSchema, n.Columns)
		if err != nil {
			return nil, nil, nil, err
		}
		arrowSchema, err := arrowSchemaFromCosma(currentSchema)
		if err != nil {
			return nil, nil, nil, err
		}
		proj, err := operator.NewProject(arrowSchema, indices)
		if err != nil {
			return nil, nil, nil, err
		}
		return src, append(ops, proj), n.Schema(), nil
	case *plan.LimitNode:
		src, ops, currentSchema, err := compileNode(n.Input, source)
		if err != nil {
			return nil, nil, nil, err
		}
		arrowSchema, err := arrowSchemaFromCosma(currentSchema)
		if err != nil {
			return nil, nil, nil, err
		}
		lim, err := operator.NewLimit(arrowSchema, n.N)
		if err != nil {
			return nil, nil, nil, err
		}
		return src, append(ops, lim), currentSchema, nil
	case *plan.FilterNode:
		src, ops, currentSchema, err := compileNode(n.Input, source)
		if err != nil {
			return nil, nil, nil, err
		}
		if currentSchema == nil {
			return nil, nil, nil, fmt.Errorf("filter schema is nil")
		}
		predicate, err := expr.BindPredicate(n.Predicate, currentSchema)
		if err != nil {
			return nil, nil, nil, err
		}
		arrowSchema, err := arrowSchemaFromCosma(currentSchema)
		if err != nil {
			return nil, nil, nil, err
		}
		filterOp, err := operator.NewFilter(arrowSchema, predicate.Eval, nil)
		if err != nil {
			return nil, nil, nil, err
		}
		return src, append(ops, filterOp), currentSchema, nil
	default:
		return nil, nil, nil, fmt.Errorf("unsupported logical node %T", node)
	}
}

func resolveIndices(s *schema.Schema, columns []string) ([]int, error) {
	if s == nil {
		return nil, fmt.Errorf("schema is nil")
	}
	indices := make([]int, len(columns))
	for i, name := range columns {
		idx, ok := s.FieldIndex(name)
		if !ok {
			return nil, fmt.Errorf("column %q index not found", name)
		}
		indices[i] = idx
	}
	return indices, nil
}

func arrowSchemaFromCosma(s *schema.Schema) (*arrow.Schema, error) {
	if s == nil {
		return nil, fmt.Errorf("schema is nil")
	}
	fields := s.Fields()
	arrowFields := make([]arrow.Field, len(fields))
	for i, field := range fields {
		if field.ArrowType == nil {
			return nil, fmt.Errorf("schema field %q has nil arrow type", field.Name)
		}
		arrowFields[i] = arrow.Field{
			Name:     field.Name,
			Type:     field.ArrowType,
			Nullable: field.Nullable,
		}
	}
	return arrow.NewSchema(arrowFields, nil), nil
}
