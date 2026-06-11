package expr

import "fmt"

// AggNode is a column-level aggregation used by GroupBy().Agg. It is a concrete
// public struct rather than an ExprNode: Agg accepts only AggNode values, so
// passing a non-aggregate expression is a compile-time error. Op is the
// reduction, Inner is the expression being reduced, and Alias is the output
// column name (empty means "derive a default").
type AggNode struct {
	Op    AggOp
	Inner Expr
	Alias string
}

// As sets the output column name for the aggregation.
func (a AggNode) As(name string) AggNode { a.Alias = name; return a }

func (a AggNode) String() string {
	if a.Alias != "" {
		return fmt.Sprintf("%s(%s) as %s", a.Op, a.Inner, a.Alias)
	}
	return fmt.Sprintf("%s(%s)", a.Op, a.Inner)
}
