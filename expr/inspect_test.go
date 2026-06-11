package expr_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/karthedew/cosma/expr"
)

func TestBinaryNodeInspectable(t *testing.T) {
	t.Parallel()

	e := expr.Col("age").Gt(expr.Lit(int64(30)))
	bn, ok := e.Node.(expr.BinaryNode)
	require.True(t, ok, "root node should be a BinaryNode")
	require.Equal(t, expr.BinaryOpGt, bn.Op)

	left, ok := bn.Left.(expr.ColumnNode)
	require.True(t, ok)
	require.Equal(t, "age", left.Name)

	right, ok := bn.Right.(expr.LiteralNode)
	require.True(t, ok)
	require.Equal(t, int64(30), right.Value)
}

func TestAggNodeIsConcreteType(t *testing.T) {
	t.Parallel()

	agg := expr.Col("salary").Sum()
	require.Equal(t, expr.AggOpSum, agg.Op)

	col, ok := agg.Inner.Node.(expr.ColumnNode)
	require.True(t, ok)
	require.Equal(t, "salary", col.Name)
}
