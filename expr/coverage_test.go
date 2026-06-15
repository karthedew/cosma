package expr

import (
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/stretchr/testify/require"

	"github.com/karthedew/cosma/schema"
)

func testSchema() *schema.Schema {
	return schema.New(
		schema.Field{Name: "i8", ArrowType: arrow.PrimitiveTypes.Int8},
		schema.Field{Name: "i16", ArrowType: arrow.PrimitiveTypes.Int16},
		schema.Field{Name: "i32", ArrowType: arrow.PrimitiveTypes.Int32},
		schema.Field{Name: "i64", ArrowType: arrow.PrimitiveTypes.Int64},
		schema.Field{Name: "u32", ArrowType: arrow.PrimitiveTypes.Uint32},
		schema.Field{Name: "u64", ArrowType: arrow.PrimitiveTypes.Uint64},
		schema.Field{Name: "f32", ArrowType: arrow.PrimitiveTypes.Float32},
		schema.Field{Name: "f64", ArrowType: arrow.PrimitiveTypes.Float64},
		schema.Field{Name: "ok", ArrowType: arrow.FixedWidthTypes.Boolean},
		schema.Field{Name: "name", ArrowType: arrow.BinaryTypes.String},
		schema.Field{Name: "bad"},
	)
}

func TestExprStringAndBuilders(t *testing.T) {
	t.Parallel()

	require.Equal(t, "<nil>", Expr{}.String())
	require.Equal(t, "age", Col("age").String())
	require.Equal(t, "\"alice\"", Lit("alice").String())

	typed := []struct {
		name string
		e    Expr
		id   arrow.Type
	}{
		{"int8", Int8(1), arrow.INT8},
		{"int16", Int16(1), arrow.INT16},
		{"int32", Int32(1), arrow.INT32},
		{"int64", Int64(1), arrow.INT64},
		{"uint8", Uint8(1), arrow.UINT8},
		{"uint16", Uint16(1), arrow.UINT16},
		{"uint32", Uint32(1), arrow.UINT32},
		{"uint64", Uint64(1), arrow.UINT64},
		{"float32", Float32(1), arrow.FLOAT32},
		{"float64", Float64(1), arrow.FLOAT64},
	}
	for _, tc := range typed {
		t.Run(tc.name, func(t *testing.T) {
			lit := tc.e.Node.(LiteralNode)
			require.Equal(t, tc.id, lit.Type.ID())
		})
	}

	now := time.Unix(10, 20).UTC()
	lit := Lit(now).Node.(LiteralNode)
	require.Equal(t, arrow.Timestamp(now.UnixNano()), lit.Value)
	require.Equal(t, arrow.TIMESTAMP, lit.Type.ID())
	require.Equal(t, "UTC", lit.Type.(*arrow.TimestampType).TimeZone)

	ts := LitTimestamp(now, "America/New_York").Node.(LiteralNode)
	require.Equal(t, arrow.Timestamp(now.UnixNano()), ts.Value)
	require.Equal(t, "America/New_York", ts.Type.(*arrow.TimestampType).TimeZone)
}

func TestBinaryBuilderMethods(t *testing.T) {
	t.Parallel()

	left := Col("a")
	right := Lit(1)
	cases := []struct {
		name string
		expr Expr
		op   BinaryOp
	}{
		{"eq", left.Eq(right), BinaryOpEq},
		{"neq", left.Neq(right), BinaryOpNeq},
		{"lt", left.Lt(right), BinaryOpLt},
		{"lte", left.Lte(right), BinaryOpLte},
		{"gt", left.Gt(right), BinaryOpGt},
		{"gte", left.Gte(right), BinaryOpGte},
		{"and", left.And(right), BinaryOpAnd},
		{"or", left.Or(right), BinaryOpOr},
		{"add", left.Add(right), BinaryOpAdd},
		{"sub", left.Sub(right), BinaryOpSub},
		{"mul", left.Mul(right), BinaryOpMul},
		{"div", left.Div(right), BinaryOpDiv},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			n := tc.expr.Node.(BinaryNode)
			require.Equal(t, tc.op, n.Op)
			require.Equal(t, left.Node, n.Left)
			require.Equal(t, right.Node, n.Right)
			require.Len(t, n.Children(), 2)
		})
	}
}

func TestUnaryAliasCastAndAggBuilders(t *testing.T) {
	t.Parallel()

	base := Col("a")
	for _, tc := range []struct {
		name string
		expr Expr
		op   UnaryOp
	}{
		{"not", base.Not(), UnaryOpNot},
		{"neg", base.Neg(), UnaryOpNeg},
		{"is_null", base.IsNull(), UnaryOpIsNull},
		{"is_not_null", base.IsNotNull(), UnaryOpIsNotNull},
	} {
		t.Run(tc.name, func(t *testing.T) {
			n := tc.expr.Node.(UnaryNode)
			require.Equal(t, tc.op, n.Op)
			require.Equal(t, base.Node, n.Inner)
			require.Equal(t, []ExprNode{base.Node}, n.Children())
		})
	}

	alias := base.As("renamed").Node.(AliasNode)
	require.Equal(t, "renamed", alias.Name)
	require.Equal(t, "a as renamed", alias.String())
	require.Equal(t, []ExprNode{base.Node}, alias.Children())
	require.Equal(t, "alias(\"empty\")", AliasNode{Name: "empty"}.String())

	cast := base.Cast(arrow.PrimitiveTypes.Float64).Node.(CastNode)
	require.Equal(t, arrow.FLOAT64, cast.Type.ID())
	require.Equal(t, []ExprNode{base.Node}, cast.Children())
	require.Equal(t, "cast(a as float64)", cast.String())

	for _, tc := range []struct {
		name string
		agg  AggNode
		op   AggOp
	}{
		{"count", base.Count(), AggOpCount},
		{"sum", base.Sum(), AggOpSum},
		{"mean", base.Mean(), AggOpMean},
		{"min", base.Min(), AggOpMin},
		{"max", base.Max(), AggOpMax},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.op, tc.agg.Op)
			require.Equal(t, base, tc.agg.Inner)
		})
	}
	require.Equal(t, "sum(a)", base.Sum().String())
	require.Equal(t, "sum(a) as total", base.Sum().As("total").String())
}

func TestOpStringers(t *testing.T) {
	t.Parallel()

	require.Equal(t, []string{"<invalid binary op>", "==", "!=", "<", "<=", ">", ">=", "and", "or", "+", "-", "*", "/"}, []string{
		BinaryOpInvalid.String(), BinaryOpEq.String(), BinaryOpNeq.String(), BinaryOpLt.String(), BinaryOpLte.String(), BinaryOpGt.String(), BinaryOpGte.String(), BinaryOpAnd.String(), BinaryOpOr.String(), BinaryOpAdd.String(), BinaryOpSub.String(), BinaryOpMul.String(), BinaryOpDiv.String(),
	})
	require.Equal(t, []string{"<invalid unary op>", "not", "-", "is_null", "is_not_null"}, []string{
		UnaryOpInvalid.String(), UnaryOpNot.String(), UnaryOpNeg.String(), UnaryOpIsNull.String(), UnaryOpIsNotNull.String(),
	})
	require.Equal(t, []string{"<invalid agg op>", "count", "sum", "mean", "min", "max"}, []string{
		AggOpInvalid.String(), AggOpCount.String(), AggOpSum.String(), AggOpMean.String(), AggOpMin.String(), AggOpMax.String(),
	})
}

func TestNodeDataTypes(t *testing.T) {
	t.Parallel()

	s := testSchema()
	colType, err := ColumnNode{Name: "i32"}.DataType(s)
	require.NoError(t, err)
	require.Equal(t, arrow.INT32, colType.ID())

	_, err = ColumnNode{Name: "i32"}.DataType(nil)
	require.ErrorContains(t, err, "schema is nil")
	_, err = ColumnNode{Name: "missing"}.DataType(s)
	require.ErrorContains(t, err, "not in schema")
	_, err = ColumnNode{Name: "bad"}.DataType(s)
	require.ErrorContains(t, err, "nil arrow type")

	for _, tc := range []struct {
		name string
		node ExprNode
		id   arrow.Type
	}{
		{"compare", Col("i32").Gt(Col("i64")).Node, arrow.BOOL},
		{"string compare", Col("name").Eq(Lit("bob")).Node, arrow.BOOL},
		{"logical", Col("ok").And(Col("ok")).Node, arrow.BOOL},
		{"add", Col("i32").Add(Col("i64")).Node, arrow.INT64},
		{"mixed signed", Col("i32").Add(Col("u32")).Node, arrow.INT64},
		{"unsigned", Col("u32").Add(Col("u64")).Node, arrow.UINT64},
		{"float32", Col("f32").Add(Col("f32")).Node, arrow.FLOAT32},
		{"float64", Col("f32").Add(Col("i32")).Node, arrow.FLOAT64},
		{"div", Col("i32").Div(Col("i32")).Node, arrow.FLOAT64},
		{"not", Col("ok").Not().Node, arrow.BOOL},
		{"is null", Col("i32").IsNull().Node, arrow.BOOL},
		{"is not null", Col("i32").IsNotNull().Node, arrow.BOOL},
		{"neg", Col("i32").Neg().Node, arrow.INT32},
		{"alias", Col("i32").As("x").Node, arrow.INT32},
		{"cast", Col("i32").Cast(arrow.PrimitiveTypes.Float64).Node, arrow.FLOAT64},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dt, err := tc.node.DataType(s)
			require.NoError(t, err)
			require.Equal(t, tc.id, dt.ID())
		})
	}

	for _, tc := range []struct {
		name string
		node ExprNode
		want string
	}{
		{"binary nil", BinaryNode{Op: BinaryOpEq, Left: Col("i32").Node}, "binary node has nil operand"},
		{"compare mismatch", Col("i32").Eq(Col("name")).Node, "cannot compare"},
		{"logical non bool", Col("i32").And(Col("ok")).Node, "requires bool"},
		{"arith non numeric", Col("name").Add(Col("i32")).Node, "cannot promote non-numeric"},
		{"binary invalid", BinaryNode{Op: BinaryOpInvalid, Left: Col("i32").Node, Right: Col("i32").Node}, "unsupported binary op"},
		{"unary nil", UnaryNode{Op: UnaryOpNot}, "unary node has nil operand"},
		{"not non bool", Col("i32").Not().Node, "not requires bool"},
		{"neg non numeric", Col("name").Neg().Node, "neg requires numeric"},
		{"unary invalid", UnaryNode{Op: UnaryOpInvalid, Inner: Col("i32").Node}, "unsupported unary op"},
		{"alias empty", AliasNode{Name: " ", Inner: Col("i32").Node}, "alias name is empty"},
		{"alias nil", AliasNode{Name: "x"}, "nil inner"},
		{"cast nil type", CastNode{Inner: Col("i32").Node}, "cast target type is nil"},
		{"cast nil inner", CastNode{Type: arrow.PrimitiveTypes.Int32}, "cast has nil inner"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := tc.node.DataType(s)
			require.ErrorContains(t, err, tc.want)
		})
	}
}

func TestLiteralInferenceAndHelpers(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		v  any
		id arrow.Type
	}{
		{true, arrow.BOOL}, {"x", arrow.STRING}, {int8(1), arrow.INT8}, {int16(1), arrow.INT16}, {int32(1), arrow.INT32}, {int(1), arrow.INT64}, {int64(1), arrow.INT64},
		{uint8(1), arrow.UINT8}, {uint16(1), arrow.UINT16}, {uint32(1), arrow.UINT32}, {uint(1), arrow.UINT64}, {uint64(1), arrow.UINT64}, {float32(1), arrow.FLOAT32}, {float64(1), arrow.FLOAT64},
		{time.Unix(0, 0), arrow.TIMESTAMP}, {arrow.Timestamp(1), arrow.TIMESTAMP},
	} {
		dt, err := inferLiteralType(tc.v)
		require.NoError(t, err)
		require.Equal(t, tc.id, dt.ID())
	}
	_, err := inferLiteralType(nil)
	require.ErrorContains(t, err, "literal value is nil")
	_, err = inferLiteralType(struct{}{})
	require.ErrorContains(t, err, "unsupported literal type")

	require.True(t, isNumeric(arrow.PrimitiveTypes.Int8))
	require.True(t, isNumeric(arrow.PrimitiveTypes.Uint64))
	require.True(t, isNumeric(arrow.PrimitiveTypes.Float64))
	require.False(t, isNumeric(nil))
	require.False(t, isNumeric(arrow.BinaryTypes.String))
	require.True(t, isFloat(arrow.PrimitiveTypes.Float32))
	require.False(t, isFloat(arrow.PrimitiveTypes.Int32))
	require.True(t, isSignedInt(arrow.PrimitiveTypes.Int64))
	require.False(t, isSignedInt(arrow.PrimitiveTypes.Uint64))
	require.True(t, isUnsignedInt(arrow.PrimitiveTypes.Uint8))
	require.False(t, isUnsignedInt(arrow.PrimitiveTypes.Int8))
	require.True(t, isBool(arrow.FixedWidthTypes.Boolean))
	require.False(t, isBool(nil))
	require.True(t, comparable(arrow.PrimitiveTypes.Int8, arrow.PrimitiveTypes.Uint64))
	require.True(t, comparable(arrow.BinaryTypes.String, arrow.BinaryTypes.String))
	require.False(t, comparable(nil, arrow.BinaryTypes.String))
	require.False(t, comparable(arrow.BinaryTypes.String, arrow.FixedWidthTypes.Boolean))
	require.Equal(t, "<nil>", typeName(nil))
}

func TestNumericPromotion(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		l, r arrow.DataType
		id   arrow.Type
	}{
		{"signed rank 1", arrow.PrimitiveTypes.Int8, arrow.PrimitiveTypes.Int8, arrow.INT8},
		{"signed rank 2", arrow.PrimitiveTypes.Int8, arrow.PrimitiveTypes.Int16, arrow.INT16},
		{"signed rank 3", arrow.PrimitiveTypes.Int16, arrow.PrimitiveTypes.Int32, arrow.INT32},
		{"signed rank 4", arrow.PrimitiveTypes.Int32, arrow.PrimitiveTypes.Int64, arrow.INT64},
		{"unsigned rank 1", arrow.PrimitiveTypes.Uint8, arrow.PrimitiveTypes.Uint8, arrow.UINT8},
		{"unsigned rank 2", arrow.PrimitiveTypes.Uint8, arrow.PrimitiveTypes.Uint16, arrow.UINT16},
		{"unsigned rank 3", arrow.PrimitiveTypes.Uint16, arrow.PrimitiveTypes.Uint32, arrow.UINT32},
		{"unsigned rank 4", arrow.PrimitiveTypes.Uint32, arrow.PrimitiveTypes.Uint64, arrow.UINT64},
		{"mixed signedness", arrow.PrimitiveTypes.Int8, arrow.PrimitiveTypes.Uint8, arrow.INT64},
		{"float32 pair", arrow.PrimitiveTypes.Float32, arrow.PrimitiveTypes.Float32, arrow.FLOAT32},
		{"float64 pair", arrow.PrimitiveTypes.Float64, arrow.PrimitiveTypes.Float32, arrow.FLOAT64},
		{"float int", arrow.PrimitiveTypes.Float32, arrow.PrimitiveTypes.Int32, arrow.FLOAT64},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dt, err := promoteNumeric(tc.l, tc.r)
			require.NoError(t, err)
			require.Equal(t, tc.id, dt.ID())
		})
	}
	_, ok := numericRank(nil)
	require.False(t, ok)
	_, ok = numericRank(arrow.BinaryTypes.String)
	require.False(t, ok)
	_, err := promoteNumeric(arrow.BinaryTypes.String, arrow.PrimitiveTypes.Int8)
	require.ErrorContains(t, err, "cannot promote non-numeric")
}

func TestWalkAndRewrite(t *testing.T) {
	t.Parallel()

	tree := Col("a").Add(Lit(1)).As("x").Cast(arrow.PrimitiveTypes.Int64).Node
	var visited []string
	err := Walk(tree, func(n ExprNode) error {
		visited = append(visited, reflect.TypeOf(n).Name())
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, []string{"ColumnNode", "LiteralNode", "BinaryNode", "AliasNode", "CastNode"}, visited)
	require.NoError(t, Walk(nil, func(ExprNode) error { return errors.New("unexpected") }))
	require.ErrorContains(t, Walk(tree, func(n ExprNode) error {
		if _, ok := n.(BinaryNode); ok {
			return errors.New("stop")
		}
		return nil
	}), "stop")

	rewritten := Rewrite(tree, func(n ExprNode) ExprNode {
		if c, ok := n.(ColumnNode); ok && c.Name == "a" {
			return ColumnNode{Name: "b"}
		}
		return n
	})
	require.Contains(t, rewritten.String(), "b")
	require.Nil(t, Rewrite(nil, func(n ExprNode) ExprNode { return n }))

	require.Panics(t, func() {
		withChildren(customNode{}, []ExprNode{ColumnNode{Name: "x"}})
	})
}

type customNode struct{}

func (customNode) String() string       { return "custom" }
func (customNode) Children() []ExprNode { return []ExprNode{ColumnNode{Name: "x"}} }
func (customNode) DataType(*schema.Schema) (arrow.DataType, error) {
	return arrow.PrimitiveTypes.Int64, nil
}
