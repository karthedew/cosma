package expr

import (
	"github.com/apache/arrow/go/v18/arrow"
)

// ExprBuilder is a thin fluent wrapper around an Expr. It exists purely for
// ergonomics — every method returns a new ExprBuilder whose underlying tree
// has the operation applied, so chains compose left-to-right. Methods that
// take a right-hand side accept any of:
//
//   - ExprBuilder (most common, when chaining)
//   - Expr (lower-level construction)
//   - a Go scalar, which is wrapped as a LiteralNode with an inferred type
//
// Errors from literal type inference are surfaced later by DataType() during
// binding rather than from the chain itself, so the fluent API never returns
// (ExprBuilder, error).
type ExprBuilder struct {
	expr Expr
}

func (b ExprBuilder) Build() Expr { return b.expr }
func (b ExprBuilder) String() string {
	if b.expr == nil {
		return "<nil>"
	}
	return b.expr.String()
}

func Col(name string) ExprBuilder {
	return ExprBuilder{expr: ColumnNode{Name: name}}
}

// Lit wraps a Go scalar as a LiteralNode. The Arrow type is inferred from
// the value (untyped ints → int64, untyped floats → float64). Use the typed
// constructors below when you need a specific Arrow width.
func Lit(v any) ExprBuilder {
	t, _ := inferLiteralType(v)
	return ExprBuilder{expr: LiteralNode{Value: v, Type: t}}
}

func Int8(v int8) ExprBuilder       { return typedLit(v, arrow.PrimitiveTypes.Int8) }
func Int16(v int16) ExprBuilder     { return typedLit(v, arrow.PrimitiveTypes.Int16) }
func Int32(v int32) ExprBuilder     { return typedLit(v, arrow.PrimitiveTypes.Int32) }
func Int64(v int64) ExprBuilder     { return typedLit(v, arrow.PrimitiveTypes.Int64) }
func Uint8(v uint8) ExprBuilder     { return typedLit(v, arrow.PrimitiveTypes.Uint8) }
func Uint16(v uint16) ExprBuilder   { return typedLit(v, arrow.PrimitiveTypes.Uint16) }
func Uint32(v uint32) ExprBuilder   { return typedLit(v, arrow.PrimitiveTypes.Uint32) }
func Uint64(v uint64) ExprBuilder   { return typedLit(v, arrow.PrimitiveTypes.Uint64) }
func Float32(v float32) ExprBuilder { return typedLit(v, arrow.PrimitiveTypes.Float32) }
func Float64(v float64) ExprBuilder { return typedLit(v, arrow.PrimitiveTypes.Float64) }

func typedLit(v any, t arrow.DataType) ExprBuilder {
	return ExprBuilder{expr: LiteralNode{Value: v, Type: t}}
}

// toExpr coerces a method argument into an Expr. ExprBuilder unwraps to its
// tree, an Expr passes through, and any other value is treated as a literal
// scalar with inferred type. Inference failures land as a LiteralNode with a
// nil Type, which DataType() will reject during binding.
func toExpr(v any) Expr {
	switch x := v.(type) {
	case ExprBuilder:
		return x.expr
	case Expr:
		return x
	default:
		t, _ := inferLiteralType(v)
		return LiteralNode{Value: v, Type: t}
	}
}

func (b ExprBuilder) binary(op BinaryOp, other any) ExprBuilder {
	return ExprBuilder{expr: BinaryNode{Op: op, Left: b.expr, Right: toExpr(other)}}
}

func (b ExprBuilder) unary(op UnaryOp) ExprBuilder {
	return ExprBuilder{expr: UnaryNode{Op: op, Inner: b.expr}}
}

func (b ExprBuilder) agg(op AggOp) ExprBuilder {
	return ExprBuilder{expr: AggNode{Op: op, Inner: b.expr}}
}

func (b ExprBuilder) Eq(other any) ExprBuilder  { return b.binary(BinaryOpEq, other) }
func (b ExprBuilder) Neq(other any) ExprBuilder { return b.binary(BinaryOpNeq, other) }
func (b ExprBuilder) Lt(other any) ExprBuilder  { return b.binary(BinaryOpLt, other) }
func (b ExprBuilder) Lte(other any) ExprBuilder { return b.binary(BinaryOpLte, other) }
func (b ExprBuilder) Gt(other any) ExprBuilder  { return b.binary(BinaryOpGt, other) }
func (b ExprBuilder) Gte(other any) ExprBuilder { return b.binary(BinaryOpGte, other) }

func (b ExprBuilder) And(other any) ExprBuilder { return b.binary(BinaryOpAnd, other) }
func (b ExprBuilder) Or(other any) ExprBuilder  { return b.binary(BinaryOpOr, other) }
func (b ExprBuilder) Not() ExprBuilder          { return b.unary(UnaryOpNot) }

func (b ExprBuilder) Add(other any) ExprBuilder { return b.binary(BinaryOpAdd, other) }
func (b ExprBuilder) Sub(other any) ExprBuilder { return b.binary(BinaryOpSub, other) }
func (b ExprBuilder) Mul(other any) ExprBuilder { return b.binary(BinaryOpMul, other) }
func (b ExprBuilder) Div(other any) ExprBuilder { return b.binary(BinaryOpDiv, other) }
func (b ExprBuilder) Neg() ExprBuilder          { return b.unary(UnaryOpNeg) }

func (b ExprBuilder) IsNull() ExprBuilder    { return b.unary(UnaryOpIsNull) }
func (b ExprBuilder) IsNotNull() ExprBuilder { return b.unary(UnaryOpIsNotNull) }

func (b ExprBuilder) Count() ExprBuilder { return b.agg(AggOpCount) }
func (b ExprBuilder) Sum() ExprBuilder   { return b.agg(AggOpSum) }
func (b ExprBuilder) Mean() ExprBuilder  { return b.agg(AggOpMean) }
func (b ExprBuilder) Min() ExprBuilder   { return b.agg(AggOpMin) }
func (b ExprBuilder) Max() ExprBuilder   { return b.agg(AggOpMax) }

func (b ExprBuilder) Alias(name string) ExprBuilder {
	return ExprBuilder{expr: AliasNode{Name: name, Inner: b.expr}}
}

func (b ExprBuilder) Cast(t arrow.DataType) ExprBuilder {
	return ExprBuilder{expr: CastNode{Inner: b.expr, Type: t}}
}
