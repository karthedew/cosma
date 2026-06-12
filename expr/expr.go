package expr

import (
	"time"

	"github.com/apache/arrow-go/v18/arrow"
)

// Expr is the public, transparent wrapper around an expression tree node. The
// exported Node field holds the root ExprNode, so the tree is fully
// inspectable, serializable, and pattern-matchable by external callers. Fluent
// methods compose new Expr values left-to-right without mutating the receiver.
type Expr struct {
	Node ExprNode
}

func (e Expr) String() string {
	if e.Node == nil {
		return "<nil>"
	}
	return e.Node.String()
}

// Col references an input column by name.
func Col(name string) Expr {
	return Expr{Node: ColumnNode{Name: name}}
}

// Lit wraps a Go scalar as a literal. The Arrow type is inferred from the value
// (untyped ints → int64, untyped floats → float64). Use the typed constructors
// below when you need a specific Arrow width.
func Lit(v any) Expr {
	// A time.Time is boxed to the canonical arrow.Timestamp (nanoseconds, UTC)
	// so the compute layer sees the same storage representation that
	// LitTimestamp produces.
	if tt, ok := v.(time.Time); ok {
		t, _ := inferLiteralType(tt)
		return Expr{Node: LiteralNode{Value: arrow.Timestamp(tt.UTC().UnixNano()), Type: t}}
	}
	t, _ := inferLiteralType(v)
	return Expr{Node: LiteralNode{Value: v, Type: t}}
}

func Int8(v int8) Expr       { return typedLit(v, arrow.PrimitiveTypes.Int8) }
func Int16(v int16) Expr     { return typedLit(v, arrow.PrimitiveTypes.Int16) }
func Int32(v int32) Expr     { return typedLit(v, arrow.PrimitiveTypes.Int32) }
func Int64(v int64) Expr     { return typedLit(v, arrow.PrimitiveTypes.Int64) }
func Uint8(v uint8) Expr     { return typedLit(v, arrow.PrimitiveTypes.Uint8) }
func Uint16(v uint16) Expr   { return typedLit(v, arrow.PrimitiveTypes.Uint16) }
func Uint32(v uint32) Expr   { return typedLit(v, arrow.PrimitiveTypes.Uint32) }
func Uint64(v uint64) Expr   { return typedLit(v, arrow.PrimitiveTypes.Uint64) }
func Float32(v float32) Expr { return typedLit(v, arrow.PrimitiveTypes.Float32) }
func Float64(v float64) Expr { return typedLit(v, arrow.PrimitiveTypes.Float64) }

func typedLit(v any, t arrow.DataType) Expr {
	return Expr{Node: LiteralNode{Value: v, Type: t}}
}

func (e Expr) binary(op BinaryOp, other Expr) Expr {
	return Expr{Node: BinaryNode{Op: op, Left: e.Node, Right: other.Node}}
}

func (e Expr) unary(op UnaryOp) Expr {
	return Expr{Node: UnaryNode{Op: op, Inner: e.Node}}
}

func (e Expr) Eq(other Expr) Expr  { return e.binary(BinaryOpEq, other) }
func (e Expr) Neq(other Expr) Expr { return e.binary(BinaryOpNeq, other) }
func (e Expr) Lt(other Expr) Expr  { return e.binary(BinaryOpLt, other) }
func (e Expr) Lte(other Expr) Expr { return e.binary(BinaryOpLte, other) }
func (e Expr) Gt(other Expr) Expr  { return e.binary(BinaryOpGt, other) }
func (e Expr) Gte(other Expr) Expr { return e.binary(BinaryOpGte, other) }

func (e Expr) And(other Expr) Expr { return e.binary(BinaryOpAnd, other) }
func (e Expr) Or(other Expr) Expr  { return e.binary(BinaryOpOr, other) }
func (e Expr) Not() Expr           { return e.unary(UnaryOpNot) }

func (e Expr) Add(other Expr) Expr { return e.binary(BinaryOpAdd, other) }
func (e Expr) Sub(other Expr) Expr { return e.binary(BinaryOpSub, other) }
func (e Expr) Mul(other Expr) Expr { return e.binary(BinaryOpMul, other) }
func (e Expr) Div(other Expr) Expr { return e.binary(BinaryOpDiv, other) }
func (e Expr) Neg() Expr           { return e.unary(UnaryOpNeg) }

func (e Expr) IsNull() Expr    { return e.unary(UnaryOpIsNull) }
func (e Expr) IsNotNull() Expr { return e.unary(UnaryOpIsNotNull) }

// As renames the expression's result via an AliasNode.
func (e Expr) As(name string) Expr {
	return Expr{Node: AliasNode{Name: name, Inner: e.Node}}
}

// Cast forces the expression's output type.
func (e Expr) Cast(t arrow.DataType) Expr {
	return Expr{Node: CastNode{Inner: e.Node, Type: t}}
}

// Count/Sum/Mean/Min/Max build an AggNode over the expression. They return the
// concrete AggNode type (not Expr) so that GroupBy().Agg only accepts
// aggregates at compile time.
func (e Expr) Count() AggNode { return AggNode{Op: AggOpCount, Inner: e} }
func (e Expr) Sum() AggNode   { return AggNode{Op: AggOpSum, Inner: e} }
func (e Expr) Mean() AggNode  { return AggNode{Op: AggOpMean, Inner: e} }
func (e Expr) Min() AggNode   { return AggNode{Op: AggOpMin, Inner: e} }
func (e Expr) Max() AggNode   { return AggNode{Op: AggOpMax, Inner: e} }
