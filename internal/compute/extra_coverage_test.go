package compute

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/stretchr/testify/require"

	"github.com/karthedew/cosma/expr"
	"github.com/karthedew/cosma/schema"
)

// TestBytesCompare_AllOps drives every comparison operator on binary columns so
// bytesOp is fully covered.
func TestBytesCompare_AllOps(t *testing.T) {

	mkRec := func() arrow.Record {
		lb := array.NewBinaryBuilder(newGoMem(), arrow.BinaryTypes.Binary)
		lb.Append([]byte("b"))
		lb.Append([]byte("b"))
		la := lb.NewArray()
		lb.Release()
		rb := array.NewBinaryBuilder(newGoMem(), arrow.BinaryTypes.Binary)
		rb.Append([]byte("a"))
		rb.Append([]byte("c"))
		ra := rb.NewArray()
		rb.Release()
		return twoColRecord(t, "l", la, "r", ra)
	}

	cases := []struct {
		name string
		op   func(a, b expr.Expr) expr.Expr
		want []bool // [ "b" vs "a", "b" vs "c" ]
	}{
		{"eq", func(a, b expr.Expr) expr.Expr { return a.Eq(b) }, []bool{false, false}},
		{"neq", func(a, b expr.Expr) expr.Expr { return a.Neq(b) }, []bool{true, true}},
		{"lt", func(a, b expr.Expr) expr.Expr { return a.Lt(b) }, []bool{false, true}},
		{"lte", func(a, b expr.Expr) expr.Expr { return a.Lte(b) }, []bool{false, true}},
		{"gt", func(a, b expr.Expr) expr.Expr { return a.Gt(b) }, []bool{true, false}},
		{"gte", func(a, b expr.Expr) expr.Expr { return a.Gte(b) }, []bool{true, false}},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			rec := mkRec()
			defer rec.Release()
			out, err := Eval(tc.op(expr.Col("l"), expr.Col("r")).Node, rec, newGoMem())
			require.NoError(t, err)
			defer out.Release()
			res := out.(*array.Boolean)
			require.Equal(t, tc.want, []bool{res.Value(0), res.Value(1)})
		})
	}
}

// TestLitUint64_UntypedUint covers the `uint` branch of litUint64 by building a
// LiteralNode whose value is a plain uint.
func TestLitUint64_UntypedUint(t *testing.T) {
	rec := int64Record(t, "a", []int64{0, 0})
	defer rec.Release()
	node := expr.LiteralNode{Value: uint(9), Type: arrow.PrimitiveTypes.Uint64}
	out, err := Eval(node, rec, newGoMem())
	require.NoError(t, err)
	defer out.Release()
	require.Equal(t, uint64(9), out.(*array.Uint64).Value(0))
}

// TestLitInt64_Int64Branch covers the int64 case of litInt64 directly.
func TestLitInt64_Int64Branch(t *testing.T) {
	rec := int64Record(t, "a", []int64{0})
	defer rec.Release()
	node := expr.LiteralNode{Value: int64(42), Type: arrow.PrimitiveTypes.Int64}
	out, err := Eval(node, rec, newGoMem())
	require.NoError(t, err)
	defer out.Release()
	require.Equal(t, int64(42), out.(*array.Int64).Value(0))
}

// TestLiteral_TypeMismatchErrors covers litAs / litInt64 / litUint64 / litTimestamp
// error paths where the boxed Go value does not match the declared Arrow type.
func TestLiteral_TypeMismatchErrors(t *testing.T) {
	rec := int64Record(t, "a", []int64{0})
	defer rec.Release()

	cases := []struct {
		name string
		node expr.LiteralNode
	}{
		{"int8 mismatch", expr.LiteralNode{Value: "x", Type: arrow.PrimitiveTypes.Int8}},
		{"int64 mismatch", expr.LiteralNode{Value: "x", Type: arrow.PrimitiveTypes.Int64}},
		{"uint64 mismatch", expr.LiteralNode{Value: "x", Type: arrow.PrimitiveTypes.Uint64}},
		{"float64 mismatch", expr.LiteralNode{Value: "x", Type: arrow.PrimitiveTypes.Float64}},
		{"string mismatch", expr.LiteralNode{Value: 1, Type: arrow.BinaryTypes.String}},
		{"bool mismatch", expr.LiteralNode{Value: 1, Type: arrow.FixedWidthTypes.Boolean}},
		{"timestamp mismatch", expr.LiteralNode{Value: "x", Type: &arrow.TimestampType{Unit: arrow.Nanosecond}}},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			_, err := Eval(tc.node, rec, newGoMem())
			require.Error(t, err)
		})
	}
}

// TestLiteral_NoResolvedType covers the nil-type guard in evalLiteral.
func TestLiteral_NoResolvedType(t *testing.T) {
	rec := int64Record(t, "a", []int64{0})
	defer rec.Release()
	_, err := Eval(expr.LiteralNode{Value: 1, Type: nil}, rec, newGoMem())
	require.Error(t, err)
}

// TestLiteral_UnsupportedBroadcastType covers the default branch of evalLiteral.
func TestLiteral_UnsupportedBroadcastType(t *testing.T) {
	rec := int64Record(t, "a", []int64{0})
	defer rec.Release()
	_, err := Eval(expr.LiteralNode{Value: arrow.Duration(1), Type: &arrow.DurationType{Unit: arrow.Second}}, rec, newGoMem())
	require.Error(t, err)
}

// TestEvalBinary_UnsupportedOp covers the default branch of evalBinary by using
// a BinaryNode with an out-of-range op code.
func TestEvalBinary_UnsupportedOp(t *testing.T) {
	rec := int64Record(t, "a", []int64{1, 2})
	defer rec.Release()
	node := expr.BinaryNode{Op: expr.BinaryOp(250), Left: expr.Col("a").Node, Right: expr.Lit(int64(1)).Node}
	_, err := Eval(node, rec, newGoMem())
	require.Error(t, err)
}

// TestEvalBinary_OperandError surfaces an error from evaluating an operand.
func TestEvalBinary_OperandError(t *testing.T) {
	rec := int64Record(t, "a", []int64{1})
	defer rec.Release()

	t.Run("left operand error", func(t *testing.T) {
		_, err := Eval(expr.Col("missing").Add(expr.Col("a")).Node, rec, newGoMem())
		require.Error(t, err)
	})

	t.Run("right operand error", func(t *testing.T) {
		_, err := Eval(expr.Col("a").Add(expr.Col("missing")).Node, rec, newGoMem())
		require.Error(t, err)
	})
}

// TestEvalUnary_UnsupportedOp covers the default branch of evalUnary and the
// inner-operand error path.
func TestEvalUnary_UnsupportedOpAndError(t *testing.T) {
	rec := int64Record(t, "a", []int64{1})
	defer rec.Release()

	t.Run("unsupported op", func(t *testing.T) {
		_, err := Eval(expr.UnaryNode{Op: expr.UnaryOp(250), Inner: expr.Col("a").Node}, rec, newGoMem())
		require.Error(t, err)
	})

	t.Run("inner error", func(t *testing.T) {
		_, err := Eval(expr.Col("missing").Neg().Node, rec, newGoMem())
		require.Error(t, err)
	})
}

// unknownNode is an ExprNode implementation outside compute's type switch, used
// to drive Eval's default "unsupported node" branch.
type unknownNode struct{}

func (unknownNode) String() string                                  { return "unknown" }
func (unknownNode) Children() []expr.ExprNode                       { return nil }
func (unknownNode) DataType(*schema.Schema) (arrow.DataType, error) { return nil, nil }

func TestEval_UnsupportedNode(t *testing.T) {
	rec := int64Record(t, "a", []int64{1})
	defer rec.Release()
	_, err := Eval(unknownNode{}, rec, newGoMem())
	require.Error(t, err)
}
