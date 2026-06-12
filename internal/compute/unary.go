package compute

import (
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/karthedew/cosma/expr"
)

// evalUnary recurses into the operand, releases the intermediate, then applies
// the unary op. IsNull/IsNotNull always produce a non-null bool array; Not and
// Neg propagate nulls.
func evalUnary(n expr.UnaryNode, rec arrow.Record, mem memory.Allocator) (arrow.Array, error) {
	in, err := Eval(n.Inner, rec, mem)
	if err != nil {
		return nil, err
	}
	defer in.Release()

	switch n.Op {
	case expr.UnaryOpIsNull:
		return nullMask(in, true, mem), nil
	case expr.UnaryOpIsNotNull:
		return nullMask(in, false, mem), nil
	case expr.UnaryOpNot:
		bl, ok := in.(*array.Boolean)
		if !ok {
			return nil, fmt.Errorf("compute.Eval: not requires bool operand, got %s", in.DataType())
		}
		return notKernel(bl, mem), nil
	case expr.UnaryOpNeg:
		out, err := negKernel(in, mem)
		if err == errNoUnaryKernel {
			if k, ok := lookupUnaryKernel(in.DataType().ID()); ok {
				return k(n.Op, in, mem)
			}
			return nil, fmt.Errorf("compute.Eval: type %s not supported for op %s", in.DataType(), n.Op)
		}
		return out, err
	default:
		return nil, fmt.Errorf("compute.Eval: unary op %s not supported", n.Op)
	}
}

// nullMask builds a non-null bool array marking which input rows are null
// (want=true) or non-null (want=false).
func nullMask(in arrow.Array, want bool, mem memory.Allocator) arrow.Array {
	n := in.Len()
	b := array.NewBooleanBuilder(mem)
	defer b.Release()
	b.Reserve(n)
	for i := 0; i < n; i++ {
		b.Append(in.IsNull(i) == want)
	}
	return b.NewArray()
}

// notKernel inverts a boolean array, propagating nulls.
func notKernel(in *array.Boolean, mem memory.Allocator) arrow.Array {
	n := in.Len()
	b := array.NewBooleanBuilder(mem)
	defer b.Release()
	b.Reserve(n)
	for i := 0; i < n; i++ {
		if in.IsNull(i) {
			b.AppendNull()
			continue
		}
		b.Append(!in.Value(i))
	}
	return b.NewArray()
}

// errNoUnaryKernel signals that the built-in switch has no case for the input
// type, so evalUnary should consult the custom kernel registry.
var errNoUnaryKernel = fmt.Errorf("no built-in unary kernel")

// negKernel arithmetically negates a numeric array, preserving type and
// propagating nulls. Unsigned types are rejected: negation has no in-type
// result.
func negKernel(in arrow.Array, mem memory.Allocator) (arrow.Array, error) {
	switch in.DataType().ID() {
	case arrow.INT8:
		return negTyped[int8](in.(*array.Int8), array.NewInt8Builder(mem)), nil
	case arrow.INT16:
		return negTyped[int16](in.(*array.Int16), array.NewInt16Builder(mem)), nil
	case arrow.INT32:
		return negTyped[int32](in.(*array.Int32), array.NewInt32Builder(mem)), nil
	case arrow.INT64:
		return negTyped[int64](in.(*array.Int64), array.NewInt64Builder(mem)), nil
	case arrow.FLOAT32:
		return negTyped[float32](in.(*array.Float32), array.NewFloat32Builder(mem)), nil
	case arrow.FLOAT64:
		return negTyped[float64](in.(*array.Float64), array.NewFloat64Builder(mem)), nil
	default:
		return nil, errNoUnaryKernel
	}
}

// signedNumber is the subset of numeric types negation produces a same-type
// result for.
type signedNumber interface {
	~int8 | ~int16 | ~int32 | ~int64 | ~float32 | ~float64
}

func negTyped[T signedNumber](in valueArray[T], b primBuilder[T]) arrow.Array {
	defer b.Release()
	n := in.Len()
	b.Reserve(n)
	for i := 0; i < n; i++ {
		if in.IsNull(i) {
			b.AppendNull()
			continue
		}
		b.Append(-in.Value(i))
	}
	return b.NewArray()
}
