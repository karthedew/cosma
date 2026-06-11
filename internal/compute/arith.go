package compute

import (
	"fmt"

	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"
	"github.com/apache/arrow/go/v18/arrow/memory"

	"github.com/karthedew/cosma/expr"
)

// number is every Arrow primitive that supports +, -, *, /.
type number interface {
	~int8 | ~int16 | ~int32 | ~int64 |
		~uint8 | ~uint16 | ~uint32 | ~uint64 |
		~float32 | ~float64
}

// arithKernel dispatches an arithmetic op across the numeric element types,
// producing an array of the same type. l and r must share length and type.
func arithKernel(op expr.BinaryOp, l, r arrow.Array, mem memory.Allocator) (arrow.Array, error) {
	if l.Len() != r.Len() {
		return nil, fmt.Errorf("compute.arith: length mismatch (%d vs %d)", l.Len(), r.Len())
	}
	if l.DataType().ID() != r.DataType().ID() {
		return nil, fmt.Errorf("compute.arith: type mismatch (%s vs %s)", l.DataType(), r.DataType())
	}

	switch l.DataType().ID() {
	case arrow.INT8:
		return arith[int8](op, l.(*array.Int8), r.(*array.Int8), array.NewInt8Builder(mem))
	case arrow.INT16:
		return arith[int16](op, l.(*array.Int16), r.(*array.Int16), array.NewInt16Builder(mem))
	case arrow.INT32:
		return arith[int32](op, l.(*array.Int32), r.(*array.Int32), array.NewInt32Builder(mem))
	case arrow.INT64:
		return arith[int64](op, l.(*array.Int64), r.(*array.Int64), array.NewInt64Builder(mem))
	case arrow.UINT8:
		return arith[uint8](op, l.(*array.Uint8), r.(*array.Uint8), array.NewUint8Builder(mem))
	case arrow.UINT16:
		return arith[uint16](op, l.(*array.Uint16), r.(*array.Uint16), array.NewUint16Builder(mem))
	case arrow.UINT32:
		return arith[uint32](op, l.(*array.Uint32), r.(*array.Uint32), array.NewUint32Builder(mem))
	case arrow.UINT64:
		return arith[uint64](op, l.(*array.Uint64), r.(*array.Uint64), array.NewUint64Builder(mem))
	case arrow.FLOAT32:
		return arith[float32](op, l.(*array.Float32), r.(*array.Float32), array.NewFloat32Builder(mem))
	case arrow.FLOAT64:
		return arith[float64](op, l.(*array.Float64), r.(*array.Float64), array.NewFloat64Builder(mem))
	default:
		return nil, fmt.Errorf("compute.arith: type %s not supported", l.DataType())
	}
}

// arith walks both inputs in lock-step, propagating nulls. Integer division by
// zero yields a null rather than a panic.
func arith[T number](op expr.BinaryOp, l, r valueArray[T], b primBuilder[T]) (arrow.Array, error) {
	defer b.Release()
	n := l.Len()
	b.Reserve(n)
	for i := 0; i < n; i++ {
		if l.IsNull(i) || r.IsNull(i) {
			b.AppendNull()
			continue
		}
		x, y := l.Value(i), r.Value(i)
		switch op {
		case expr.BinaryOpAdd:
			b.Append(x + y)
		case expr.BinaryOpSub:
			b.Append(x - y)
		case expr.BinaryOpMul:
			b.Append(x * y)
		case expr.BinaryOpDiv:
			if y == 0 {
				b.AppendNull()
			} else {
				b.Append(x / y)
			}
		default:
			return nil, fmt.Errorf("compute.arith: op %s not supported", op)
		}
	}
	return b.NewArray(), nil
}
