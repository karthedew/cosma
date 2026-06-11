package compute

import (
	"fmt"

	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"
	"github.com/apache/arrow/go/v18/arrow/memory"

	"github.com/karthedew/cosma/expr"
)

// kleeneKernel evaluates AND/OR over two boolean arrays using three-valued
// (Kleene) logic, implemented directly on the validity/value bitmaps rather than
// via Arrow's KleeneAnd/KleeneOr (not exposed as array kernels in Arrow Go v18).
//
// AND: FALSE dominates — NULL AND FALSE → FALSE, NULL AND TRUE → NULL.
// OR:  TRUE  dominates — NULL OR  TRUE  → TRUE,  NULL OR  FALSE → NULL.
// When neither operand is null the result is the ordinary boolean op.
func kleeneKernel(op expr.BinaryOp, l, r *array.Boolean, mem memory.Allocator) (arrow.Array, error) {
	if l.Len() != r.Len() {
		return nil, fmt.Errorf("compute.logical: length mismatch (%d vs %d)", l.Len(), r.Len())
	}
	n := l.Len()
	b := array.NewBooleanBuilder(mem)
	defer b.Release()
	b.Reserve(n)

	for i := 0; i < n; i++ {
		lNull, rNull := l.IsNull(i), r.IsNull(i)
		switch op {
		case expr.BinaryOpAnd:
			// FALSE dominates AND.
			if (!lNull && !l.Value(i)) || (!rNull && !r.Value(i)) {
				b.Append(false)
				continue
			}
			if lNull || rNull {
				b.AppendNull()
				continue
			}
			b.Append(l.Value(i) && r.Value(i))
		case expr.BinaryOpOr:
			// TRUE dominates OR.
			if (!lNull && l.Value(i)) || (!rNull && r.Value(i)) {
				b.Append(true)
				continue
			}
			if lNull || rNull {
				b.AppendNull()
				continue
			}
			b.Append(l.Value(i) || r.Value(i))
		default:
			return nil, fmt.Errorf("compute.logical: op %s not supported", op)
		}
	}
	return b.NewArray(), nil
}

// logicalKernel routes AND/OR to the Kleene boolean kernel, validating types.
func logicalKernel(op expr.BinaryOp, l, r arrow.Array, mem memory.Allocator) (arrow.Array, error) {
	lb, ok := l.(*array.Boolean)
	if !ok {
		return nil, fmt.Errorf("compute.logical: %s requires bool operands, got %s", op, l.DataType())
	}
	rb, ok := r.(*array.Boolean)
	if !ok {
		return nil, fmt.Errorf("compute.logical: %s requires bool operands, got %s", op, r.DataType())
	}
	return kleeneKernel(op, lb, rb, mem)
}
