// Package compute holds the chunk-level Arrow kernels that back Cosma's eager
// DataFrame operations: expression evaluation, filtering, and (in time)
// arithmetic, aggregation, and hashing. It depends only on Arrow and the
// public expression tree — never on the dataframe package — so eager
// operations call into it without an import cycle.
package compute

import (
	"cmp"
	"fmt"

	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"
	"github.com/apache/arrow/go/v18/arrow/memory"

	"github.com/karthedew/cosma/expr"
)

// Eval evaluates an expression tree against a single record batch and returns
// a newly-allocated arrow.Array of length rec.NumRows().
//
// Ownership: the caller owns the returned array and must Release it. rec and
// its columns are not released. Every intermediate array allocated while
// recursing into children is released before Eval returns.
func Eval(e expr.ExprNode, rec arrow.Record, mem memory.Allocator) (arrow.Array, error) {
	if e == nil {
		return nil, fmt.Errorf("compute.Eval: nil expression")
	}
	if rec == nil {
		return nil, fmt.Errorf("compute.Eval: nil record")
	}
	if mem == nil {
		mem = memory.DefaultAllocator
	}

	switch n := e.(type) {
	case expr.ColumnNode:
		return evalColumn(n, rec)
	case expr.LiteralNode:
		return evalLiteral(n, int(rec.NumRows()), mem)
	case expr.BinaryNode:
		return evalBinary(n, rec, mem)
	case expr.AliasNode:
		return Eval(n.Inner, rec, mem)
	default:
		return nil, fmt.Errorf("compute.Eval: unsupported node %T", e)
	}
}

// evalColumn returns the named column from rec, retained so the caller's
// Release matches Eval's ownership contract.
func evalColumn(n expr.ColumnNode, rec arrow.Record) (arrow.Array, error) {
	for i, f := range rec.Schema().Fields() {
		if f.Name == n.Name {
			arr := rec.Column(i)
			arr.Retain()
			return arr, nil
		}
	}
	return nil, fmt.Errorf("compute.Eval: column %q not in batch", n.Name)
}

// evalBinary recurses into both operands, releases the intermediates, then
// dispatches to a kernel. Comparison ops produce a boolean array.
func evalBinary(n expr.BinaryNode, rec arrow.Record, mem memory.Allocator) (arrow.Array, error) {
	left, err := Eval(n.Left, rec, mem)
	if err != nil {
		return nil, err
	}
	defer left.Release()

	right, err := Eval(n.Right, rec, mem)
	if err != nil {
		return nil, err
	}
	defer right.Release()

	switch n.Op {
	case expr.BinaryOpEq, expr.BinaryOpNeq,
		expr.BinaryOpLt, expr.BinaryOpLte,
		expr.BinaryOpGt, expr.BinaryOpGte:
		return compareKernel(n.Op, left, right, mem)
	case expr.BinaryOpAdd, expr.BinaryOpSub,
		expr.BinaryOpMul, expr.BinaryOpDiv:
		return arithKernel(n.Op, left, right, mem)
	default:
		return nil, fmt.Errorf("compute.Eval: binary op %s not yet implemented", n.Op)
	}
}

// valueArray is the structural shape of every primitive Arrow array that
// kernels read. *array.Int64, *array.String, *array.Boolean etc. each satisfy
// it for their own element type, so a generic kernel body lives in one place.
type valueArray[T any] interface {
	Len() int
	IsNull(i int) bool
	Value(i int) T
}

// cmpKernel walks both inputs in lock-step, propagating nulls and appending the
// boolean result of op. op captures the per-operator semantics.
func cmpKernel[T any](l, r valueArray[T], op func(a, b T) bool, mem memory.Allocator) arrow.Array {
	n := l.Len()
	b := array.NewBooleanBuilder(mem)
	defer b.Release()
	b.Reserve(n)
	for i := 0; i < n; i++ {
		if l.IsNull(i) || r.IsNull(i) {
			b.AppendNull()
			continue
		}
		b.Append(op(l.Value(i), r.Value(i)))
	}
	return b.NewArray()
}

// orderedOp returns the comparison semantics for any cmp.Ordered type, covering
// every operator. cmp.Ordered spans the integers, floats, and strings.
func orderedOp[T cmp.Ordered](op expr.BinaryOp) func(a, b T) bool {
	switch op {
	case expr.BinaryOpEq:
		return func(a, b T) bool { return a == b }
	case expr.BinaryOpNeq:
		return func(a, b T) bool { return a != b }
	case expr.BinaryOpLt:
		return func(a, b T) bool { return a < b }
	case expr.BinaryOpLte:
		return func(a, b T) bool { return a <= b }
	case expr.BinaryOpGt:
		return func(a, b T) bool { return a > b }
	case expr.BinaryOpGte:
		return func(a, b T) bool { return a >= b }
	default:
		return nil
	}
}

// compareKernel dispatches a comparison op across the supported element types.
// Booleans support only == and !=, since they are not ordered.
func compareKernel(op expr.BinaryOp, l, r arrow.Array, mem memory.Allocator) (arrow.Array, error) {
	if l.Len() != r.Len() {
		return nil, fmt.Errorf("compute.compare: length mismatch (%d vs %d)", l.Len(), r.Len())
	}
	if l.DataType().ID() != r.DataType().ID() {
		return nil, fmt.Errorf("compute.compare: type mismatch (%s vs %s)", l.DataType(), r.DataType())
	}

	switch l.DataType().ID() {
	case arrow.INT8:
		return ordered[int8](op, l.(*array.Int8), r.(*array.Int8), mem)
	case arrow.INT16:
		return ordered[int16](op, l.(*array.Int16), r.(*array.Int16), mem)
	case arrow.INT32:
		return ordered[int32](op, l.(*array.Int32), r.(*array.Int32), mem)
	case arrow.INT64:
		return ordered[int64](op, l.(*array.Int64), r.(*array.Int64), mem)
	case arrow.UINT8:
		return ordered[uint8](op, l.(*array.Uint8), r.(*array.Uint8), mem)
	case arrow.UINT16:
		return ordered[uint16](op, l.(*array.Uint16), r.(*array.Uint16), mem)
	case arrow.UINT32:
		return ordered[uint32](op, l.(*array.Uint32), r.(*array.Uint32), mem)
	case arrow.UINT64:
		return ordered[uint64](op, l.(*array.Uint64), r.(*array.Uint64), mem)
	case arrow.FLOAT32:
		return ordered[float32](op, l.(*array.Float32), r.(*array.Float32), mem)
	case arrow.FLOAT64:
		return ordered[float64](op, l.(*array.Float64), r.(*array.Float64), mem)
	case arrow.STRING:
		return ordered[string](op, l.(*array.String), r.(*array.String), mem)
	case arrow.BOOL:
		return boolCompare(op, l.(*array.Boolean), r.(*array.Boolean), mem)
	default:
		return nil, fmt.Errorf("compute.compare: type %s not supported", l.DataType())
	}
}

// ordered resolves the operator for an ordered element type and applies it.
func ordered[T cmp.Ordered](op expr.BinaryOp, l, r valueArray[T], mem memory.Allocator) (arrow.Array, error) {
	fn := orderedOp[T](op)
	if fn == nil {
		return nil, fmt.Errorf("compute.compare: op %s not supported", op)
	}
	return cmpKernel[T](l, r, fn, mem), nil
}

// boolCompare handles == and != for booleans, the only ordered-free comparisons
// that make sense on bool.
func boolCompare(op expr.BinaryOp, l, r valueArray[bool], mem memory.Allocator) (arrow.Array, error) {
	switch op {
	case expr.BinaryOpEq:
		return cmpKernel[bool](l, r, func(a, b bool) bool { return a == b }, mem), nil
	case expr.BinaryOpNeq:
		return cmpKernel[bool](l, r, func(a, b bool) bool { return a != b }, mem), nil
	default:
		return nil, fmt.Errorf("compute.compare: op %s not supported on bool", op)
	}
}

// primBuilder is the dual of valueArray for outputs, letting broadcast capture
// the "build n copies of v" loop in one place.
type primBuilder[T any] interface {
	Reserve(int)
	Append(T)
	AppendNull()
	Release()
	NewArray() arrow.Array
}

func broadcast[T any](b primBuilder[T], v T, n int) arrow.Array {
	defer b.Release()
	b.Reserve(n)
	for i := 0; i < n; i++ {
		b.Append(v)
	}
	return b.NewArray()
}

// evalLiteral materializes the scalar as an Arrow array of length n.
func evalLiteral(lit expr.LiteralNode, n int, mem memory.Allocator) (arrow.Array, error) {
	if lit.Type == nil {
		return nil, fmt.Errorf("compute.Eval: literal has no resolved type")
	}
	switch lit.Type.ID() {
	case arrow.INT8:
		v, err := litAs[int8](lit.Value)
		if err != nil {
			return nil, err
		}
		return broadcast[int8](array.NewInt8Builder(mem), v, n), nil
	case arrow.INT16:
		v, err := litAs[int16](lit.Value)
		if err != nil {
			return nil, err
		}
		return broadcast[int16](array.NewInt16Builder(mem), v, n), nil
	case arrow.INT32:
		v, err := litAs[int32](lit.Value)
		if err != nil {
			return nil, err
		}
		return broadcast[int32](array.NewInt32Builder(mem), v, n), nil
	case arrow.INT64:
		v, err := litInt64(lit.Value)
		if err != nil {
			return nil, err
		}
		return broadcast[int64](array.NewInt64Builder(mem), v, n), nil
	case arrow.UINT8:
		v, err := litAs[uint8](lit.Value)
		if err != nil {
			return nil, err
		}
		return broadcast[uint8](array.NewUint8Builder(mem), v, n), nil
	case arrow.UINT16:
		v, err := litAs[uint16](lit.Value)
		if err != nil {
			return nil, err
		}
		return broadcast[uint16](array.NewUint16Builder(mem), v, n), nil
	case arrow.UINT32:
		v, err := litAs[uint32](lit.Value)
		if err != nil {
			return nil, err
		}
		return broadcast[uint32](array.NewUint32Builder(mem), v, n), nil
	case arrow.UINT64:
		v, err := litUint64(lit.Value)
		if err != nil {
			return nil, err
		}
		return broadcast[uint64](array.NewUint64Builder(mem), v, n), nil
	case arrow.FLOAT32:
		v, err := litAs[float32](lit.Value)
		if err != nil {
			return nil, err
		}
		return broadcast[float32](array.NewFloat32Builder(mem), v, n), nil
	case arrow.FLOAT64:
		v, err := litAs[float64](lit.Value)
		if err != nil {
			return nil, err
		}
		return broadcast[float64](array.NewFloat64Builder(mem), v, n), nil
	case arrow.STRING:
		v, err := litAs[string](lit.Value)
		if err != nil {
			return nil, err
		}
		return broadcast[string](array.NewStringBuilder(mem), v, n), nil
	case arrow.BOOL:
		v, err := litAs[bool](lit.Value)
		if err != nil {
			return nil, err
		}
		return broadcast[bool](array.NewBooleanBuilder(mem), v, n), nil
	default:
		return nil, fmt.Errorf("compute.Eval: literal broadcast for %s not yet implemented", lit.Type)
	}
}

// litAs is the strict converter: the Go value must already be of type T.
func litAs[T any](v any) (T, error) {
	x, ok := v.(T)
	if !ok {
		var zero T
		return zero, fmt.Errorf("compute.Eval: expected %T literal, got %T", zero, v)
	}
	return x, nil
}

// litInt64 widens the untyped-int case (Lit(5) stores int) for the Int64 path.
func litInt64(v any) (int64, error) {
	switch x := v.(type) {
	case int:
		return int64(x), nil
	case int64:
		return x, nil
	default:
		return 0, fmt.Errorf("compute.Eval: expected int64 literal, got %T", v)
	}
}

func litUint64(v any) (uint64, error) {
	switch x := v.(type) {
	case uint:
		return uint64(x), nil
	case uint64:
		return x, nil
	default:
		return 0, fmt.Errorf("compute.Eval: expected uint64 literal, got %T", v)
	}
}
