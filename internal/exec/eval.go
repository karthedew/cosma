package exec

import (
	"cmp"
	"context"
	"fmt"

	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"
	"github.com/apache/arrow/go/v18/arrow/memory"

	"github.com/karthedew/cosma/internal/expr"
)

// Eval evaluates an expression tree against a record batch and returns an
// arrow.Array whose length is batch.NumRows() (or len(sel) once selection
// vectors are wired up). It is the executor's per-batch entry point.
//
// Ownership contract:
//   - The caller owns the returned array and must Release it.
//   - Eval is responsible for releasing every intermediate array it
//     allocates while recursing into children, before returning.
//   - Inputs (batch, child arrays held by callers) are not released by
//     Eval; only intermediates produced by Eval itself are.
//
// sel is a row-index selection vector reserved for future filter→project
// fusion. nil means "all rows in batch order"; a non-nil slice currently
// returns an error so that operators don't accidentally rely on a feature
// that hasn't been implemented yet. The argument lives in the signature
// today so adding selection later doesn't break every call site.
//
// ctx cancellation is checked at the entry of each Eval call. Long kernels
// added in the future should also poll ctx.Err periodically.
func Eval(
	ctx context.Context,
	e expr.Expr,
	batch arrow.Record,
	sel []int32,
	mem memory.Allocator,
) (arrow.Array, error) {
	if ctx == nil {
		return nil, fmt.Errorf("Eval: nil context")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if e == nil {
		return nil, fmt.Errorf("Eval: nil expression")
	}
	if batch == nil {
		return nil, fmt.Errorf("Eval: nil record batch")
	}
	if sel != nil {
		return nil, fmt.Errorf("Eval: selection vectors not yet supported")
	}
	if mem == nil {
		mem = memory.DefaultAllocator
	}

	switch n := e.(type) {
	case expr.ColumnNode:
		return evalColumn(n, batch)
	case expr.LiteralNode:
		return evalLiteral(n, int(batch.NumRows()), mem)
	case expr.BinaryNode:
		return evalBinary(ctx, n, batch, sel, mem)
	default:
		return nil, fmt.Errorf("Eval: unsupported node %T", e)
	}
}

// evalColumn returns the named column from batch, retained so the caller's
// Release matches Eval's contract. The original batch column is unaffected.
func evalColumn(n expr.ColumnNode, batch arrow.Record) (arrow.Array, error) {
	idx := -1
	for i, f := range batch.Schema().Fields() {
		if f.Name == n.Name {
			idx = i
			break
		}
	}
	if idx < 0 {
		return nil, fmt.Errorf("Eval: column %q not in batch", n.Name)
	}
	arr := batch.Column(idx)
	arr.Retain()
	return arr, nil
}

// evalBinary recurses into both operands, releases intermediates, then
// dispatches to a kernel. Adding a new op is one case here plus a kernel
// function; adding a new type is one case in the kernel.
func evalBinary(
	ctx context.Context,
	n expr.BinaryNode,
	batch arrow.Record,
	sel []int32,
	mem memory.Allocator,
) (arrow.Array, error) {
	left, err := Eval(ctx, n.Left, batch, sel, mem)
	if err != nil {
		return nil, err
	}
	defer left.Release()

	right, err := Eval(ctx, n.Right, batch, sel, mem)
	if err != nil {
		return nil, err
	}
	defer right.Release()

	switch n.Op {
	case expr.BinaryOpEq:
		return kernelEq(left, right, mem)
	case expr.BinaryOpGt:
		return kernelGt(left, right, mem)
	default:
		return nil, fmt.Errorf("Eval: binary op %s not yet implemented", n.Op)
	}
}

// valueArray is the structural shape of every primitive Arrow array that
// kernels read. *array.Int8, *array.String, *array.Boolean etc. each satisfy
// it for their own element type. Generic kernels take it directly so the
// per-type loop body lives in exactly one place.
type valueArray[T any] interface {
	Len() int
	IsNull(i int) bool
	Value(i int) T
}

// cmpKernel is the comparison template: walk both inputs in lock-step,
// short-circuit on null, append the boolean result. cmp captures the
// per-op semantics so this function never grows.
func cmpKernel[T any](
	l, r valueArray[T],
	op func(a, b T) bool,
	mem memory.Allocator,
) arrow.Array {
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

func eqOp[T comparable](a, b T) bool { return a == b }
func gtOp[T cmp.Ordered](a, b T) bool { return a > b }

// kernelEq dispatches == across every type that supports it. comparable in
// Go covers numerics, strings, and bool — exactly the set we want here.
func kernelEq(l, r arrow.Array, mem memory.Allocator) (arrow.Array, error) {
	if err := checkBinaryShape("kernelEq", l, r); err != nil {
		return nil, err
	}
	switch l.DataType().ID() {
	case arrow.INT8:
		return cmpKernel[int8](l.(*array.Int8), r.(*array.Int8), eqOp[int8], mem), nil
	case arrow.INT16:
		return cmpKernel[int16](l.(*array.Int16), r.(*array.Int16), eqOp[int16], mem), nil
	case arrow.INT32:
		return cmpKernel[int32](l.(*array.Int32), r.(*array.Int32), eqOp[int32], mem), nil
	case arrow.INT64:
		return cmpKernel[int64](l.(*array.Int64), r.(*array.Int64), eqOp[int64], mem), nil
	case arrow.UINT8:
		return cmpKernel[uint8](l.(*array.Uint8), r.(*array.Uint8), eqOp[uint8], mem), nil
	case arrow.UINT16:
		return cmpKernel[uint16](l.(*array.Uint16), r.(*array.Uint16), eqOp[uint16], mem), nil
	case arrow.UINT32:
		return cmpKernel[uint32](l.(*array.Uint32), r.(*array.Uint32), eqOp[uint32], mem), nil
	case arrow.UINT64:
		return cmpKernel[uint64](l.(*array.Uint64), r.(*array.Uint64), eqOp[uint64], mem), nil
	case arrow.FLOAT32:
		return cmpKernel[float32](l.(*array.Float32), r.(*array.Float32), eqOp[float32], mem), nil
	case arrow.FLOAT64:
		return cmpKernel[float64](l.(*array.Float64), r.(*array.Float64), eqOp[float64], mem), nil
	case arrow.STRING:
		return cmpKernel[string](l.(*array.String), r.(*array.String), eqOp[string], mem), nil
	case arrow.BOOL:
		return cmpKernel[bool](l.(*array.Boolean), r.(*array.Boolean), eqOp[bool], mem), nil
	default:
		return nil, fmt.Errorf("kernelEq: type %s not yet implemented", l.DataType())
	}
}

// kernelGt dispatches > across ordered types — numerics and strings. Bool
// is intentionally excluded because cmp.Ordered does not include it.
func kernelGt(l, r arrow.Array, mem memory.Allocator) (arrow.Array, error) {
	if err := checkBinaryShape("kernelGt", l, r); err != nil {
		return nil, err
	}
	switch l.DataType().ID() {
	case arrow.INT8:
		return cmpKernel[int8](l.(*array.Int8), r.(*array.Int8), gtOp[int8], mem), nil
	case arrow.INT16:
		return cmpKernel[int16](l.(*array.Int16), r.(*array.Int16), gtOp[int16], mem), nil
	case arrow.INT32:
		return cmpKernel[int32](l.(*array.Int32), r.(*array.Int32), gtOp[int32], mem), nil
	case arrow.INT64:
		return cmpKernel[int64](l.(*array.Int64), r.(*array.Int64), gtOp[int64], mem), nil
	case arrow.UINT8:
		return cmpKernel[uint8](l.(*array.Uint8), r.(*array.Uint8), gtOp[uint8], mem), nil
	case arrow.UINT16:
		return cmpKernel[uint16](l.(*array.Uint16), r.(*array.Uint16), gtOp[uint16], mem), nil
	case arrow.UINT32:
		return cmpKernel[uint32](l.(*array.Uint32), r.(*array.Uint32), gtOp[uint32], mem), nil
	case arrow.UINT64:
		return cmpKernel[uint64](l.(*array.Uint64), r.(*array.Uint64), gtOp[uint64], mem), nil
	case arrow.FLOAT32:
		return cmpKernel[float32](l.(*array.Float32), r.(*array.Float32), gtOp[float32], mem), nil
	case arrow.FLOAT64:
		return cmpKernel[float64](l.(*array.Float64), r.(*array.Float64), gtOp[float64], mem), nil
	case arrow.STRING:
		return cmpKernel[string](l.(*array.String), r.(*array.String), gtOp[string], mem), nil
	default:
		return nil, fmt.Errorf("kernelGt: type %s not supported", l.DataType())
	}
}

func checkBinaryShape(kernel string, l, r arrow.Array) error {
	if l.Len() != r.Len() {
		return fmt.Errorf("%s: length mismatch (%d vs %d)", kernel, l.Len(), r.Len())
	}
	if l.DataType().ID() != r.DataType().ID() {
		return fmt.Errorf("%s: type mismatch (%s vs %s)", kernel, l.DataType(), r.DataType())
	}
	return nil
}

// primBuilder is the dual of valueArray for outputs. *array.Int8Builder etc.
// each satisfy it for their element type, which lets broadcast capture the
// "build N copies of v" loop in one place.
type primBuilder[T any] interface {
	Reserve(int)
	Append(T)
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

// evalLiteral materializes the scalar as an Arrow array of length n. Each
// case picks the matching builder and converts the Go value to the kernel
// type — most are direct type assertions, with int/uint widening for the
// untyped-literal cases (Lit(5) → int64, Lit(uint(5)) → uint64).
func evalLiteral(lit expr.LiteralNode, n int, mem memory.Allocator) (arrow.Array, error) {
	if lit.Type == nil {
		return nil, fmt.Errorf("Eval: literal has no resolved type")
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
		return nil, fmt.Errorf("Eval: literal broadcast for %s not yet implemented", lit.Type)
	}
}

// litAs is the strict converter: the Go value must already be of the
// expected type. The typed builders (expr.Int32, expr.Float32, ...) feed
// matching values through this path.
func litAs[T any](v any) (T, error) {
	x, ok := v.(T)
	if !ok {
		var zero T
		return zero, fmt.Errorf("Eval: expected %T literal, got %T", zero, v)
	}
	return x, nil
}

// litInt64 widens the untyped-int case (Lit(5) stores int) so it can feed
// the Int64 builder without forcing every caller to use Int64(...) explicitly.
func litInt64(v any) (int64, error) {
	switch x := v.(type) {
	case int:
		return int64(x), nil
	case int64:
		return x, nil
	default:
		return 0, fmt.Errorf("Eval: expected int64 literal, got %T", v)
	}
}

func litUint64(v any) (uint64, error) {
	switch x := v.(type) {
	case uint:
		return uint64(x), nil
	case uint64:
		return x, nil
	default:
		return 0, fmt.Errorf("Eval: expected uint64 literal, got %T", v)
	}
}
