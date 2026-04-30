package exec

import (
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

// evalLiteral materializes the scalar as an Arrow array of length n. This
// is intentionally simple — a future scalar-broadcast type can replace the
// per-row append once kernels learn to consume it.
func evalLiteral(lit expr.LiteralNode, n int, mem memory.Allocator) (arrow.Array, error) {
	if lit.Type == nil {
		return nil, fmt.Errorf("Eval: literal has no resolved type")
	}
	switch lit.Type.ID() {
	case arrow.INT64:
		v, err := asInt64(lit.Value)
		if err != nil {
			return nil, err
		}
		b := array.NewInt64Builder(mem)
		defer b.Release()
		b.Reserve(n)
		for i := 0; i < n; i++ {
			b.Append(v)
		}
		return b.NewArray(), nil
	default:
		return nil, fmt.Errorf("Eval: literal broadcast for %s not yet implemented", lit.Type)
	}
}

func asInt64(v any) (int64, error) {
	switch x := v.(type) {
	case int:
		return int64(x), nil
	case int64:
		return x, nil
	case int32:
		return int64(x), nil
	case int16:
		return int64(x), nil
	case int8:
		return int64(x), nil
	default:
		return 0, fmt.Errorf("Eval: cannot use %T as int64 literal", v)
	}
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
	default:
		return nil, fmt.Errorf("Eval: binary op %s not yet implemented", n.Op)
	}
}

// kernelEq is the dispatcher for == across types. The pattern — switch on
// type id, hand off to a monomorphic per-type kernel — is the template
// every comparison/arithmetic kernel will follow.
func kernelEq(l, r arrow.Array, mem memory.Allocator) (arrow.Array, error) {
	if l.Len() != r.Len() {
		return nil, fmt.Errorf("kernelEq: length mismatch (%d vs %d)", l.Len(), r.Len())
	}
	if l.DataType().ID() != r.DataType().ID() {
		return nil, fmt.Errorf("kernelEq: type mismatch (%s vs %s)", l.DataType(), r.DataType())
	}
	switch l.DataType().ID() {
	case arrow.INT64:
		return eqInt64(l.(*array.Int64), r.(*array.Int64), mem), nil
	default:
		return nil, fmt.Errorf("kernelEq: type %s not yet implemented", l.DataType())
	}
}

func eqInt64(l, r *array.Int64, mem memory.Allocator) arrow.Array {
	n := l.Len()
	b := array.NewBooleanBuilder(mem)
	defer b.Release()
	b.Reserve(n)
	for i := 0; i < n; i++ {
		if l.IsNull(i) || r.IsNull(i) {
			b.AppendNull()
			continue
		}
		b.Append(l.Value(i) == r.Value(i))
	}
	return b.NewArray()
}
