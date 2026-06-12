package compute

import (
	"context"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	acompute "github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/karthedew/cosma/expr"
)

// evalCast evaluates the inner expression then casts the result to the target
// type via Arrow's compute cast kernels. Null values are preserved.
func evalCast(n expr.CastNode, rec arrow.Record, mem memory.Allocator) (arrow.Array, error) {
	if n.Type == nil {
		return nil, fmt.Errorf("compute.Eval: cast target type is nil")
	}
	in, err := Eval(n.Inner, rec, mem)
	if err != nil {
		return nil, err
	}
	defer in.Release()

	if arrow.TypeEqual(in.DataType(), n.Type) {
		in.Retain()
		return in, nil
	}

	ctx := acompute.WithAllocator(context.Background(), mem)
	out, err := acompute.CastArray(ctx, in, acompute.SafeCastOptions(n.Type))
	if err != nil {
		return nil, fmt.Errorf("compute.Eval: cast %s to %s: %w", in.DataType(), n.Type, err)
	}
	return out, nil
}
