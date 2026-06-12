package compute

import (
	"fmt"

	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"
	"github.com/apache/arrow/go/v18/arrow/memory"
)

// Take reorders a chunked array by row indices, returning a new single-chunk
// array. indices select logical rows across all chunks of chunked: index i
// refers to the i-th row in chunk order. A null at a selected row is preserved
// as a null in the output.
//
// Ownership: the caller owns the returned array and must Release it. chunked is
// not retained or released.
//
// Take is the shared gather primitive behind Sort (reorder by a permutation) and
// Join (gather matched rows). Indices may be negative to emit a null at that
// output position; this lets a left/outer join fill unmatched right-side rows
// with nulls.
func Take(chunked *arrow.Chunked, indices []int64, mem memory.Allocator) (arrow.Array, error) {
	if chunked == nil {
		return nil, fmt.Errorf("compute.Take: nil column")
	}
	if mem == nil {
		mem = memory.DefaultAllocator
	}

	// Flatten chunk boundaries so a logical index maps to (chunk, offset) in
	// O(log nChunks) via a cumulative-offset lookup, but we walk indices once and
	// resolve each directly.
	chunks := chunked.Chunks()
	cum := make([]int64, len(chunks)+1)
	for i, ch := range chunks {
		cum[i+1] = cum[i] + int64(ch.Len())
	}
	total := cum[len(chunks)]

	locate := func(idx int64) (chunk int, off int, null bool, err error) {
		if idx < 0 {
			return 0, 0, true, nil
		}
		if idx >= total {
			return 0, 0, false, fmt.Errorf("compute.Take: index %d out of range [0,%d)", idx, total)
		}
		// Linear-by-chunk search; chunk counts are small in practice.
		for c := 0; c < len(chunks); c++ {
			if idx < cum[c+1] {
				return c, int(idx - cum[c]), false, nil
			}
		}
		return 0, 0, false, fmt.Errorf("compute.Take: index %d out of range [0,%d)", idx, total)
	}

	switch chunked.DataType().ID() {
	case arrow.INT8:
		return takeTyped[int8](chunks, indices, locate, array.NewInt8Builder(mem), func(a arrow.Array) valueArray[int8] { return a.(*array.Int8) })
	case arrow.INT16:
		return takeTyped[int16](chunks, indices, locate, array.NewInt16Builder(mem), func(a arrow.Array) valueArray[int16] { return a.(*array.Int16) })
	case arrow.INT32:
		return takeTyped[int32](chunks, indices, locate, array.NewInt32Builder(mem), func(a arrow.Array) valueArray[int32] { return a.(*array.Int32) })
	case arrow.INT64:
		return takeTyped[int64](chunks, indices, locate, array.NewInt64Builder(mem), func(a arrow.Array) valueArray[int64] { return a.(*array.Int64) })
	case arrow.UINT8:
		return takeTyped[uint8](chunks, indices, locate, array.NewUint8Builder(mem), func(a arrow.Array) valueArray[uint8] { return a.(*array.Uint8) })
	case arrow.UINT16:
		return takeTyped[uint16](chunks, indices, locate, array.NewUint16Builder(mem), func(a arrow.Array) valueArray[uint16] { return a.(*array.Uint16) })
	case arrow.UINT32:
		return takeTyped[uint32](chunks, indices, locate, array.NewUint32Builder(mem), func(a arrow.Array) valueArray[uint32] { return a.(*array.Uint32) })
	case arrow.UINT64:
		return takeTyped[uint64](chunks, indices, locate, array.NewUint64Builder(mem), func(a arrow.Array) valueArray[uint64] { return a.(*array.Uint64) })
	case arrow.FLOAT32:
		return takeTyped[float32](chunks, indices, locate, array.NewFloat32Builder(mem), func(a arrow.Array) valueArray[float32] { return a.(*array.Float32) })
	case arrow.FLOAT64:
		return takeTyped[float64](chunks, indices, locate, array.NewFloat64Builder(mem), func(a arrow.Array) valueArray[float64] { return a.(*array.Float64) })
	case arrow.STRING:
		return takeTyped[string](chunks, indices, locate, array.NewStringBuilder(mem), func(a arrow.Array) valueArray[string] { return a.(*array.String) })
	case arrow.LARGE_STRING:
		return takeTyped[string](chunks, indices, locate, array.NewLargeStringBuilder(mem), func(a arrow.Array) valueArray[string] { return a.(*array.LargeString) })
	case arrow.BOOL:
		return takeTyped[bool](chunks, indices, locate, array.NewBooleanBuilder(mem), func(a arrow.Array) valueArray[bool] { return a.(*array.Boolean) })
	default:
		return nil, fmt.Errorf("compute.Take: type %s not supported", chunked.DataType())
	}
}

// takeTyped gathers indices from a chunked column into a fresh builder. locate
// maps a logical index to its (chunk, offset); a negative index (or one flagged
// null by locate) emits a null at that output position.
func takeTyped[T any](
	chunks []arrow.Array,
	indices []int64,
	locate func(int64) (int, int, bool, error),
	b primBuilder[T],
	as func(arrow.Array) valueArray[T],
) (arrow.Array, error) {
	defer b.Release()

	views := make([]valueArray[T], len(chunks))
	for i, ch := range chunks {
		views[i] = as(ch)
	}

	b.Reserve(len(indices))
	for _, idx := range indices {
		c, off, isNull, err := locate(idx)
		if err != nil {
			return nil, err
		}
		if isNull {
			b.AppendNull()
			continue
		}
		v := views[c]
		if v.IsNull(off) {
			b.AppendNull()
			continue
		}
		b.Append(v.Value(off))
	}
	return b.NewArray(), nil
}
