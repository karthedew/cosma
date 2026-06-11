package compute

import (
	"cmp"
	"fmt"
	"sort"

	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"
)

// SortIndices returns a stable permutation of row indices that orders chunked by
// value. Nulls always sort last, regardless of direction. The result is used to
// reorder every column of a DataFrame by the same key (a take/gather).
func SortIndices(chunked *arrow.Chunked, descending bool) ([]int, error) {
	if chunked == nil {
		return nil, fmt.Errorf("compute.SortIndices: nil column")
	}
	switch chunked.DataType().ID() {
	case arrow.INT8:
		return argsort[int8](flatten[int8](chunked.Chunks(), func(a arrow.Array) valueArray[int8] { return a.(*array.Int8) }), descending), nil
	case arrow.INT16:
		return argsort[int16](flatten[int16](chunked.Chunks(), func(a arrow.Array) valueArray[int16] { return a.(*array.Int16) }), descending), nil
	case arrow.INT32:
		return argsort[int32](flatten[int32](chunked.Chunks(), func(a arrow.Array) valueArray[int32] { return a.(*array.Int32) }), descending), nil
	case arrow.INT64:
		return argsort[int64](flatten[int64](chunked.Chunks(), func(a arrow.Array) valueArray[int64] { return a.(*array.Int64) }), descending), nil
	case arrow.UINT8:
		return argsort[uint8](flatten[uint8](chunked.Chunks(), func(a arrow.Array) valueArray[uint8] { return a.(*array.Uint8) }), descending), nil
	case arrow.UINT16:
		return argsort[uint16](flatten[uint16](chunked.Chunks(), func(a arrow.Array) valueArray[uint16] { return a.(*array.Uint16) }), descending), nil
	case arrow.UINT32:
		return argsort[uint32](flatten[uint32](chunked.Chunks(), func(a arrow.Array) valueArray[uint32] { return a.(*array.Uint32) }), descending), nil
	case arrow.UINT64:
		return argsort[uint64](flatten[uint64](chunked.Chunks(), func(a arrow.Array) valueArray[uint64] { return a.(*array.Uint64) }), descending), nil
	case arrow.FLOAT32:
		return argsort[float32](flatten[float32](chunked.Chunks(), func(a arrow.Array) valueArray[float32] { return a.(*array.Float32) }), descending), nil
	case arrow.FLOAT64:
		return argsort[float64](flatten[float64](chunked.Chunks(), func(a arrow.Array) valueArray[float64] { return a.(*array.Float64) }), descending), nil
	case arrow.STRING:
		return argsort[string](flatten[string](chunked.Chunks(), func(a arrow.Array) valueArray[string] { return a.(*array.String) }), descending), nil
	default:
		return nil, fmt.Errorf("compute.SortIndices: type %s not supported", chunked.DataType())
	}
}

// flat is a chunked column flattened to a contiguous typed slice plus a validity
// mask, so the argsort comparator can index rows in O(1).
type flat[T any] struct {
	vals  []T
	valid []bool
}

func flatten[T any](chunks []arrow.Array, as func(arrow.Array) valueArray[T]) flat[T] {
	var f flat[T]
	for _, ch := range chunks {
		va := as(ch)
		for i := 0; i < va.Len(); i++ {
			if va.IsNull(i) {
				var zero T
				f.vals = append(f.vals, zero)
				f.valid = append(f.valid, false)
				continue
			}
			f.vals = append(f.vals, va.Value(i))
			f.valid = append(f.valid, true)
		}
	}
	return f
}

func argsort[T cmp.Ordered](f flat[T], descending bool) []int {
	idx := make([]int, len(f.vals))
	for i := range idx {
		idx[i] = i
	}
	sort.SliceStable(idx, func(i, j int) bool {
		a, b := idx[i], idx[j]
		if !f.valid[a] || !f.valid[b] {
			// Nulls sort last; a valid value precedes a null.
			return f.valid[a] && !f.valid[b]
		}
		if descending {
			return f.vals[a] > f.vals[b]
		}
		return f.vals[a] < f.vals[b]
	})
	return idx
}
