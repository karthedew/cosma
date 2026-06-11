package compute

import (
	"cmp"
	"fmt"

	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"
)

// Aggregates holds the reductions for one numeric column, computed in a single
// pass. Sum, Min and Max are boxed in the column's element type and are nil when
// Count == 0 (empty or all-null). Mean is valid only when Count > 0.
type Aggregates struct {
	Count int64
	Sum   any
	Min   any
	Max   any
	Mean  float64
}

// Reduce folds a chunked column into its Sum/Count/Min/Max/Mean, skipping nulls.
// It is the building block for both whole-column aggregation and the per-group
// partials of GroupBy.
func Reduce(chunked *arrow.Chunked) (Aggregates, error) {
	if chunked == nil {
		return Aggregates{}, fmt.Errorf("compute.Reduce: nil column")
	}
	switch chunked.DataType().ID() {
	case arrow.INT8:
		return reduceTyped[int8](chunked.Chunks(), func(a arrow.Array) valueArray[int8] { return a.(*array.Int8) }), nil
	case arrow.INT16:
		return reduceTyped[int16](chunked.Chunks(), func(a arrow.Array) valueArray[int16] { return a.(*array.Int16) }), nil
	case arrow.INT32:
		return reduceTyped[int32](chunked.Chunks(), func(a arrow.Array) valueArray[int32] { return a.(*array.Int32) }), nil
	case arrow.INT64:
		return reduceTyped[int64](chunked.Chunks(), func(a arrow.Array) valueArray[int64] { return a.(*array.Int64) }), nil
	case arrow.UINT8:
		return reduceTyped[uint8](chunked.Chunks(), func(a arrow.Array) valueArray[uint8] { return a.(*array.Uint8) }), nil
	case arrow.UINT16:
		return reduceTyped[uint16](chunked.Chunks(), func(a arrow.Array) valueArray[uint16] { return a.(*array.Uint16) }), nil
	case arrow.UINT32:
		return reduceTyped[uint32](chunked.Chunks(), func(a arrow.Array) valueArray[uint32] { return a.(*array.Uint32) }), nil
	case arrow.UINT64:
		return reduceTyped[uint64](chunked.Chunks(), func(a arrow.Array) valueArray[uint64] { return a.(*array.Uint64) }), nil
	case arrow.FLOAT32:
		return reduceTyped[float32](chunked.Chunks(), func(a arrow.Array) valueArray[float32] { return a.(*array.Float32) }), nil
	case arrow.FLOAT64:
		return reduceTyped[float64](chunked.Chunks(), func(a arrow.Array) valueArray[float64] { return a.(*array.Float64) }), nil
	case arrow.STRING:
		return reduceOrdered[string](chunked.Chunks(), func(a arrow.Array) valueArray[string] { return a.(*array.String) }), nil
	case arrow.LARGE_STRING:
		return reduceOrdered[string](chunked.Chunks(), func(a arrow.Array) valueArray[string] { return a.(*array.LargeString) }), nil
	default:
		return Aggregates{}, fmt.Errorf("compute.Reduce: type %s not supported", chunked.DataType())
	}
}

// reduceOrdered folds an ordered, non-numeric column (e.g. strings) into Count
// and lexicographic Min/Max. Sum and Mean are left zero, as they are undefined.
func reduceOrdered[T cmp.Ordered](chunks []arrow.Array, as func(arrow.Array) valueArray[T]) Aggregates {
	var mn, mx T
	var count int64
	var seen bool

	for _, ch := range chunks {
		va := as(ch)
		for i := 0; i < va.Len(); i++ {
			if va.IsNull(i) {
				continue
			}
			v := va.Value(i)
			if !seen {
				mn, mx, seen = v, v, true
			} else {
				if v < mn {
					mn = v
				}
				if v > mx {
					mx = v
				}
			}
			count++
		}
	}

	ag := Aggregates{Count: count}
	if count > 0 {
		ag.Min = mn
		ag.Max = mx
	}
	return ag
}

// reduceTyped is the single-pass fold shared by every numeric element type.
func reduceTyped[T number](chunks []arrow.Array, as func(arrow.Array) valueArray[T]) Aggregates {
	var sum, mn, mx T
	var count int64
	var seen bool

	for _, ch := range chunks {
		va := as(ch)
		for i := 0; i < va.Len(); i++ {
			if va.IsNull(i) {
				continue
			}
			v := va.Value(i)
			sum += v
			if !seen {
				mn, mx, seen = v, v, true
			} else {
				if v < mn {
					mn = v
				}
				if v > mx {
					mx = v
				}
			}
			count++
		}
	}

	ag := Aggregates{Count: count}
	if count > 0 {
		ag.Sum = sum
		ag.Min = mn
		ag.Max = mx
		ag.Mean = float64(sum) / float64(count)
	}
	return ag
}
