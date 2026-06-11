package compute

import (
	"cmp"
	"fmt"

	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"
	"github.com/apache/arrow/go/v18/arrow/memory"
)

// GroupReduce folds a chunked column into per-group Aggregates. groupIDs holds
// one group index per logical row, in row order across chunks, so it must have
// length chunked.Len(); numGroups is the number of distinct groups. The result
// is indexed by group id. It is the per-column half of a two-phase GroupBy: the
// caller assigns group ids once and reduces every value column against them.
func GroupReduce(groupIDs []int, numGroups int, chunked *arrow.Chunked) ([]Aggregates, error) {
	if chunked == nil {
		return nil, fmt.Errorf("compute.GroupReduce: nil column")
	}
	if int64(len(groupIDs)) != int64(chunked.Len()) {
		return nil, fmt.Errorf("compute.GroupReduce: %d group ids for %d rows", len(groupIDs), chunked.Len())
	}

	switch chunked.DataType().ID() {
	case arrow.INT8:
		return groupTyped[int8](groupIDs, numGroups, chunked.Chunks(), func(a arrow.Array) valueArray[int8] { return a.(*array.Int8) }), nil
	case arrow.INT16:
		return groupTyped[int16](groupIDs, numGroups, chunked.Chunks(), func(a arrow.Array) valueArray[int16] { return a.(*array.Int16) }), nil
	case arrow.INT32:
		return groupTyped[int32](groupIDs, numGroups, chunked.Chunks(), func(a arrow.Array) valueArray[int32] { return a.(*array.Int32) }), nil
	case arrow.INT64:
		return groupTyped[int64](groupIDs, numGroups, chunked.Chunks(), func(a arrow.Array) valueArray[int64] { return a.(*array.Int64) }), nil
	case arrow.UINT8:
		return groupTyped[uint8](groupIDs, numGroups, chunked.Chunks(), func(a arrow.Array) valueArray[uint8] { return a.(*array.Uint8) }), nil
	case arrow.UINT16:
		return groupTyped[uint16](groupIDs, numGroups, chunked.Chunks(), func(a arrow.Array) valueArray[uint16] { return a.(*array.Uint16) }), nil
	case arrow.UINT32:
		return groupTyped[uint32](groupIDs, numGroups, chunked.Chunks(), func(a arrow.Array) valueArray[uint32] { return a.(*array.Uint32) }), nil
	case arrow.UINT64:
		return groupTyped[uint64](groupIDs, numGroups, chunked.Chunks(), func(a arrow.Array) valueArray[uint64] { return a.(*array.Uint64) }), nil
	case arrow.FLOAT32:
		return groupTyped[float32](groupIDs, numGroups, chunked.Chunks(), func(a arrow.Array) valueArray[float32] { return a.(*array.Float32) }), nil
	case arrow.FLOAT64:
		return groupTyped[float64](groupIDs, numGroups, chunked.Chunks(), func(a arrow.Array) valueArray[float64] { return a.(*array.Float64) }), nil
	case arrow.STRING:
		return groupOrdered[string](groupIDs, numGroups, chunked.Chunks(), func(a arrow.Array) valueArray[string] { return a.(*array.String) }), nil
	case arrow.LARGE_STRING:
		return groupOrdered[string](groupIDs, numGroups, chunked.Chunks(), func(a arrow.Array) valueArray[string] { return a.(*array.LargeString) }), nil
	default:
		// Count is type-agnostic: it only counts non-null values, so any
		// Arrow type (utf8, bool, etc.) can be reduced for AggOpCount even
		// though Sum/Min/Max/Mean are undefined. Other aggregations on a
		// non-numeric column leave their fields zero; callers route only
		// Count to this result.
		return groupCount(groupIDs, numGroups, chunked.Chunks()), nil
	}
}

// groupCount counts non-null values per group regardless of the column's Arrow
// type. Only the Count field of each Aggregates is populated.
func groupCount(groupIDs []int, numGroups int, chunks []arrow.Array) []Aggregates {
	counts := make([]int64, numGroups)
	row := 0
	for _, ch := range chunks {
		for i := 0; i < ch.Len(); i++ {
			g := groupIDs[row]
			row++
			if ch.IsNull(i) {
				continue
			}
			counts[g]++
		}
	}
	out := make([]Aggregates, numGroups)
	for g := 0; g < numGroups; g++ {
		out[g] = Aggregates{Count: counts[g]}
	}
	return out
}

func groupTyped[T number](groupIDs []int, numGroups int, chunks []arrow.Array, as func(arrow.Array) valueArray[T]) []Aggregates {
	sums := make([]T, numGroups)
	mins := make([]T, numGroups)
	maxs := make([]T, numGroups)
	counts := make([]int64, numGroups)
	seen := make([]bool, numGroups)

	row := 0
	for _, ch := range chunks {
		va := as(ch)
		for i := 0; i < va.Len(); i++ {
			g := groupIDs[row]
			row++
			if va.IsNull(i) {
				continue
			}
			v := va.Value(i)
			sums[g] += v
			if !seen[g] {
				mins[g], maxs[g], seen[g] = v, v, true
			} else {
				if v < mins[g] {
					mins[g] = v
				}
				if v > maxs[g] {
					maxs[g] = v
				}
			}
			counts[g]++
		}
	}

	out := make([]Aggregates, numGroups)
	for g := 0; g < numGroups; g++ {
		ag := Aggregates{Count: counts[g]}
		if counts[g] > 0 {
			ag.Sum = sums[g]
			ag.Min = mins[g]
			ag.Max = maxs[g]
			ag.Mean = float64(sums[g]) / float64(counts[g])
		}
		out[g] = ag
	}
	return out
}

// groupOrdered folds an ordered, non-numeric column (e.g. strings) into
// per-group Count and lexicographic Min/Max. Sum and Mean stay zero.
func groupOrdered[T cmp.Ordered](groupIDs []int, numGroups int, chunks []arrow.Array, as func(arrow.Array) valueArray[T]) []Aggregates {
	mins := make([]T, numGroups)
	maxs := make([]T, numGroups)
	counts := make([]int64, numGroups)
	seen := make([]bool, numGroups)

	row := 0
	for _, ch := range chunks {
		va := as(ch)
		for i := 0; i < va.Len(); i++ {
			g := groupIDs[row]
			row++
			if va.IsNull(i) {
				continue
			}
			v := va.Value(i)
			if !seen[g] {
				mins[g], maxs[g], seen[g] = v, v, true
			} else {
				if v < mins[g] {
					mins[g] = v
				}
				if v > maxs[g] {
					maxs[g] = v
				}
			}
			counts[g]++
		}
	}

	out := make([]Aggregates, numGroups)
	for g := 0; g < numGroups; g++ {
		ag := Aggregates{Count: counts[g]}
		if counts[g] > 0 {
			ag.Min = mins[g]
			ag.Max = maxs[g]
		}
		out[g] = ag
	}
	return out
}

// BoxedValues reads a chunked column into one boxed Go value per row, in row
// order, with nil for nulls. It backs group-key construction where the key type
// is only known at runtime.
func BoxedValues(chunked *arrow.Chunked) ([]any, error) {
	if chunked == nil {
		return nil, fmt.Errorf("compute.BoxedValues: nil column")
	}
	out := make([]any, 0, chunked.Len())
	for _, ch := range chunked.Chunks() {
		switch chunked.DataType().ID() {
		case arrow.INT8:
			box[int8](ch.(*array.Int8), &out)
		case arrow.INT16:
			box[int16](ch.(*array.Int16), &out)
		case arrow.INT32:
			box[int32](ch.(*array.Int32), &out)
		case arrow.INT64:
			box[int64](ch.(*array.Int64), &out)
		case arrow.UINT8:
			box[uint8](ch.(*array.Uint8), &out)
		case arrow.UINT16:
			box[uint16](ch.(*array.Uint16), &out)
		case arrow.UINT32:
			box[uint32](ch.(*array.Uint32), &out)
		case arrow.UINT64:
			box[uint64](ch.(*array.Uint64), &out)
		case arrow.FLOAT32:
			box[float32](ch.(*array.Float32), &out)
		case arrow.FLOAT64:
			box[float64](ch.(*array.Float64), &out)
		case arrow.STRING:
			box[string](ch.(*array.String), &out)
		case arrow.BOOL:
			box[bool](ch.(*array.Boolean), &out)
		default:
			return nil, fmt.Errorf("compute.BoxedValues: type %s not supported", chunked.DataType())
		}
	}
	return out, nil
}

func box[T any](va valueArray[T], out *[]any) {
	for i := 0; i < va.Len(); i++ {
		if va.IsNull(i) {
			*out = append(*out, nil)
			continue
		}
		*out = append(*out, va.Value(i))
	}
}

// BuildArray materializes a slice of boxed values into an Arrow array of the
// given type. A nil element becomes a null. It is the inverse of BoxedValues and
// is used to emit group keys and reduction results.
func BuildArray(dtype arrow.DataType, vals []any, mem memory.Allocator) (arrow.Array, error) {
	if mem == nil {
		mem = memory.DefaultAllocator
	}
	switch dtype.ID() {
	case arrow.INT8:
		return build[int8](array.NewInt8Builder(mem), vals)
	case arrow.INT16:
		return build[int16](array.NewInt16Builder(mem), vals)
	case arrow.INT32:
		return build[int32](array.NewInt32Builder(mem), vals)
	case arrow.INT64:
		return build[int64](array.NewInt64Builder(mem), vals)
	case arrow.UINT8:
		return build[uint8](array.NewUint8Builder(mem), vals)
	case arrow.UINT16:
		return build[uint16](array.NewUint16Builder(mem), vals)
	case arrow.UINT32:
		return build[uint32](array.NewUint32Builder(mem), vals)
	case arrow.UINT64:
		return build[uint64](array.NewUint64Builder(mem), vals)
	case arrow.FLOAT32:
		return build[float32](array.NewFloat32Builder(mem), vals)
	case arrow.FLOAT64:
		return build[float64](array.NewFloat64Builder(mem), vals)
	case arrow.STRING:
		return build[string](array.NewStringBuilder(mem), vals)
	case arrow.BOOL:
		return build[bool](array.NewBooleanBuilder(mem), vals)
	default:
		return nil, fmt.Errorf("compute.BuildArray: type %s not supported", dtype)
	}
}

func build[T any](b primBuilder[T], vals []any) (arrow.Array, error) {
	defer b.Release()
	b.Reserve(len(vals))
	for _, v := range vals {
		if v == nil {
			b.AppendNull()
			continue
		}
		x, ok := v.(T)
		if !ok {
			var zero T
			return nil, fmt.Errorf("compute.BuildArray: expected %T, got %T", zero, v)
		}
		b.Append(x)
	}
	return b.NewArray(), nil
}
