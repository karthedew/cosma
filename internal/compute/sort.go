package compute

import (
	"cmp"
	"fmt"
	"sort"

	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"
)

// SortKey names a column-sort spec resolved against a chunked column: the column
// to compare plus its direction and null placement.
type SortKey struct {
	Column     *arrow.Chunked
	Descending bool
	NullsFirst bool
}

// SortIndices returns a stable permutation of row indices that orders chunked by
// value. By default nulls sort last; set nullsFirst to place them first. The
// result is a take/gather permutation: reorder every column of a DataFrame by it
// to keep rows aligned.
func SortIndices(chunked *arrow.Chunked, descending, nullsFirst bool) ([]int64, error) {
	if chunked == nil {
		return nil, fmt.Errorf("compute.SortIndices: nil column")
	}
	cmpFn, err := comparatorFor(chunked)
	if err != nil {
		return nil, err
	}
	n := chunked.Len()
	idx := identity(n)
	sort.SliceStable(idx, func(i, j int) bool {
		return cmpFn(idx[i], idx[j], descending, nullsFirst) < 0
	})
	return idx, nil
}

// SortIndicesMulti returns a stable permutation ordering rows lexicographically
// by keys: keys[0] is most significant, ties broken by keys[1], and so on. Each
// key carries its own direction and null placement. All key columns must share
// the same logical length.
func SortIndicesMulti(keys []SortKey) ([]int64, error) {
	if len(keys) == 0 {
		return nil, fmt.Errorf("compute.SortIndicesMulti: no keys")
	}
	cmps := make([]rowComparator, len(keys))
	n := -1
	for i, k := range keys {
		if k.Column == nil {
			return nil, fmt.Errorf("compute.SortIndicesMulti: key %d has nil column", i)
		}
		if n == -1 {
			n = k.Column.Len()
		} else if k.Column.Len() != n {
			return nil, fmt.Errorf("compute.SortIndicesMulti: key %d length %d != %d", i, k.Column.Len(), n)
		}
		c, err := comparatorFor(k.Column)
		if err != nil {
			return nil, err
		}
		cmps[i] = c
	}

	idx := identity(n)
	sort.SliceStable(idx, func(i, j int) bool {
		a, b := idx[i], idx[j]
		for ki := range keys {
			c := cmps[ki](a, b, keys[ki].Descending, keys[ki].NullsFirst)
			if c != 0 {
				return c < 0
			}
		}
		return false
	})
	return idx, nil
}

func identity(n int) []int64 {
	idx := make([]int64, n)
	for i := range idx {
		idx[i] = int64(i)
	}
	return idx
}

// rowComparator orders two logical row indices of one column, returning -1, 0,
// or +1, honoring descending direction and null placement.
type rowComparator func(a, b int64, descending, nullsFirst bool) int

// comparatorFor flattens a chunked column once and returns a comparator over its
// logical row indices.
func comparatorFor(chunked *arrow.Chunked) (rowComparator, error) {
	switch chunked.DataType().ID() {
	case arrow.INT8:
		return rowCmp(flatten[int8](chunked.Chunks(), func(a arrow.Array) valueArray[int8] { return a.(*array.Int8) })), nil
	case arrow.INT16:
		return rowCmp(flatten[int16](chunked.Chunks(), func(a arrow.Array) valueArray[int16] { return a.(*array.Int16) })), nil
	case arrow.INT32:
		return rowCmp(flatten[int32](chunked.Chunks(), func(a arrow.Array) valueArray[int32] { return a.(*array.Int32) })), nil
	case arrow.INT64:
		return rowCmp(flatten[int64](chunked.Chunks(), func(a arrow.Array) valueArray[int64] { return a.(*array.Int64) })), nil
	case arrow.UINT8:
		return rowCmp(flatten[uint8](chunked.Chunks(), func(a arrow.Array) valueArray[uint8] { return a.(*array.Uint8) })), nil
	case arrow.UINT16:
		return rowCmp(flatten[uint16](chunked.Chunks(), func(a arrow.Array) valueArray[uint16] { return a.(*array.Uint16) })), nil
	case arrow.UINT32:
		return rowCmp(flatten[uint32](chunked.Chunks(), func(a arrow.Array) valueArray[uint32] { return a.(*array.Uint32) })), nil
	case arrow.UINT64:
		return rowCmp(flatten[uint64](chunked.Chunks(), func(a arrow.Array) valueArray[uint64] { return a.(*array.Uint64) })), nil
	case arrow.FLOAT32:
		return rowCmp(flatten[float32](chunked.Chunks(), func(a arrow.Array) valueArray[float32] { return a.(*array.Float32) })), nil
	case arrow.FLOAT64:
		return rowCmp(flatten[float64](chunked.Chunks(), func(a arrow.Array) valueArray[float64] { return a.(*array.Float64) })), nil
	case arrow.STRING:
		return rowCmp(flatten[string](chunked.Chunks(), func(a arrow.Array) valueArray[string] { return a.(*array.String) })), nil
	case arrow.LARGE_STRING:
		return rowCmp(flatten[string](chunked.Chunks(), func(a arrow.Array) valueArray[string] { return a.(*array.LargeString) })), nil
	default:
		return nil, fmt.Errorf("compute.SortIndices: type %s not supported", chunked.DataType())
	}
}

// flat is a chunked column flattened to a contiguous typed slice plus a validity
// mask, so the comparator can index rows in O(1).
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

// rowCmp closes over a flattened column and returns a comparator that, given two
// logical indices, orders them by value with null and direction handling. The
// returned value is direction-applied: a < b means a sorts before b.
func rowCmp[T cmp.Ordered](f flat[T]) rowComparator {
	return func(a, b int64, descending, nullsFirst bool) int {
		va, vb := f.valid[a], f.valid[b]
		if !va || !vb {
			if !va && !vb {
				return 0
			}
			// One side is null. Null placement is independent of direction.
			if nullsFirst {
				if !va {
					return -1
				}
				return 1
			}
			if !va {
				return 1
			}
			return -1
		}
		var c int
		switch {
		case f.vals[a] < f.vals[b]:
			c = -1
		case f.vals[a] > f.vals[b]:
			c = 1
		default:
			c = 0
		}
		if descending {
			return -c
		}
		return c
	}
}
