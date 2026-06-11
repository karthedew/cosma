package dataframe

import (
	"fmt"
	"sort"

	"github.com/apache/arrow/go/v18/arrow/memory"

	"github.com/karthedew/cosma/expr"
	"github.com/karthedew/cosma/internal/compute"
)

// Sort returns a new DataFrame ordered by the given keys. Keys are applied
// lexicographically: the first key is most significant, ties are broken by the
// next, and so on. Each key independently controls direction (Desc) and null
// placement (NullsFirst); by default order is ascending with nulls last. Every
// column is reordered by the same permutation so rows stay aligned. The sort is
// stable and the receiver is unchanged.
func (df *DataFrame) Sort(keys ...expr.SortKey) (*DataFrame, error) {
	if len(keys) == 0 {
		return nil, fmt.Errorf("sort: no sort keys")
	}

	type keyData struct {
		vals       []any
		descending bool
		nullsFirst bool
	}
	resolved := make([]keyData, len(keys))
	for i, k := range keys {
		idx := df.columnIndex(k.Column)
		if idx < 0 {
			return nil, fmt.Errorf("sort: column %q not found", k.Column)
		}
		boxed, err := compute.BoxedValues(df.cols[idx].Chunked())
		if err != nil {
			return nil, fmt.Errorf("sort: column %q: %w", k.Column, err)
		}
		resolved[i] = keyData{vals: boxed, descending: k.Descending, nullsFirst: k.NullsFirst}
	}

	n := int(df.NumRows())
	order := make([]int, n)
	for i := range order {
		order[i] = i
	}

	sort.SliceStable(order, func(i, j int) bool {
		a, b := order[i], order[j]
		for _, kd := range resolved {
			c := compareBoxed(kd.vals[a], kd.vals[b], kd.nullsFirst)
			if c == 0 {
				continue
			}
			if kd.descending {
				return c > 0
			}
			return c < 0
		}
		return false
	})

	mem := memory.DefaultAllocator
	series := make([]*Series, len(df.cols))
	for i := range df.cols {
		boxed, err := compute.BoxedValues(df.cols[i].Chunked())
		if err != nil {
			return nil, fmt.Errorf("sort: column %q: %w", df.cols[i].Name(), err)
		}
		reordered := make([]any, len(order))
		for r, o := range order {
			reordered[r] = boxed[o]
		}
		arr, err := compute.BuildArray(df.cols[i].DataType(), reordered, mem)
		if err != nil {
			return nil, fmt.Errorf("sort: column %q: %w", df.cols[i].Name(), err)
		}
		series[i] = seriesFromArray(df.cols[i].Name(), arr)
	}

	return New(series)
}

// compareBoxed orders two boxed column values for sorting, returning -1, 0, or
// +1. Nulls (nil) order last by default, or first when nullsFirst is set. The
// comparison is direction-agnostic; callers flip the result for descending.
func compareBoxed(a, b any, nullsFirst bool) int {
	aNull, bNull := a == nil, b == nil
	if aNull || bNull {
		if aNull && bNull {
			return 0
		}
		if nullsFirst {
			if aNull {
				return -1
			}
			return 1
		}
		if aNull {
			return 1
		}
		return -1
	}
	return compareNonNull(a, b)
}

func compareNonNull(a, b any) int {
	switch x := a.(type) {
	case int8:
		return cmpOrdered(x, b.(int8))
	case int16:
		return cmpOrdered(x, b.(int16))
	case int32:
		return cmpOrdered(x, b.(int32))
	case int64:
		return cmpOrdered(x, b.(int64))
	case uint8:
		return cmpOrdered(x, b.(uint8))
	case uint16:
		return cmpOrdered(x, b.(uint16))
	case uint32:
		return cmpOrdered(x, b.(uint32))
	case uint64:
		return cmpOrdered(x, b.(uint64))
	case float32:
		return cmpOrdered(x, b.(float32))
	case float64:
		return cmpOrdered(x, b.(float64))
	case string:
		return cmpOrdered(x, b.(string))
	case bool:
		bx := b.(bool)
		switch {
		case x == bx:
			return 0
		case !x:
			return -1
		default:
			return 1
		}
	default:
		return 0
	}
}

type orderedValue interface {
	~int8 | ~int16 | ~int32 | ~int64 |
		~uint8 | ~uint16 | ~uint32 | ~uint64 |
		~float32 | ~float64 | ~string
}

func cmpOrdered[T orderedValue](a, b T) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}
