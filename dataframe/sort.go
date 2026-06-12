package dataframe

import (
	"context"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/karthedew/cosma/expr"
	"github.com/karthedew/cosma/internal/compute"
)

// Sort returns a new DataFrame ordered by the given keys. Keys are applied
// lexicographically: the first key is most significant, ties are broken by the
// next, and so on. Each key independently controls direction (Desc) and null
// placement (NullsFirst); by default order is ascending with nulls last. Every
// column is reordered by the same permutation (via compute.Take) so rows stay
// aligned. The sort is stable and the receiver is unchanged.
//
// ctx is checked after the sort permutation is computed and before the (poten-
// tially large) column gather, so a cancelled context short-circuits the work.
func (df *DataFrame) Sort(ctx context.Context, keys ...expr.SortKey) (*DataFrame, error) {
	if len(keys) == 0 {
		return nil, fmt.Errorf("sort: no sort keys")
	}

	sortKeys := make([]compute.SortKey, len(keys))
	for i, k := range keys {
		idx := df.columnIndex(k.Column)
		if idx < 0 {
			return nil, fmt.Errorf("sort: column %q not found", k.Column)
		}
		sortKeys[i] = compute.SortKey{
			Column:     df.cols[idx].Chunked(),
			Descending: k.Descending,
			NullsFirst: k.NullsFirst,
		}
	}

	order, err := compute.SortIndicesMulti(sortKeys)
	if err != nil {
		return nil, fmt.Errorf("sort: %w", err)
	}

	if err := ctx.Err(); err != nil {
		return nil, err
	}

	mem := memory.DefaultAllocator
	series := make([]*Series, len(df.cols))
	for i := range df.cols {
		arr, err := compute.Take(df.cols[i].Chunked(), order, mem)
		if err != nil {
			return nil, fmt.Errorf("sort: column %q: %w", df.cols[i].Name(), err)
		}
		series[i] = seriesFromArray(df.cols[i].Name(), arr)
	}

	return New(series)
}
