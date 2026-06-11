package dataframe

import (
	"fmt"

	"github.com/apache/arrow/go/v18/arrow/memory"

	"github.com/karthedew/cosma/internal/compute"
)

// Sort returns a new DataFrame ordered by the named column. With descending
// false the order is ascending; nulls always sort last. Every column is
// reordered by the same permutation, so rows stay aligned. The sort is stable
// and the receiver is unchanged.
func (df *DataFrame) Sort(col string, descending bool) (*DataFrame, error) {
	idx := df.columnIndex(col)
	if idx < 0 {
		return nil, fmt.Errorf("sort: column %q not found", col)
	}

	order, err := compute.SortIndices(df.cols[idx].Chunked(), descending)
	if err != nil {
		return nil, fmt.Errorf("sort: %w", err)
	}

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
