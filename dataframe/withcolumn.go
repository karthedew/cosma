package dataframe

import (
	"fmt"

	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/memory"

	"github.com/karthedew/cosma/internal/compute"
	"github.com/karthedew/cosma/internal/expr"
)

// WithColumn returns a new DataFrame with a column named name set to the result
// of evaluating e over each row. If a column with that name already exists it is
// replaced in place; otherwise the new column is appended. The receiver is
// unchanged.
//
// e is evaluated one canonical chunk at a time and the per-segment results are
// reassembled into a single chunked column aligned to the frame's height.
func (df *DataFrame) WithColumn(name string, e expr.Expr) (*DataFrame, error) {
	if name == "" {
		return nil, fmt.Errorf("withcolumn: empty column name")
	}
	if e == nil {
		return nil, fmt.Errorf("withcolumn: nil expression")
	}

	iter, err := NewRecordBatchIter(df)
	if err != nil {
		return nil, err
	}
	mem := memory.DefaultAllocator

	var dtype arrow.DataType
	segments := make([]arrow.Array, 0, df.NumChunks())
	release := func() {
		for _, a := range segments {
			a.Release()
		}
	}

	for {
		rec, ok, err := iter.Next()
		if err != nil {
			release()
			return nil, err
		}
		if !ok {
			break
		}
		arr, err := compute.Eval(e, rec, mem)
		rec.Release()
		if err != nil {
			release()
			return nil, fmt.Errorf("withcolumn: %w", err)
		}
		if dtype == nil {
			dtype = arr.DataType()
		}
		segments = append(segments, arr)
	}

	if dtype == nil {
		release()
		return nil, fmt.Errorf("withcolumn: cannot derive %q from an empty frame", name)
	}

	chunked := arrow.NewChunked(dtype, segments)
	release() // NewChunked retained the segments; the Series now owns chunked

	newCol := NewSeriesFromChunked(name, chunked)

	idx := df.columnIndex(name)
	series := make([]*Series, 0, len(df.cols)+1)
	for i := range df.cols {
		if i == idx {
			series = append(series, newCol)
			continue
		}
		c := df.cols[i]
		series = append(series, &c)
	}
	if idx < 0 {
		series = append(series, newCol)
	}

	return New(series)
}
