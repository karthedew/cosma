package dataframe

import (
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
)

// Concat stacks DataFrames vertically (row-wise). Every input must share the
// same column names, order, and types. The result's columns hold the
// concatenation of each input's chunks, so it is zero-copy and preserves chunk
// boundaries. Inputs are unchanged.
func Concat(dfs ...*DataFrame) (*DataFrame, error) {
	if len(dfs) == 0 {
		return nil, fmt.Errorf("concat: no frames")
	}
	for i, d := range dfs {
		if d == nil {
			return nil, fmt.Errorf("concat: frame %d is nil", i)
		}
	}

	first := dfs[0]
	ncol := first.NumCols()
	names := first.Columns()

	// Validate every other frame matches the first's shape and types.
	for i := 1; i < len(dfs); i++ {
		d := dfs[i]
		if d.NumCols() != ncol {
			return nil, fmt.Errorf("concat: frame %d has %d columns, want %d", i, d.NumCols(), ncol)
		}
		for c := 0; c < ncol; c++ {
			if d.cols[c].Name() != names[c] {
				return nil, fmt.Errorf("concat: frame %d column %d is %q, want %q", i, c, d.cols[c].Name(), names[c])
			}
			if !arrow.TypeEqual(d.cols[c].DataType(), first.cols[c].DataType()) {
				return nil, fmt.Errorf("concat: frame %d column %q type %s != %s", i, names[c], d.cols[c].DataType(), first.cols[c].DataType())
			}
		}
	}

	series := make([]*Series, ncol)
	for c := 0; c < ncol; c++ {
		var chunks []arrow.Array
		for _, d := range dfs {
			chunks = append(chunks, d.cols[c].Chunked().Chunks()...)
		}
		// NewChunked retains each chunk; the source frames keep their own refs.
		ch := arrow.NewChunked(first.cols[c].DataType(), chunks)
		series[c] = NewSeriesFromChunked(names[c], ch)
	}

	return New(series)
}

// HStack appends the columns of others to the receiver, horizontally. Every
// frame must have the same number of rows, and column names must stay unique
// across the union. Columns are shared (zero-copy); the receiver is unchanged.
func (df *DataFrame) HStack(others ...*DataFrame) (*DataFrame, error) {
	series := make([]*Series, 0, df.NumCols())
	for i := range df.cols {
		c := df.cols[i]
		series = append(series, &c)
	}
	for i, o := range others {
		if o == nil {
			return nil, fmt.Errorf("hstack: frame %d is nil", i)
		}
		for j := range o.cols {
			c := o.cols[j]
			series = append(series, &c)
		}
	}
	// New enforces unique names and equal column lengths (matching heights).
	out, err := New(series)
	if err != nil {
		return nil, fmt.Errorf("hstack: %w", err)
	}
	return out, nil
}
