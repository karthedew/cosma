package dataframe

import (
	"fmt"

	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"

	"github.com/karthedew/cosma/schema"
)

// RecordBatchIter yields Arrow record batches from a DataFrame, one per segment
// of the canonical chunk layout (see DataFrame.ChunkSizes). Each segment lies
// within a single chunk of every column, so columns with differing chunk
// boundaries are materialized by zero-copy slicing rather than rejected.
type RecordBatchIter struct {
	df      *DataFrame
	schema  *arrow.Schema
	offsets []int64   // canonical segment boundaries; len == numSegments+1
	colCum  [][]int64 // per-column cumulative chunk offsets
	segIdx  int
}

func NewRecordBatchIter(df *DataFrame) (*RecordBatchIter, error) {
	return NewRecordBatchIterWithSchema(df, nil)
}

func NewRecordBatchIterWithSchema(df *DataFrame, arrSchema *arrow.Schema) (*RecordBatchIter, error) {
	if df == nil {
		return nil, fmt.Errorf("df is nil")
	}
	if arrSchema == nil {
		var err error
		arrSchema, err = schema.ToArrow(df.schema)
		if err != nil {
			return nil, fmt.Errorf("arrow schema: %w", err)
		}
	}

	colCum := make([][]int64, len(df.cols))
	for i := range df.cols {
		chunked := df.cols[i].Chunked()
		if chunked == nil {
			return nil, fmt.Errorf("column %q has no data", df.cols[i].Name())
		}
		colCum[i] = columnOffsets(chunked)
	}

	return &RecordBatchIter{
		df:      df,
		schema:  arrSchema,
		offsets: df.chunkOffsets(),
		colCum:  colCum,
	}, nil
}

func (it *RecordBatchIter) Schema() *arrow.Schema {
	if it == nil {
		return nil
	}
	return it.schema
}

func (it *RecordBatchIter) Next() (arrow.Record, bool, error) {
	if it == nil || it.segIdx >= len(it.offsets)-1 {
		return nil, false, nil
	}

	lo := it.offsets[it.segIdx]
	hi := it.offsets[it.segIdx+1]

	fields := it.df.schema.Fields()
	arrs := make([]arrow.Array, len(fields))
	for i := range fields {
		arr, err := sliceColumnRange(it.df.cols[i].Chunked().Chunks(), it.colCum[i], lo, hi)
		if err != nil {
			return nil, false, fmt.Errorf("column %q: %w", fields[i].Name, err)
		}
		arrs[i] = arr
	}

	rec := array.NewRecord(it.schema, arrs, hi-lo)
	// NewRecord retains each column, so release the slices we created.
	for _, a := range arrs {
		a.Release()
	}
	it.segIdx++
	return rec, true, nil
}

// sliceColumnRange returns the rows [lo, hi) of a column as a single array. The
// canonical layout guarantees the range lies within one chunk, so this is a
// zero-copy slice. cum holds the column's cumulative chunk offsets.
func sliceColumnRange(chunks []arrow.Array, cum []int64, lo, hi int64) (arrow.Array, error) {
	for c := 0; c < len(chunks); c++ {
		if cum[c] == cum[c+1] {
			continue // skip empty chunk
		}
		if lo >= cum[c] && hi <= cum[c+1] {
			return array.NewSlice(chunks[c], lo-cum[c], hi-cum[c]), nil
		}
	}
	return nil, fmt.Errorf("range [%d,%d) not contained in a single chunk", lo, hi)
}
