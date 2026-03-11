package dataframe

import (
	"fmt"

	"github.com/apache/arrow/go/v18/arrow"
)

// RecordBatchIter yields Arrow Records (RecordBatches) from a DataFrame.
// Polars often aligns by chunk index; you can do the same.
type RecordBatchIter struct {
	df        *DataFrame
	chunkIdx  int
	maxChunks int
}

func NewRecordBatchIter(df *DataFrame) (*RecordBatchIter, error) {
	// Minimal stub: assumes all series columns have the same number of chunks
	// and ignores scalar columns.
	// TODO: support scalars (materialize) and misaligned chunks (slice by row range).
	if df == nil {
		return nil, fmt.Errorf("df is nil")
	}

	// Find max chunks across series
	max := 0
	for _, c := range df.cols {
		chunked := c.Chunked()
		if chunked != nil {
			if n := len(chunked.Chunks()); n > max {
				max = n
			}
		}
	}
	return &RecordBatchIter{df: df, chunkIdx: 0, maxChunks: max}, nil
}

func (it *RecordBatchIter) Next() (arrow.Record, bool, error) {
	if it.chunkIdx >= it.maxChunks {
		return nil, false, nil
	}

	fields := it.df.schema.Fields()
	arrs := make([]arrow.Array, len(fields))

	for i := range fields {
		col := it.df.cols[i]
		chunked := col.Chunked()
		if chunked == nil {
			// TODO: handle missing chunked column
			arrs[i] = nil
			continue
		}

		chunks := chunked.Chunks()
		if it.chunkIdx >= len(chunks) {
			// TODO: handle uneven chunk counts
			arrs[i] = chunks[len(chunks)-1]
		} else {
			arrs[i] = chunks[it.chunkIdx]
		}
	}

	// TODO: build a real arrow.Schema from df.schema (Cosma schema) + Arrow dtypes.
	// For now, use a placeholder Arrow schema with unknown types would not work.
	// Return "not implemented" to avoid emitting invalid records.
	it.chunkIdx++
	return nil, false, fmt.Errorf("RecordBatchIter.Next not implemented: need Arrow schema construction + scalar handling")
}
