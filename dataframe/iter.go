package dataframe

import (
	"fmt"

	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"
)

// RecordBatchIter yields Arrow Records (RecordBatches) from a DataFrame.
// Polars often aligns by chunk index; you can do the same.
type RecordBatchIter struct {
	df        *DataFrame
	chunkIdx  int
	maxChunks int
	schema    *arrow.Schema
}

func NewRecordBatchIter(df *DataFrame) (*RecordBatchIter, error) {
	return NewRecordBatchIterWithSchema(df, nil)
}

func NewRecordBatchIterWithSchema(df *DataFrame, arrSchema *arrow.Schema) (*RecordBatchIter, error) {
	// Minimal stub: assumes all series columns have the same number of chunks
	// and ignores scalar columns.
	// TODO: support scalars (materialize) and misaligned chunks (slice by row range).
	if df == nil {
		return nil, fmt.Errorf("df is nil")
	}
	if arrSchema == nil {
		var err error
		arrSchema, err = arrowSchemaFromSchema(df.schema)
		if err != nil {
			return nil, fmt.Errorf("arrow schema: %w", err)
		}
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
	return &RecordBatchIter{df: df, chunkIdx: 0, maxChunks: max, schema: arrSchema}, nil
}

func (it *RecordBatchIter) Next() (arrow.Record, bool, error) {
	if it.chunkIdx >= it.maxChunks {
		return nil, false, nil
	}

	fields := it.df.schema.Fields()
	arrs := make([]arrow.Array, len(fields))
	var rows int64 = -1

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
			return nil, false, fmt.Errorf("column %q has fewer chunks (%d) than expected (%d)", fields[i].Name, len(chunks), it.maxChunks)
		}
		arrs[i] = chunks[it.chunkIdx]
		if arrs[i] == nil {
			return nil, false, fmt.Errorf("column %q chunk %d is nil", fields[i].Name, it.chunkIdx)
		}
		if rows == -1 {
			rows = int64(arrs[i].Len())
		} else if int64(arrs[i].Len()) != rows {
			return nil, false, fmt.Errorf("column %q chunk %d len=%d != %d", fields[i].Name, it.chunkIdx, arrs[i].Len(), rows)
		}
	}

	it.chunkIdx++
	if rows < 0 {
		rows = 0
	}
	rec := array.NewRecord(it.schema, arrs, rows)
	return rec, true, nil
}
