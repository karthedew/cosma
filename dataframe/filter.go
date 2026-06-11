package dataframe

import (
	"fmt"

	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/memory"

	"github.com/karthedew/cosma/expr"
	"github.com/karthedew/cosma/internal/compute"
)

// Filter returns a new DataFrame containing only the rows for which predicate
// evaluates to true. predicate must be a boolean expression over the frame's
// columns; rows where it evaluates to null are dropped. The receiver is
// unchanged — surviving rows are copied into freshly-allocated columns.
//
// The predicate is evaluated one canonical chunk at a time, so a frame with
// misaligned column chunking is filtered without first being rechunked.
func (df *DataFrame) Filter(predicate expr.Expr) (*DataFrame, error) {
	if predicate.Node == nil {
		return nil, fmt.Errorf("filter: nil predicate")
	}

	iter, err := NewRecordBatchIter(df)
	if err != nil {
		return nil, err
	}
	arrSchema := iter.Schema()
	mem := memory.DefaultAllocator

	filtered := make([]arrow.Record, 0, df.NumChunks())
	defer releaseRecords(filtered)

	for {
		rec, ok, err := iter.Next()
		if err != nil {
			return nil, err
		}
		if !ok {
			break
		}

		mask, err := compute.Eval(predicate.Node, rec, mem)
		if err != nil {
			rec.Release()
			return nil, fmt.Errorf("filter: %w", err)
		}
		out, err := compute.FilterRecord(rec, mask, mem)
		mask.Release()
		rec.Release()
		if err != nil {
			return nil, fmt.Errorf("filter: %w", err)
		}
		filtered = append(filtered, out)
	}

	return FromRecordBatches(arrSchema, filtered)
}
