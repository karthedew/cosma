package dataframe

import (
	"context"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/karthedew/cosma/expr"
	"github.com/karthedew/cosma/internal/compute"
)

// Filter returns a new DataFrame containing only the rows for which predicate
// evaluates to true. predicate must be a boolean expression over the frame's
// columns; rows where it evaluates to null are dropped. The receiver is
// unchanged — surviving rows are copied into freshly-allocated columns.
//
// When compute.Parallelism() != 1 and the frame has more than one chunk, the
// per-chunk eval+filter work is fanned out across compute.Parallelism()
// goroutines (defaulting to GOMAXPROCS). For a single chunk or parallelism==1
// the serial path is taken. ctx is checked between record batches so a
// cancelled context stops the scan early.
func (df *DataFrame) Filter(ctx context.Context, predicate expr.Expr) (*DataFrame, error) {
	if predicate.Node == nil {
		return nil, fmt.Errorf("filter: nil predicate")
	}

	iter, err := NewRecordBatchIter(df)
	if err != nil {
		return nil, err
	}
	arrSchema := iter.Schema()
	mem := memory.DefaultAllocator

	// Collect all record batches from the iterator.
	allBatches := make([]arrow.Record, 0, df.NumChunks())
	for {
		if err := ctx.Err(); err != nil {
			releaseRecords(allBatches)
			return nil, err
		}
		rec, ok, err := iter.Next()
		if err != nil {
			releaseRecords(allBatches)
			return nil, err
		}
		if !ok {
			break
		}
		allBatches = append(allBatches, rec)
	}
	// allBatches are owned here; release on all exit paths.
	defer releaseRecords(allBatches)

	filtered, err := filterBatches(ctx, predicate.Node, allBatches, mem)
	if err != nil {
		return nil, fmt.Errorf("filter: %w", err)
	}
	// filtered records are newly allocated; release after FromRecordBatches
	// retains the column arrays.
	defer releaseRecords(filtered)

	return FromRecordBatches(arrSchema, filtered)
}

// filterBatches applies the predicate to each batch, using the parallel path
// when there are multiple batches and parallelism is not forced to 1.
func filterBatches(
	ctx context.Context,
	predicate expr.ExprNode,
	batches []arrow.Record,
	mem memory.Allocator,
) ([]arrow.Record, error) {
	if len(batches) > 1 && compute.Parallelism() != 1 {
		return compute.EvalParallel(ctx, predicate, batches, mem)
	}
	return filterBatchesSerial(ctx, predicate, batches, mem)
}

// filterBatchesSerial is the original serial filter loop.
func filterBatchesSerial(
	ctx context.Context,
	predicate expr.ExprNode,
	batches []arrow.Record,
	mem memory.Allocator,
) ([]arrow.Record, error) {
	out := make([]arrow.Record, 0, len(batches))
	for _, rec := range batches {
		if err := ctx.Err(); err != nil {
			releaseRecords(out)
			return nil, err
		}
		mask, err := compute.Eval(predicate, rec, mem)
		if err != nil {
			releaseRecords(out)
			return nil, err
		}
		filtered, err := compute.FilterRecord(rec, mask, mem)
		mask.Release()
		if err != nil {
			releaseRecords(out)
			return nil, err
		}
		out = append(out, filtered)
	}
	return out, nil
}
