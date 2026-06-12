package compute

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/memory"

	"github.com/karthedew/cosma/expr"
)

// chunkWork is one unit of work handed to a filter worker: the chunk index and
// the pre-sliced record batch.
type chunkWork struct {
	idx int
	rec arrow.Record
}

// chunkResult is the output of one filter worker.
type chunkResult struct {
	idx int
	rec arrow.Record
	err error
}

// EvalParallel evaluates predicate and applies FilterRecord across each
// record batch produced by iter, fanning the work out over up to workers()
// goroutines. The output slices are returned in the same order as the input
// batches.
//
// It is the parallel counterpart of the serial loop in dataframe.Filter; the
// dataframe package calls it when Parallelism() > 1.
//
// Ownership: each returned arrow.Record is newly allocated and owned by the
// caller. ctx cancellation causes an early return; already-filtered records are
// released before returning the context error.
func EvalParallel(
	ctx context.Context,
	predicate expr.ExprNode,
	batches []arrow.Record,
	mem memory.Allocator,
) ([]arrow.Record, error) {
	if len(batches) == 0 {
		return nil, nil
	}
	if mem == nil {
		mem = memory.DefaultAllocator
	}

	n := len(batches)
	w := workers()
	if w > n {
		w = n
	}

	start := time.Now()
	var totalRows int64
	for _, b := range batches {
		totalRows += b.NumRows()
	}

	workCh := make(chan chunkWork, n)
	for i, rec := range batches {
		workCh <- chunkWork{idx: i, rec: rec}
	}
	close(workCh)

	resultCh := make(chan chunkResult, n)

	var wg sync.WaitGroup
	wg.Add(w)
	for range w {
		go func() {
			defer wg.Done()
			for cw := range workCh {
				if ctx.Err() != nil {
					resultCh <- chunkResult{idx: cw.idx, err: ctx.Err()}
					continue
				}
				mask, err := Eval(predicate, cw.rec, mem)
				if err != nil {
					resultCh <- chunkResult{idx: cw.idx, err: fmt.Errorf("chunk %d eval: %w", cw.idx, err)}
					continue
				}
				out, err := FilterRecord(cw.rec, mask, mem)
				mask.Release()
				if err != nil {
					resultCh <- chunkResult{idx: cw.idx, err: fmt.Errorf("chunk %d filter: %w", cw.idx, err)}
					continue
				}
				resultCh <- chunkResult{idx: cw.idx, rec: out}
			}
		}()
	}

	go func() {
		wg.Wait()
		close(resultCh)
	}()

	results := make([]arrow.Record, n)
	var firstErr error
	for r := range resultCh {
		if r.err != nil && firstErr == nil {
			firstErr = r.err
		}
		if r.rec != nil {
			results[r.idx] = r.rec
		}
	}

	if firstErr != nil {
		for _, rec := range results {
			if rec != nil {
				rec.Release()
			}
		}
		return nil, firstErr
	}

	storeMetrics(OpMetrics{
		Op:      "filter",
		Workers: w,
		Rows:    totalRows,
		Elapsed: time.Since(start),
	})

	return results, nil
}
