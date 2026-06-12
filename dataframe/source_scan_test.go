package dataframe_test

import (
	"context"
	"sync"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/require"

	"github.com/karthedew/cosma/dataframe"
	"github.com/karthedew/cosma/expr"
)

// memReader is an in-memory array.RecordReader over a fixed slice of synthetic
// record batches. It is the test stand-in for a custom streaming source: it
// owns its batches and releases them on Release.
type memReader struct {
	schema   *arrow.Schema
	recs     []arrow.Record
	idx      int
	cur      arrow.Record
	hook     func() // called before yielding each batch (cancellation tests)
	mu       sync.Mutex
	retained bool
}

func newMemReader(schema *arrow.Schema, recs []arrow.Record) *memReader {
	for _, r := range recs {
		r.Retain()
	}
	return &memReader{schema: schema, recs: recs, idx: 0, retained: true}
}

func (r *memReader) Retain() {}

func (r *memReader) Release() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.retained {
		return
	}
	r.retained = false
	for _, rec := range r.recs {
		rec.Release()
	}
}

func (r *memReader) Schema() *arrow.Schema          { return r.schema }
func (r *memReader) Record() arrow.Record           { return r.cur }
func (r *memReader) RecordBatch() arrow.RecordBatch { return r.cur }
func (r *memReader) Err() error                     { return nil }

func (r *memReader) Next() bool {
	if r.hook != nil {
		r.hook()
	}
	if r.idx >= len(r.recs) {
		r.cur = nil
		return false
	}
	r.cur = r.recs[r.idx]
	r.idx++
	return true
}

// twoBatchSource builds a SourceScan over two synthetic batches with columns
// x:int64 and age:int64, matching sampleFrame's values split across batches.
func twoBatchSource(t *testing.T, open func(ctx context.Context, hints dataframe.ScanHints) (array.RecordReader, error)) dataframe.SourceScan {
	t.Helper()
	return dataframe.SourceScan{
		Schema: twoBatchSchema(),
		Open:   open,
	}
}

func twoBatchSchema() *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{
		{Name: "x", Type: arrow.PrimitiveTypes.Int64},
		{Name: "age", Type: arrow.PrimitiveTypes.Int64},
	}, nil)
}

// buildBatches materializes the sampleFrame data as two record batches the
// memReader yields in sequence.
func buildBatches(t *testing.T) []arrow.Record {
	t.Helper()
	schema := twoBatchSchema()
	mem := memory.DefaultAllocator

	build := func(x, age []int64) arrow.Record {
		xb := array.NewInt64Builder(mem)
		ab := array.NewInt64Builder(mem)
		defer xb.Release()
		defer ab.Release()
		xb.AppendValues(x, nil)
		ab.AppendValues(age, nil)
		xa := xb.NewArray()
		aa := ab.NewArray()
		defer xa.Release()
		defer aa.Release()
		return array.NewRecord(schema, []arrow.Array{xa, aa}, int64(len(x)))
	}

	b1 := build([]int64{-2, -1, 0, 1}, []int64{30, 25, 40, 20})
	b2 := build([]int64{2, 3, 4, 5}, []int64{35, 50, 10, 45})
	return []arrow.Record{b1, b2}
}

// releaseBatches releases the batches owned by the test (the memReader retains
// its own references).
func releaseBatches(recs []arrow.Record) {
	for _, r := range recs {
		r.Release()
	}
}

// sampleFrameForSource is the eager equivalent of the two-batch source, used for
// parity assertions.
func sampleFrameForSource(t *testing.T) *dataframe.DataFrame {
	t.Helper()
	x, err := dataframe.NewSeries("x", []int64{-2, -1, 0, 1, 2, 3, 4, 5})
	require.NoError(t, err)
	age, err := dataframe.NewSeries("age", []int64{30, 25, 40, 20, 35, 50, 10, 45})
	require.NoError(t, err)
	df, err := dataframe.New([]*dataframe.Series{x, age})
	require.NoError(t, err)
	return df
}

// TestSourceScanCollectParity: an in-memory SourceScan drained eagerly matches
// the equivalent eager DataFrame for a Filter+Select pipeline.
func TestSourceScanCollectParity(t *testing.T) {
	ctx := context.Background()
	batches := buildBatches(t)
	defer releaseBatches(batches)

	ss := twoBatchSource(t, func(ctx context.Context, hints dataframe.ScanHints) (array.RecordReader, error) {
		return newMemReader(twoBatchSchema(), batches), nil
	})

	got, err := dataframe.NewLazySourceScan(ss).
		Filter(expr.Col("x").Gt(expr.Lit(int64(0)))).
		Select("age").
		Collect(ctx)
	require.NoError(t, err)

	eager := sampleFrameForSource(t)
	filtered, err := eager.Filter(ctx, expr.Col("x").Gt(expr.Lit(int64(0))))
	require.NoError(t, err)
	want, err := filtered.Select("age")
	require.NoError(t, err)

	require.Equal(t, colValues(t, want, "age"), colValues(t, got, "age"))
}

// TestSourceScanStreamParity: CollectStream over an in-memory SourceScan matches
// the equivalent eager DataFrame result.
func TestSourceScanStreamParity(t *testing.T) {
	ctx := context.Background()
	batches := buildBatches(t)
	defer releaseBatches(batches)

	ss := twoBatchSource(t, func(ctx context.Context, hints dataframe.ScanHints) (array.RecordReader, error) {
		return newMemReader(twoBatchSchema(), batches), nil
	})

	reader, err := dataframe.NewLazySourceScan(ss).
		Filter(expr.Col("x").Gt(expr.Lit(int64(0)))).
		CollectStream(ctx)
	require.NoError(t, err)
	defer reader.Release()
	got := drainReader(t, reader, "x")

	require.Equal(t, []any{int64(1), int64(2), int64(3), int64(4), int64(5)}, got)
}

// TestSourceScanObservesHints: an Open that records its ScanHints sees the
// pushed filters, columns, and limit from Filter/Select/Limit above the scan.
func TestSourceScanObservesHints(t *testing.T) {
	ctx := context.Background()
	batches := buildBatches(t)
	defer releaseBatches(batches)

	var seen dataframe.ScanHints
	var seenOnce bool
	ss := twoBatchSource(t, func(ctx context.Context, hints dataframe.ScanHints) (array.RecordReader, error) {
		seen = hints
		seenOnce = true
		return newMemReader(twoBatchSchema(), batches), nil
	})

	// Filter -> Select -> Limit above the scan. The optimizer pushes the filter
	// and projection onto the ScanNode, and the executor forwards them to Open
	// as hints. The limit must NOT be pushed: the Filter between the Limit and
	// the scan changes which rows survive, so truncating at the scan would be
	// unsound (see limitScanBelow).
	_, err := dataframe.NewLazySourceScan(ss).
		Filter(expr.Col("x").Gt(expr.Lit(int64(0)))).
		Select("x").
		Limit(2).
		Collect(ctx)
	require.NoError(t, err)

	require.True(t, seenOnce, "Open was not called")
	require.Len(t, seen.Filters, 1, "expected the pushed filter")
	require.Equal(t, []string{"x"}, seen.Columns, "expected the pushed projection")
	require.Equal(t, int64(-1), seen.Limit, "limit must not push across a filter")

	// With only a row-preserving Select between the Limit and the scan, the
	// limit does push.
	seenOnce = false
	_, err = dataframe.NewLazySourceScan(ss).
		Select("x").
		Limit(2).
		Collect(ctx)
	require.NoError(t, err)
	require.True(t, seenOnce, "Open was not called")
	require.Empty(t, seen.Filters)
	require.Equal(t, []string{"x"}, seen.Columns)
	require.Equal(t, int64(2), seen.Limit, "expected the pushed limit")
}

// TestSourceScanIgnoresHints: a source that ignores every hint still produces a
// correct result, because the surviving Filter/Select/Limit nodes above the
// scan finish the job (pushdown is advisory).
func TestSourceScanIgnoresHints(t *testing.T) {
	ctx := context.Background()
	batches := buildBatches(t)
	defer releaseBatches(batches)

	// Open ignores hints entirely: always returns the full source.
	ss := twoBatchSource(t, func(ctx context.Context, hints dataframe.ScanHints) (array.RecordReader, error) {
		return newMemReader(twoBatchSchema(), batches), nil
	})

	got, err := dataframe.NewLazySourceScan(ss).
		Filter(expr.Col("x").Gt(expr.Lit(int64(0)))).
		Select("x").
		Limit(2).
		Collect(ctx)
	require.NoError(t, err)

	// Filter x>0 yields {1,2,3,4,5}; Limit 2 takes the first two.
	require.Equal(t, []any{int64(1), int64(2)}, colValues(t, got, "x"))
}

// TestSourceScanContextCancellation: cancelling the context mid-stream stops the
// stream and surfaces the cancellation error.
func TestSourceScanContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	batches := buildBatches(t)
	defer releaseBatches(batches)

	reader := &memReader{schema: twoBatchSchema(), recs: nil}
	ss := twoBatchSource(t, func(ctx context.Context, hints dataframe.ScanHints) (array.RecordReader, error) {
		r := newMemReader(twoBatchSchema(), batches)
		// Cancel before the first batch is yielded so the ctx wrapper trips.
		r.hook = func() { cancel() }
		reader = r
		return r, nil
	})
	_ = reader

	stream, err := dataframe.NewLazySourceScan(ss).CollectStream(ctx)
	require.NoError(t, err)
	defer stream.Release()

	for stream.Next() {
		// drain until the ctx wrapper stops us
	}
	require.ErrorIs(t, stream.Err(), context.Canceled)
}

// TestNewLazySourceScanNilSchema: a SourceScan with no schema is rejected at
// build time (the binder needs a schema up front).
func TestNewLazySourceScanNilSchema(t *testing.T) {
	ss := dataframe.SourceScan{
		Open: func(ctx context.Context, hints dataframe.ScanHints) (array.RecordReader, error) {
			return nil, nil
		},
	}
	_, err := dataframe.NewLazySourceScan(ss).Collect(context.Background())
	require.Error(t, err)
}
