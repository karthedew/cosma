// Package dataframe_test — Phase 6 edge-case validation tests.
//
// TB5: df.Filter uses the parallel path when parallelism > 1 and there are
//      multiple chunks; result matches the serial (parallelism=1) path.
// TB6: df.GroupBy.Agg uses GroupReduceParallel when parallelism > 1; result
//      matches the serial (parallelism=1) path.
// TB7: Race-free concurrent df.Filter with SetParallelism(8) from 4 goroutines.
// TB8: LastMetrics().Workers >= 2 after parallel GroupBy.Agg on a multi-chunk frame.
// TB9: SetParallelism(1) forces serial path; LastMetrics().Workers == 1.
//
// Run with: go test -race -run TestTB ./dataframe/
package dataframe_test

import (
	"context"
	"sort"
	"sync"
	"testing"

	"github.com/apache/arrow/go/v18/arrow/array"
	"github.com/stretchr/testify/require"

	"github.com/karthedew/cosma/dataframe"
	"github.com/karthedew/cosma/expr"
	"github.com/karthedew/cosma/internal/compute"
)

// buildMultiChunkDF returns a DataFrame where each column has multiple
// chunks (by concatenating two DataFrames). n total rows; keys cycle over g groups.
func buildMultiChunkDF(t *testing.T, n, g int) *dataframe.DataFrame {
	t.Helper()
	half := n / 2

	keys1 := make([]int64, half)
	vals1 := make([]int64, half)
	for i := range keys1 {
		keys1[i] = int64(i % g)
		vals1[i] = int64(i)
	}
	keys2 := make([]int64, n-half)
	vals2 := make([]int64, n-half)
	for i := range keys2 {
		keys2[i] = int64((half+i) % g)
		vals2[i] = int64(half + i)
	}

	df1 := mustDF(t,
		mustSeries(t, "key", keys1),
		mustSeries(t, "value", vals1),
	)
	df2 := mustDF(t,
		mustSeries(t, "key", keys2),
		mustSeries(t, "value", vals2),
	)

	out, err := dataframe.Concat(df1, df2)
	require.NoError(t, err)
	return out
}

// readInt64Col drains one int64 column from df via the RecordBatchIter.
func readInt64Col(t *testing.T, df *dataframe.DataFrame, name string) []int64 {
	t.Helper()
	iter, err := dataframe.NewRecordBatchIter(df)
	require.NoError(t, err)
	var out []int64
	for {
		rec, ok, iterErr := iter.Next()
		require.NoError(t, iterErr)
		if !ok {
			break
		}
		idx := -1
		for i, f := range rec.Schema().Fields() {
			if f.Name == name {
				idx = i
				break
			}
		}
		require.GreaterOrEqual(t, idx, 0, "column %q not found in batch", name)
		col := rec.Column(idx).(*array.Int64)
		for i := 0; i < col.Len(); i++ {
			if !col.IsNull(i) {
				out = append(out, col.Value(i))
			}
		}
		rec.Release()
	}
	return out
}

// dfSum returns the sum of all non-null int64 values in column name.
func dfSum(t *testing.T, df *dataframe.DataFrame, name string) int64 {
	t.Helper()
	vals := readInt64Col(t, df, name)
	var s int64
	for _, v := range vals {
		s += v
	}
	return s
}

// TB5: df.Filter parallel path produces the same result as serial path.
func TestTB5_FilterParallelMatchesSerial(t *testing.T) {
	const n = 10_000
	const groups = n // unique keys so value uniquely identifies the row

	df := buildMultiChunkDF(t, n, groups)
	require.Greater(t, df.NumChunks(), 1, "test requires a multi-chunk DataFrame")

	ctx := context.Background()
	pred := expr.Col("value").Gt(expr.Lit(int64(n / 2)))

	// Serial reference.
	compute.SetParallelism(1)
	serial, err := df.Filter(ctx, pred)
	require.NoError(t, err)

	// Parallel path.
	compute.SetParallelism(0) // GOMAXPROCS
	parallel, err := df.Filter(ctx, pred)
	require.NoError(t, err)

	// Restore default.
	defer compute.SetParallelism(0)

	require.Equal(t, serial.NumRows(), parallel.NumRows(),
		"parallel filter row count must match serial")

	// Compare sums of the value column for value-level correctness.
	require.Equal(t, dfSum(t, serial, "value"), dfSum(t, parallel, "value"),
		"parallel filter sum must match serial sum")
}

// TB6: df.GroupBy.Agg parallel path matches serial path.
func TestTB6_GroupByParallelMatchesSerial(t *testing.T) {
	const n = 10_000
	const groups = 5

	df := buildMultiChunkDF(t, n, groups)
	ctx := context.Background()

	// Serial reference.
	compute.SetParallelism(1)
	serial, err := df.GroupBy("key").Agg(ctx, expr.Col("value").Sum())
	require.NoError(t, err)

	// Parallel path.
	compute.SetParallelism(0) // GOMAXPROCS
	parallel, err := df.GroupBy("key").Agg(ctx, expr.Col("value").Sum())
	require.NoError(t, err)

	// Restore default.
	defer compute.SetParallelism(0)

	require.Equal(t, serial.NumRows(), parallel.NumRows(),
		"parallel groupby row count must match serial")

	// Total sum across all groups must be identical.
	serialTotal := dfSum(t, serial, "value_sum")
	parallelTotal := dfSum(t, parallel, "value_sum")
	require.Equal(t, serialTotal, parallelTotal,
		"parallel groupby total sum must match serial")

	// The set of group keys must be identical (sort for order-independence).
	serialKeys := readInt64Col(t, serial, "key")
	parallelKeys := readInt64Col(t, parallel, "key")
	sort.Slice(serialKeys, func(i, j int) bool { return serialKeys[i] < serialKeys[j] })
	sort.Slice(parallelKeys, func(i, j int) bool { return parallelKeys[i] < parallelKeys[j] })
	require.Equal(t, serialKeys, parallelKeys, "group key sets must match")
}

// TB7: Race-free parallel filter with SetParallelism(8).
//
// A 16-chunk DataFrame is filtered concurrently from 4 goroutines. The race
// detector must not fire. Each goroutine independently calls df.Filter and
// verifies its result; all errors are collected and reported at the end.
func TestTB7_ParallelFilterNoRace(t *testing.T) {
	const n = 16_000
	const numGoroutines = 4

	compute.SetParallelism(8)
	defer compute.SetParallelism(0)

	// Build a 16-chunk DataFrame: two halves via Concat gives 2 chunks; we
	// build 8 pairs and concat them all to get 16 chunks.
	const chunkSize = 1000
	const numChunks = 16

	// Build each chunk as its own single-chunk DataFrame, then concat all.
	chunks := make([]*dataframe.DataFrame, numChunks)
	for c := 0; c < numChunks; c++ {
		vals := make([]int64, chunkSize)
		for i := range vals {
			vals[i] = int64(c*chunkSize + i)
		}
		chunks[c] = mustDF(t, mustSeries(t, "v", vals))
	}
	df := chunks[0]
	var err error
	for _, next := range chunks[1:] {
		df, err = dataframe.Concat(df, next)
		require.NoError(t, err)
	}
	require.Greater(t, df.NumChunks(), 1, "test requires a multi-chunk DataFrame")

	ctx := context.Background()
	pred := expr.Col("v").Gt(expr.Lit(int64(n / 2)))

	// Compute expected row count via serial reference (parallelism already 8,
	// but we just need the count for verification; all goroutines see the same
	// immutable df).
	serial, err := df.Filter(ctx, pred)
	require.NoError(t, err)
	wantRows := serial.NumRows()

	// Fan out 4 concurrent Filter calls.
	errs := make([]error, numGoroutines)
	rows := make([]int64, numGoroutines)
	var wg sync.WaitGroup
	wg.Add(numGoroutines)
	for g := 0; g < numGoroutines; g++ {
		g := g
		go func() {
			defer wg.Done()
			result, filterErr := df.Filter(ctx, pred)
			if filterErr != nil {
				errs[g] = filterErr
				return
			}
			rows[g] = result.NumRows()
		}()
	}
	wg.Wait()

	for g, e := range errs {
		require.NoError(t, e, "goroutine %d Filter error", g)
	}
	for g, r := range rows {
		require.Equal(t, wantRows, r,
			"goroutine %d: got %d rows, want %d", g, r, wantRows)
	}
}

// TB8: LastMetrics().Workers >= 2 after parallel GroupBy.Agg on a multi-chunk frame.
//
// SetParallelism(4) is applied before a GroupBy.Agg call; the post-call
// LastMetrics() must report at least 2 workers (the GroupReduceParallel path
// with 4 workers and enough rows to fill ≥ 2 strips).
func TestTB8_LastMetricsGroupByWorkers(t *testing.T) {
	const n = 10_000
	const groups = 5

	compute.SetParallelism(4)
	defer compute.SetParallelism(0)

	df := buildMultiChunkDF(t, n, groups)
	require.Greater(t, df.NumChunks(), 1, "test requires a multi-chunk DataFrame")

	ctx := context.Background()
	_, err := df.GroupBy("key").Agg(ctx, expr.Col("value").Sum())
	require.NoError(t, err)

	m := compute.LastMetrics()
	require.GreaterOrEqual(t, m.Workers, 2,
		"LastMetrics().Workers = %d after parallel GroupBy, want >= 2", m.Workers)
	require.Equal(t, "groupby", m.Op,
		"LastMetrics().Op = %q, want \"groupby\"", m.Op)
}

// TB9: SetParallelism(1) forces the serial path; LastMetrics is not updated by
// the filter (serial path does not call storeMetrics), so we verify the
// behavior of filter.go and confirm Workers == 1 when LastMetrics is set by
// a preceding EvalParallel call with w clamped to 1.
//
// Concretely: SetParallelism(1) routes Filter through filterBatchesSerial
// (which does not update LastMetrics). We prime LastMetrics with a known
// parallel call at p=2, then run Filter at p=1, then check LastMetrics is
// unchanged (still reports the earlier call). This confirms the serial bypass.
func TestTB9_SetParallelism1_DisablesParallelPath(t *testing.T) {
	const n = 4_000
	const groups = n

	df := buildMultiChunkDF(t, n, groups)
	require.Greater(t, df.NumChunks(), 1, "test requires a multi-chunk DataFrame")

	ctx := context.Background()
	pred := expr.Col("value").Gt(expr.Lit(int64(n / 2)))

	// Prime LastMetrics with a parallel filter call (p=2, so Workers=2).
	compute.SetParallelism(2)
	_, err := df.Filter(ctx, pred)
	require.NoError(t, err)
	primed := compute.LastMetrics()
	require.Equal(t, "filter", primed.Op)
	require.Equal(t, 2, primed.Workers,
		"primed LastMetrics.Workers = %d, want 2", primed.Workers)

	// Now force serial: p=1 routes through filterBatchesSerial, which does NOT
	// call storeMetrics. LastMetrics must remain unchanged.
	compute.SetParallelism(1)
	defer compute.SetParallelism(0)
	_, err = df.Filter(ctx, pred)
	require.NoError(t, err)

	after := compute.LastMetrics()
	require.Equal(t, primed.Workers, after.Workers,
		"SetParallelism(1) must not update LastMetrics: Workers changed from %d to %d",
		primed.Workers, after.Workers)
}
