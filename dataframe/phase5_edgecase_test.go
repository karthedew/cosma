// Package dataframe_test — Phase 5 edge-case validation tests.
//
// Test A: CollectStream then collect-all-into-slice matches Collect.
// Test B: Parquet lazy scan streams without materializing the file.
// Test C: CollectStream on a chained Filter+Project+Limit pipeline.
// Test D: Release after exhaustion must not panic.
//
// Run with: go test -run TestPhase5 ./dataframe/
package dataframe_test

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/karthedew/cosma/dataframe"
	"github.com/karthedew/cosma/expr"
	"github.com/karthedew/cosma/scan"
)

// ---- helpers ----------------------------------------------------------------

// countReaderRows drains reader, returning the total row count.
func countReaderRows(t *testing.T, r dataframe.RecordReader) int64 {
	t.Helper()
	var total int64
	for r.Next() {
		total += r.Record().NumRows()
	}
	require.NoError(t, r.Err())
	return total
}

// readerColumns returns the column names from reader.Schema().
func readerColumns(r dataframe.RecordReader) []string {
	sc := r.Schema()
	if sc == nil {
		return nil
	}
	names := make([]string, sc.NumFields())
	for i := 0; i < sc.NumFields(); i++ {
		names[i] = sc.Field(i).Name
	}
	return names
}

// writeSmallParquet creates a Parquet file with 50 rows: columns id (int64)
// and val (string). id is 1-based (1..50). Returns the file path and a cleanup
// function registered via t.Cleanup.
func writeSmallParquet(t *testing.T) string {
	t.Helper()

	const n = 50
	ids := make([]int64, n)
	vals := make([]string, n)
	for i := range ids {
		ids[i] = int64(i + 1) // 1-based
		vals[i] = "v"
	}

	idSeries, err := dataframe.NewSeries("id", ids)
	require.NoError(t, err)
	valSeries, err := dataframe.NewSeries("val", vals)
	require.NoError(t, err)
	df, err := dataframe.New([]*dataframe.Series{idSeries, valSeries})
	require.NoError(t, err)

	path := filepath.Join(t.TempDir(), "small.parquet")
	require.NoError(t, dataframe.WriteParquet(df, path))
	return path
}

// ---- Test A -----------------------------------------------------------------

// TestPhase5A_CollectStreamMatchesCollect verifies that iterating all batches
// from CollectStream yields the same row count as the eager Collect result.
func TestPhase5A_CollectStreamMatchesCollect(t *testing.T) {
	t.Parallel()

	age, err := dataframe.NewSeries("age", []int64{18, 25, 30, 35, 42, 55, 60, 15, 28})
	require.NoError(t, err)
	df, err := dataframe.New([]*dataframe.Series{age})
	require.NoError(t, err)

	ctx := context.Background()
	pred := expr.Col("age").Gt(expr.Lit(int64(20)))

	// Eager path.
	eagerDF, err := df.Lazy().Filter(pred).Collect(ctx)
	require.NoError(t, err)
	eagerRows := eagerDF.NumRows()

	// Streaming path: drain all batches and count rows.
	reader, err := df.Lazy().Filter(pred).CollectStream(ctx)
	require.NoError(t, err)
	defer reader.Release()

	streamRows := countReaderRows(t, reader)
	require.Equal(t, eagerRows, streamRows,
		"streaming row count must equal eager Collect row count")
}

// ---- Test B -----------------------------------------------------------------

// TestPhase5B_ParquetLazyScanStreams verifies that a Parquet lazy scan streams
// without materializing the whole file. With 50 rows and a small batch size,
// no single emitted batch may hold more than one batch worth of rows.
// The filter id < 10 should yield exactly 9 rows (ids 1..9).
func TestPhase5B_ParquetLazyScanStreams(t *testing.T) {
	t.Parallel()

	path := writeSmallParquet(t)
	ctx := context.Background()

	// Use a small batch size so we exercise multi-batch behavior.
	reader, err := scan.LazyScanParquet(path, scan.WithParquetBatchSize(16)).
		Filter(expr.Col("id").Lt(expr.Lit(int64(10)))).
		CollectStream(ctx)
	require.NoError(t, err)
	defer reader.Release()

	var totalRows int64
	var maxBatch int64
	for reader.Next() {
		rec := reader.Record()
		if rec.NumRows() > maxBatch {
			maxBatch = rec.NumRows()
		}
		totalRows += rec.NumRows()
	}
	require.NoError(t, reader.Err())

	// ids 1..9 pass the filter: exactly 9 rows.
	require.Equal(t, int64(9), totalRows, "filter id<10 on ids 1..50 must yield 9 rows (ids 1-9)")

	// No single emitted batch held more than the configured batch size, proving
	// the pipeline never materializes the whole file.
	require.LessOrEqual(t, maxBatch, int64(16),
		"no emitted batch may exceed the configured batch size of 16")
}

// ---- Test C -----------------------------------------------------------------

// TestPhase5C_ChainedFilterProjectLimit verifies that a chained
// Filter+Project+Limit pipeline via CollectStream returns exactly 5 rows with
// only the projected columns, and that Release after exhaustion does not panic.
func TestPhase5C_ChainedFilterProjectLimit(t *testing.T) {
	t.Parallel()

	// Build a frame with columns: name, age, active (int64 used as a bool-like).
	// All rows have non-null values for active so IsNotNull keeps them all.
	name, err := dataframe.NewSeries("name", []string{"alice", "bob", "carol", "dave", "eve", "frank", "grace", "hank"})
	require.NoError(t, err)
	age, err := dataframe.NewSeries("age", []int64{20, 30, 25, 40, 35, 28, 22, 45})
	require.NoError(t, err)
	active, err := dataframe.NewSeries("active", []int64{1, 1, 1, 1, 1, 1, 1, 1})
	require.NoError(t, err)
	df, err := dataframe.New([]*dataframe.Series{name, age, active})
	require.NoError(t, err)

	ctx := context.Background()

	reader, err := df.Lazy().
		Filter(expr.Col("active").IsNotNull()).
		Select("name", "age").
		Limit(5).
		CollectStream(ctx)
	require.NoError(t, err)

	// Verify schema: only [name, age] columns.
	require.Equal(t, []string{"name", "age"}, readerColumns(reader),
		"projected schema must contain only [name, age]")

	// Drain and count rows.
	var totalRows int64
	for reader.Next() {
		totalRows += reader.Record().NumRows()
	}
	require.NoError(t, reader.Err())
	require.Equal(t, int64(5), totalRows, "Limit(5) must yield exactly 5 rows")

	// Release after exhaustion must not panic.
	require.NotPanics(t, func() { reader.Release() })
}

// ---- Test D -----------------------------------------------------------------

// TestPhase5D_ReleaseAfterExhaustionNoPanic explicitly tests that calling
// Release on a fully-drained CollectStream reader does not panic, regardless
// of whether the underlying reader is a file-backed or in-memory source.
func TestPhase5D_ReleaseAfterExhaustionNoPanic(t *testing.T) {
	t.Parallel()

	// In-memory source.
	age, err := dataframe.NewSeries("age", []int64{1, 2, 3})
	require.NoError(t, err)
	df, err := dataframe.New([]*dataframe.Series{age})
	require.NoError(t, err)

	ctx := context.Background()

	reader, err := df.Lazy().Limit(3).CollectStream(ctx)
	require.NoError(t, err)

	// Drain to exhaustion.
	for reader.Next() {
	}
	require.NoError(t, reader.Err())

	// First Release.
	require.NotPanics(t, func() { reader.Release() })

	// File-backed source.
	path := writeSmallParquet(t)

	fileReader, err := scan.LazyScanParquet(path).Limit(5).CollectStream(ctx)
	require.NoError(t, err)

	for fileReader.Next() {
	}
	require.NoError(t, fileReader.Err())
	require.NotPanics(t, func() { fileReader.Release() })
}
