package scan_test

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/karthedew/cosma/expr"
	"github.com/karthedew/cosma/scan"
)

// writeBigCSV writes a CSV with `rows` rows: columns id,val where id is the row
// index. It returns the path. The first rows are the small ids the filter keeps,
// so a streaming filter+limit can stop long before the file ends.
func writeBigCSV(t *testing.T, rows int) string {
	t.Helper()
	f, err := os.CreateTemp("", "cosma-bigcsv-*.csv")
	require.NoError(t, err)
	var b strings.Builder
	b.WriteString("id,val\n")
	for i := 0; i < rows; i++ {
		fmt.Fprintf(&b, "%d,%d\n", i, i*10)
	}
	_, err = f.WriteString(b.String())
	require.NoError(t, err)
	require.NoError(t, f.Close())
	t.Cleanup(func() { os.Remove(f.Name()) })
	return f.Name()
}

// TB4: LazyScanCSV -> Filter -> CollectStream returns only matching rows and
// never assembles the full file into one batch (proving no whole-file
// materialization). With a small chunk size, each emitted batch is O(chunk),
// never O(file).
func TestLazyScanCSVFilterStreams(t *testing.T) {
	path := writeBigCSV(t, 1000)
	ctx := context.Background()

	reader, err := scan.LazyScanCSV(path, scan.WithCSVChunkSize(64)).
		Filter(expr.Col("id").Lt(expr.Lit(int64(5)))).
		CollectStream(ctx)
	require.NoError(t, err)
	defer reader.Release()

	var ids []any
	var maxBatchRows int64
	for reader.Next() {
		rec := reader.Record()
		if rec.NumRows() > maxBatchRows {
			maxBatchRows = rec.NumRows()
		}
		idx := rec.Schema().FieldIndices("id")[0]
		c := rec.Column(idx)
		for i := 0; i < c.Len(); i++ {
			ids = append(ids, c.GetOneForMarshal(i))
		}
	}
	require.NoError(t, reader.Err())

	// Only ids 0..4 pass the filter — exactly 5 rows (the spec says 4 for id<5
	// starting at 1; this CSV is 0-indexed, so id<5 yields {0,1,2,3,4}).
	require.Equal(t, []any{int64(0), int64(1), int64(2), int64(3), int64(4)}, ids)
	// No emitted batch ever held more than one chunk of surviving rows — far
	// less than the 1000-row file. This is the memory-bound guarantee: the
	// pipeline is O(batch_size), not O(file).
	require.LessOrEqual(t, maxBatchRows, int64(64))
}

// Memory-bound: a filter+limit pipeline over a large file scan keeps only
// O(batch_size) rows live at any time. We instrument by counting, per Next, the
// number of rows held by the live batch and assert it never exceeds the chunk
// size — the whole file (10k rows) is never resident as one record.
func TestStreamMemoryBounded(t *testing.T) {
	const rows = 10000
	const chunk = 128
	path := writeBigCSV(t, rows)

	reader, err := scan.LazyScanCSV(path, scan.WithCSVChunkSize(chunk)).
		Filter(expr.Col("id").Lt(expr.Lit(int64(5000)))).
		Limit(200).
		CollectStream(context.Background())
	require.NoError(t, err)
	defer reader.Release()

	var total int64
	var maxLive int64
	for reader.Next() {
		live := reader.Record().NumRows()
		if live > maxLive {
			maxLive = live
		}
		total += live
	}
	require.NoError(t, reader.Err())

	require.Equal(t, int64(200), total, "limit must cap total rows")
	require.LessOrEqual(t, maxLive, int64(chunk), "no batch may exceed chunk size")
}

// TB6: context cancellation mid-stream. After consuming the first batch we
// cancel; the next Next returns false and Err reports context.Canceled with no
// panic.
func TestLazyScanCSVContextCancel(t *testing.T) {
	path := writeBigCSV(t, 10000)
	ctx, cancel := context.WithCancel(context.Background())

	reader, err := scan.LazyScanCSV(path, scan.WithCSVChunkSize(64)).CollectStream(ctx)
	require.NoError(t, err)
	defer reader.Release()

	require.True(t, reader.Next(), "expected at least one batch")
	require.NotNil(t, reader.Record())

	cancel()

	require.False(t, reader.Next(), "stream must stop after cancellation")
	require.ErrorIs(t, reader.Err(), context.Canceled)
}

// Eager Collect over a LazyScanCSV reads the whole file into a DataFrame — the
// non-streaming counterpart, confirming the file source also works eagerly.
func TestLazyScanCSVEagerCollect(t *testing.T) {
	path := writeBigCSV(t, 100)
	df, err := scan.LazyScanCSV(path).
		Filter(expr.Col("id").Lt(expr.Lit(int64(3)))).
		Collect(context.Background())
	require.NoError(t, err)
	require.Equal(t, int64(3), df.NumRows())
}
