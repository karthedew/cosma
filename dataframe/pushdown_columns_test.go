package dataframe_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/karthedew/cosma/dataframe"
	"github.com/karthedew/cosma/expr"
)

// TestStreamingFilterColumnOutsideProjection is the end-to-end regression for
// the pushColumns unsoundness (issue #13): a streaming Project over a Filter
// whose predicate column is dropped by the projection. The column projection is
// applied at the scan under streaming execution, so before the fix the scan
// pruned "age" and the surviving Filter(age > 20) failed with a missing column.
// The fix pushes the union of projected and filter-referenced columns, so the
// scan emits both and the pipeline is correct.
func TestStreamingFilterColumnOutsideProjection(t *testing.T) {
	ctx := context.Background()
	batches := buildBatches(t)
	defer releaseBatches(batches)

	ss := twoBatchSource(t, func(ctx context.Context, hints dataframe.ScanHints) (array.RecordReader, error) {
		return newMemReader(twoBatchSchema(), batches), nil
	})

	// Project(x) -> Filter(age > 20) -> Scan{x, age}: age is referenced by the
	// filter but dropped by the final projection.
	reader, err := dataframe.NewLazySourceScan(ss).
		Filter(expr.Col("age").Gt(expr.Lit(int64(20)))).
		Select("x").
		CollectStream(ctx)
	require.NoError(t, err)
	defer reader.Release()

	got := drainReader(t, reader, "x")
	// age > 20 keeps x in {-2,-1,0, 2,3,5} (age 20 and 10 drop x=1 and x=4).
	require.Equal(t, []any{int64(-2), int64(-1), int64(0), int64(2), int64(3), int64(5)}, got)
}

// TestStreamingSortKeyOutsideProjection: a streaming pipeline that sorts by a
// column the projection drops. Sort is pipeline-breaking (it materializes), but
// it still reads its key column from the scan, so the key must survive pushdown.
func TestStreamingSortKeyOutsideProjection(t *testing.T) {
	ctx := context.Background()
	batches := buildBatches(t)
	defer releaseBatches(batches)

	ss := twoBatchSource(t, func(ctx context.Context, hints dataframe.ScanHints) (array.RecordReader, error) {
		return newMemReader(twoBatchSchema(), batches), nil
	})

	// Project(x) -> Sort(by age) -> Scan{x, age}.
	df, err := dataframe.NewLazySourceScan(ss).
		Sort(expr.By("age")).
		Select("x").
		Collect(ctx)
	require.NoError(t, err)

	// Rows sorted by age ascending: age 10,20,25,30,35,40,45,50 -> x 4,1,-1,-2,2,0,5,3.
	require.Equal(t, []any{int64(4), int64(1), int64(-1), int64(-2), int64(2), int64(0), int64(5), int64(3)},
		colValues(t, df, "x"))
}
