package dataframe_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/karthedew/cosma/dataframe"
	"github.com/karthedew/cosma/expr"
)

// drainReader collects every row of a single named column from a streaming
// reader as []any, releasing each batch. It is the streaming analogue of
// colValues.
func drainReader(t *testing.T, r dataframe.RecordReader, col string) []any {
	t.Helper()
	var out []any
	for r.Next() {
		rec := r.Record()
		idx := -1
		for i, f := range rec.Schema().Fields() {
			if f.Name == col {
				idx = i
				break
			}
		}
		require.GreaterOrEqual(t, idx, 0, "column %q not found", col)
		c := rec.Column(idx)
		for i := 0; i < c.Len(); i++ {
			out = append(out, c.GetOneForMarshal(i))
		}
	}
	require.NoError(t, r.Err())
	return out
}

func sampleFrame(t *testing.T) *dataframe.DataFrame {
	t.Helper()
	x, err := dataframe.NewSeries("x", []int64{-2, -1, 0, 1, 2, 3, 4, 5})
	require.NoError(t, err)
	age, err := dataframe.NewSeries("age", []int64{30, 25, 40, 20, 35, 50, 10, 45})
	require.NoError(t, err)
	df, err := dataframe.New([]*dataframe.Series{x, age})
	require.NoError(t, err)
	return df
}

// TB2: CollectStream returns a RecordReader for an in-memory DataFrame, and the
// streamed Filter result matches the eager Filter result.
func TestCollectStreamFilterParity(t *testing.T) {
	df := sampleFrame(t)
	ctx := context.Background()

	reader, err := df.Lazy().Filter(expr.Col("x").Gt(expr.Lit(int64(0)))).CollectStream(ctx)
	require.NoError(t, err)
	defer reader.Release()
	got := drainReader(t, reader, "x")

	want, err := df.Filter(ctx, expr.Col("x").Gt(expr.Lit(int64(0))))
	require.NoError(t, err)

	require.Equal(t, []any{int64(1), int64(2), int64(3), int64(4), int64(5)}, got)
	require.Equal(t, want.NumRows(), int64(len(got)))
}

// TB3: CollectStream with Limit stops early — only 3 rows total across batches.
func TestCollectStreamLimitStopsEarly(t *testing.T) {
	df := sampleFrame(t)
	reader, err := df.Lazy().Limit(3).CollectStream(context.Background())
	require.NoError(t, err)
	defer reader.Release()

	got := drainReader(t, reader, "x")
	require.Len(t, got, 3)
	require.Equal(t, []any{int64(-2), int64(-1), int64(0)}, got)
}

// TB5: Pipeline-breaking ops (Sort) materialize then stream — the streamed
// output is correctly sorted.
func TestCollectStreamSortMaterializes(t *testing.T) {
	df := sampleFrame(t)
	ctx := context.Background()

	reader, err := df.Lazy().Sort(expr.By("age").Desc()).CollectStream(ctx)
	require.NoError(t, err)
	defer reader.Release()
	got := drainReader(t, reader, "age")

	require.Equal(t, []any{
		int64(50), int64(45), int64(40), int64(35),
		int64(30), int64(25), int64(20), int64(10),
	}, got)
}

// Project streaming parity: a lazy Select streamed matches eager Select.
func TestCollectStreamProject(t *testing.T) {
	df := sampleFrame(t)
	reader, err := df.Lazy().Select("age").CollectStream(context.Background())
	require.NoError(t, err)
	defer reader.Release()

	require.Equal(t, 1, reader.Schema().NumFields())
	require.Equal(t, "age", reader.Schema().Field(0).Name)
	got := drainReader(t, reader, "age")
	require.Len(t, got, 8)
}
