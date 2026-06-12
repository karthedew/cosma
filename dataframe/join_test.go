package dataframe_test

import (
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/stretchr/testify/require"

	"github.com/karthedew/cosma/dataframe"
)

// nullableInt64Col returns the named column's values as (value, isNull) pairs.
func nullableInt64Col(t *testing.T, df *dataframe.DataFrame, name string) ([]int64, []bool) {
	t.Helper()
	iter, err := dataframe.NewRecordBatchIter(df)
	require.NoError(t, err)
	var vals []int64
	var nulls []bool
	for {
		rec, ok, err := iter.Next()
		require.NoError(t, err)
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
		require.GreaterOrEqual(t, idx, 0, "column %q not found", name)
		col := rec.Column(idx).(*array.Int64)
		for i := 0; i < col.Len(); i++ {
			nulls = append(nulls, col.IsNull(i))
			if col.IsNull(i) {
				vals = append(vals, 0)
			} else {
				vals = append(vals, col.Value(i))
			}
		}
		rec.Release()
	}
	return vals, nulls
}

func strCol(t *testing.T, df *dataframe.DataFrame, name string) []string {
	t.Helper()
	iter, err := dataframe.NewRecordBatchIter(df)
	require.NoError(t, err)
	var out []string
	for {
		rec, ok, err := iter.Next()
		require.NoError(t, err)
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
		require.GreaterOrEqual(t, idx, 0, "column %q not found", name)
		col := rec.Column(idx).(*array.String)
		for i := 0; i < col.Len(); i++ {
			out = append(out, col.Value(i))
		}
		rec.Release()
	}
	return out
}

func mustDF(t *testing.T, series ...*dataframe.Series) *dataframe.DataFrame {
	t.Helper()
	df, err := dataframe.New(series)
	require.NoError(t, err)
	return df
}

func mustSeries(t *testing.T, name string, values any) *dataframe.Series {
	t.Helper()
	s, err := dataframe.NewSeries(name, values)
	require.NoError(t, err)
	return s
}

// TB3: inner join drops left rows whose key has no right match.
func TestJoinInner(t *testing.T) {
	t.Parallel()

	left := mustDF(t,
		mustSeries(t, "id", []int64{1, 2, 3}),
		mustSeries(t, "a", []string{"x", "y", "z"}),
	)
	right := mustDF(t,
		mustSeries(t, "id", []int64{1, 3}),
		mustSeries(t, "b", []int64{10, 30}),
	)

	got, err := left.Join(context.Background(), right, "id", "inner")
	require.NoError(t, err)

	require.Equal(t, []string{"id", "a", "b"}, got.Columns())
	require.Equal(t, int64(2), got.NumRows())
	require.Equal(t, []int64{1, 3}, int64Col(t, got, "id"))
	require.Equal(t, []string{"x", "z"}, strCol(t, got, "a"))
	require.Equal(t, []int64{10, 30}, int64Col(t, got, "b"))
}

// TB4: left join keeps every left row; missing right values become null.
func TestJoinLeft(t *testing.T) {
	t.Parallel()

	left := mustDF(t,
		mustSeries(t, "id", []int64{1, 2, 3}),
		mustSeries(t, "a", []string{"x", "y", "z"}),
	)
	right := mustDF(t,
		mustSeries(t, "id", []int64{1, 3}),
		mustSeries(t, "b", []int64{10, 30}),
	)

	got, err := left.Join(context.Background(), right, "id", "left")
	require.NoError(t, err)

	require.Equal(t, int64(3), got.NumRows())
	require.Equal(t, []int64{1, 2, 3}, int64Col(t, got, "id"))
	bVals, bNulls := nullableInt64Col(t, got, "b")
	require.Equal(t, []bool{false, true, false}, bNulls)
	require.Equal(t, int64(10), bVals[0])
	require.Equal(t, int64(30), bVals[2])
}

// TB5: a non-key column present in both frames is suffixed _right on the right.
func TestJoinColumnConflict(t *testing.T) {
	t.Parallel()

	left := mustDF(t,
		mustSeries(t, "id", []int64{1, 2}),
		mustSeries(t, "amount", []int64{100, 200}),
	)
	right := mustDF(t,
		mustSeries(t, "id", []int64{1, 2}),
		mustSeries(t, "amount", []int64{11, 22}),
	)

	got, err := left.Join(context.Background(), right, "id", "inner")
	require.NoError(t, err)

	require.Equal(t, []string{"id", "amount", "amount_right"}, got.Columns())
	require.Equal(t, []int64{100, 200}, int64Col(t, got, "amount"))
	require.Equal(t, []int64{11, 22}, int64Col(t, got, "amount_right"))
}

func TestJoinUnknownKey(t *testing.T) {
	t.Parallel()
	left := mustDF(t, mustSeries(t, "id", []int64{1}))
	right := mustDF(t, mustSeries(t, "id", []int64{1}))
	_, err := left.Join(context.Background(), right, "missing", "inner")
	require.Error(t, err)
}

func TestJoinUnsupportedHow(t *testing.T) {
	t.Parallel()
	left := mustDF(t, mustSeries(t, "id", []int64{1}))
	right := mustDF(t, mustSeries(t, "id", []int64{1}))
	_, err := left.Join(context.Background(), right, "id", "outer")
	require.Error(t, err)
}
