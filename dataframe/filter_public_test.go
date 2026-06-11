package dataframe_test

import (
	"testing"

	"github.com/apache/arrow/go/v18/arrow/array"
	"github.com/stretchr/testify/require"

	"github.com/karthedew/cosma/dataframe"
	"github.com/karthedew/cosma/expr"
)

func int64Col(t *testing.T, df *dataframe.DataFrame, name string) []int64 {
	t.Helper()
	iter, err := dataframe.NewRecordBatchIter(df)
	require.NoError(t, err)
	var out []int64
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
			out = append(out, col.Value(i))
		}
		rec.Release()
	}
	return out
}

func TestFilterPublicExpr(t *testing.T) {
	t.Parallel()

	age, err := dataframe.NewSeries("age", []int64{18, 30, 25, 42, 21})
	require.NoError(t, err)
	df, err := dataframe.New([]*dataframe.Series{age})
	require.NoError(t, err)

	got, err := df.Filter(expr.Col("age").Gt(expr.Lit(int64(25))))
	require.NoError(t, err)

	require.Equal(t, []int64{30, 42}, int64Col(t, got, "age"))
}
