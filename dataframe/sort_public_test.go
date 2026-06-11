package dataframe_test

import (
	"testing"

	"github.com/apache/arrow/go/v18/arrow/array"
	"github.com/stretchr/testify/require"

	"github.com/karthedew/cosma/dataframe"
	"github.com/karthedew/cosma/expr"
)

func stringColPub(t *testing.T, df *dataframe.DataFrame, name string) []string {
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
		require.GreaterOrEqual(t, idx, 0)
		col := rec.Column(idx).(*array.String)
		for i := 0; i < col.Len(); i++ {
			out = append(out, col.Value(i))
		}
		rec.Release()
	}
	return out
}

func TestSortAscendingPublic(t *testing.T) {
	t.Parallel()

	age, err := dataframe.NewSeries("age", []int64{30, 10, 20})
	require.NoError(t, err)
	df, err := dataframe.New([]*dataframe.Series{age})
	require.NoError(t, err)

	got, err := df.Sort(expr.By("age"))
	require.NoError(t, err)
	require.Equal(t, []int64{10, 20, 30}, int64Col(t, got, "age"))
}

func TestSortMultiColumnPublic(t *testing.T) {
	t.Parallel()

	dept, err := dataframe.NewSeries("dept", []string{"b", "a", "a", "b"})
	require.NoError(t, err)
	salary, err := dataframe.NewSeries("salary", []int64{10, 30, 20, 40})
	require.NoError(t, err)
	df, err := dataframe.New([]*dataframe.Series{dept, salary})
	require.NoError(t, err)

	got, err := df.Sort(expr.By("dept"), expr.By("salary").Desc())
	require.NoError(t, err)

	require.Equal(t, []string{"a", "a", "b", "b"}, stringColPub(t, got, "dept"))
	require.Equal(t, []int64{30, 20, 40, 10}, int64Col(t, got, "salary"))
}
