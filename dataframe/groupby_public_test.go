package dataframe_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/karthedew/cosma/dataframe"
	"github.com/karthedew/cosma/expr"
)

func TestGroupByAggNode(t *testing.T) {
	t.Parallel()

	dept, err := dataframe.NewSeries("dept", []string{"eng", "sales", "eng", "sales"})
	require.NoError(t, err)
	salary, err := dataframe.NewSeries("salary", []int64{100, 50, 200, 70})
	require.NoError(t, err)
	id, err := dataframe.NewSeries("id", []int64{1, 2, 3, 4})
	require.NoError(t, err)
	df, err := dataframe.New([]*dataframe.Series{dept, salary, id})
	require.NoError(t, err)

	got, err := df.GroupBy("dept").Agg(context.Background(),
		expr.Col("salary").Sum().As("total"),
		expr.Col("id").Count().As("n"),
	)
	require.NoError(t, err)

	require.Equal(t, []string{"dept", "total", "n"}, got.Columns())
	require.EqualValues(t, 2, got.NumRows())
	require.Equal(t, []int64{300, 120}, int64Col(t, got, "total"))
	require.Equal(t, []int64{2, 2}, int64Col(t, got, "n"))
}

func TestGroupByCountStringColumn(t *testing.T) {
	t.Parallel()

	dept, err := dataframe.NewSeries("dept", []string{"eng", "sales", "eng", "sales"})
	require.NoError(t, err)
	name, err := dataframe.NewSeries("name", []string{"a", "b", "c", "d"})
	require.NoError(t, err)
	df, err := dataframe.New([]*dataframe.Series{dept, name})
	require.NoError(t, err)

	got, err := df.GroupBy("dept").Agg(context.Background(), expr.Col("name").Count().As("n"))
	require.NoError(t, err)

	require.Equal(t, []string{"dept", "n"}, got.Columns())
	require.EqualValues(t, 2, got.NumRows())
	require.Equal(t, []int64{2, 2}, int64Col(t, got, "n"))
}
