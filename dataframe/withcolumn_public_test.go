package dataframe_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/karthedew/cosma/dataframe"
	"github.com/karthedew/cosma/expr"
)

func TestWithColumnAliasAdds(t *testing.T) {
	t.Parallel()

	age, err := dataframe.NewSeries("age", []int64{10, 20, 30})
	require.NoError(t, err)
	df, err := dataframe.New([]*dataframe.Series{age})
	require.NoError(t, err)

	got, err := df.WithColumn(expr.Col("age").Mul(expr.Lit(int64(2))).As("double_age"))
	require.NoError(t, err)

	require.Equal(t, []string{"age", "double_age"}, got.Columns())
	require.Equal(t, []int64{20, 40, 60}, int64Col(t, got, "double_age"))
	require.Equal(t, []int64{10, 20, 30}, int64Col(t, got, "age"))
}

func TestWithColumnSingleRootReplaces(t *testing.T) {
	t.Parallel()

	age, err := dataframe.NewSeries("age", []int64{10, 20, 30})
	require.NoError(t, err)
	df, err := dataframe.New([]*dataframe.Series{age})
	require.NoError(t, err)

	got, err := df.WithColumn(expr.Col("age").Add(expr.Lit(int64(1))))
	require.NoError(t, err)

	require.Equal(t, []string{"age"}, got.Columns())
	require.Equal(t, []int64{11, 21, 31}, int64Col(t, got, "age"))
}

func TestWithColumnMultiColumnNoAliasErrors(t *testing.T) {
	t.Parallel()

	a, err := dataframe.NewSeries("a", []int64{1, 2, 3})
	require.NoError(t, err)
	b, err := dataframe.NewSeries("b", []int64{4, 5, 6})
	require.NoError(t, err)
	df, err := dataframe.New([]*dataframe.Series{a, b})
	require.NoError(t, err)

	_, err = df.WithColumn(expr.Col("a").Add(expr.Col("b")))
	require.Error(t, err)
}
