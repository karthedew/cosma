package dataframe_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/karthedew/cosma/dataframe"
	"github.com/karthedew/cosma/expr"
)

// TB7: a cancelled context stops Filter before it returns a result.
func TestFilterContextCancelled(t *testing.T) {
	t.Parallel()

	const n = 100_000
	vals := make([]int64, n)
	for i := range vals {
		vals[i] = int64(i)
	}
	col, err := dataframe.NewSeries("a", vals)
	require.NoError(t, err)
	df, err := dataframe.New([]*dataframe.Series{col})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel before Filter runs

	got, err := df.Filter(ctx, expr.Col("a").Gt(expr.Lit(int64(0))))
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, got)
}

func TestSortContextCancelled(t *testing.T) {
	t.Parallel()

	const n = 100_000
	vals := make([]int64, n)
	for i := range vals {
		vals[i] = int64(n - i)
	}
	col, err := dataframe.NewSeries("a", vals)
	require.NoError(t, err)
	df, err := dataframe.New([]*dataframe.Series{col})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	got, err := df.Sort(ctx, expr.By("a"))
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, got)
}

func TestJoinContextCancelled(t *testing.T) {
	t.Parallel()

	const n = 100_000
	ids := make([]int64, n)
	for i := range ids {
		ids[i] = int64(i)
	}
	leftCol, err := dataframe.NewSeries("id", ids)
	require.NoError(t, err)
	left, err := dataframe.New([]*dataframe.Series{leftCol})
	require.NoError(t, err)
	rightCol, err := dataframe.NewSeries("id", ids)
	require.NoError(t, err)
	right, err := dataframe.New([]*dataframe.Series{rightCol})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	got, err := left.Join(ctx, right, "id", "inner")
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, got)
}
