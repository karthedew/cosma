// Package dataframe_test — Phase 4 parity tests.
//
// For every lazy operator, Collect must produce a result identical to the
// corresponding eager call: same row count, same column names, and same values.
package dataframe_test

import (
	"context"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/karthedew/cosma/dataframe"
	"github.com/karthedew/cosma/expr"
)

// dfEqual asserts that two DataFrames are identical: same columns (in order),
// same row count, and same int64/string values in every column.
func dfEqual(t *testing.T, want, got *dataframe.DataFrame, msg string) {
	t.Helper()
	require.Equal(t, want.Columns(), got.Columns(), "%s: columns mismatch", msg)
	require.Equal(t, want.NumRows(), got.NumRows(), "%s: row count mismatch", msg)
	for _, col := range want.Columns() {
		wantVals := colValues(t, want, col)
		gotVals := colValues(t, got, col)
		require.Equal(t, wantVals, gotVals, "%s: column %q values mismatch", msg, col)
	}
}

// colValues reads a column as []any — int64 or string — so dfEqual works for
// both numeric and string columns without knowing the dtype in advance.
func colValues(t *testing.T, df *dataframe.DataFrame, name string) []any {
	t.Helper()
	iter, err := dataframe.NewRecordBatchIter(df)
	require.NoError(t, err)

	var out []any
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
		require.GreaterOrEqual(t, idx, 0, "column %q not found in schema", name)

		col := rec.Column(idx)
		for i := 0; i < col.Len(); i++ {
			out = append(out, col.GetOneForMarshal(i))
		}
		rec.Release()
	}
	return out
}

// sortDF returns a shallow copy of a DataFrame sorted lexicographically by all
// columns so that order-insensitive comparisons work (e.g. GroupBy).
// We use a stable sort on int64Col values of the first column.
func int64ColFromDF(t *testing.T, df *dataframe.DataFrame, name string) []int64 {
	t.Helper()
	raw := colValues(t, df, name)
	out := make([]int64, len(raw))
	for i, v := range raw {
		switch x := v.(type) {
		case int64:
			out[i] = x
		case float64:
			out[i] = int64(x)
		default:
			t.Fatalf("unexpected type %T for column %q row %d", v, name, i)
		}
	}
	return out
}

// ---- helpers ----------------------------------------------------------------

func newTestDF(t *testing.T) *dataframe.DataFrame {
	t.Helper()
	age, err := dataframe.NewSeries("age", []int64{18, 30, 25, 42, 21})
	require.NoError(t, err)
	name, err := dataframe.NewSeries("name", []string{"alice", "bob", "carol", "dave", "eve"})
	require.NoError(t, err)
	df, err := dataframe.New([]*dataframe.Series{age, name})
	require.NoError(t, err)
	return df
}

// ---- Filter -----------------------------------------------------------------

func TestLazyParity_Filter(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	df := newTestDF(t)

	pred := expr.Col("age").Gt(expr.Lit(int64(25)))

	lazy, err := df.Lazy().Filter(pred).Collect(ctx)
	require.NoError(t, err)

	eager, err := df.Filter(ctx, pred)
	require.NoError(t, err)

	dfEqual(t, eager, lazy, "Filter")
}

// ---- Select -----------------------------------------------------------------

func TestLazyParity_Select(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	df := newTestDF(t)

	lazy, err := df.Lazy().Select("age", "name").Collect(ctx)
	require.NoError(t, err)

	eager, err := df.Select("age", "name")
	require.NoError(t, err)

	dfEqual(t, eager, lazy, "Select")
}

// ---- Limit ------------------------------------------------------------------

func TestLazyParity_Limit(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	df := newTestDF(t)

	lazy, err := df.Lazy().Limit(2).Collect(ctx)
	require.NoError(t, err)

	eager := df.Limit(2)

	dfEqual(t, eager, lazy, "Limit")
}

// ---- Sort -------------------------------------------------------------------

func TestLazyParity_Sort(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	df := newTestDF(t)

	lazy, err := df.Lazy().Sort(expr.By("age").Desc()).Collect(ctx)
	require.NoError(t, err)

	eager, err := df.Sort(ctx, expr.By("age").Desc())
	require.NoError(t, err)

	dfEqual(t, eager, lazy, "Sort")
}

// ---- WithColumn -------------------------------------------------------------

func TestLazyParity_WithColumn(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	df := newTestDF(t)

	e := expr.Col("age").Mul(expr.Lit(int64(2))).As("double")

	lazy, err := df.Lazy().WithColumn(e).Collect(ctx)
	require.NoError(t, err)

	eager, err := df.WithColumn(e)
	require.NoError(t, err)

	dfEqual(t, eager, lazy, "WithColumn")
}

// ---- GroupBy / Agg ----------------------------------------------------------

// GroupBy results are unordered so we sort by dept before comparing.
func TestLazyParity_GroupBy(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	dept, err := dataframe.NewSeries("dept", []string{"eng", "sales", "eng", "sales"})
	require.NoError(t, err)
	salary, err := dataframe.NewSeries("salary", []int64{100, 50, 200, 70})
	require.NoError(t, err)
	df, err := dataframe.New([]*dataframe.Series{dept, salary})
	require.NoError(t, err)

	agg := expr.Col("salary").Sum().As("total")

	lazy, err := df.Lazy().GroupBy("dept").Agg(agg).Collect(ctx)
	require.NoError(t, err)

	eager, err := df.GroupBy("dept").Agg(ctx, agg)
	require.NoError(t, err)

	// Both results must have the same columns.
	require.Equal(t, eager.Columns(), lazy.Columns(), "GroupBy: columns mismatch")
	require.Equal(t, eager.NumRows(), lazy.NumRows(), "GroupBy: row count mismatch")

	// Sort both by "total" so the comparison is order-independent.
	lazyTotals := int64ColFromDF(t, lazy, "total")
	eagerTotals := int64ColFromDF(t, eager, "total")
	sort.Slice(lazyTotals, func(i, j int) bool { return lazyTotals[i] < lazyTotals[j] })
	sort.Slice(eagerTotals, func(i, j int) bool { return eagerTotals[i] < eagerTotals[j] })
	require.Equal(t, eagerTotals, lazyTotals, "GroupBy: sorted totals mismatch")
}

// ---- Join -------------------------------------------------------------------

func TestLazyParity_Join(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	leftID, err := dataframe.NewSeries("id", []int64{1, 2, 3})
	require.NoError(t, err)
	leftA, err := dataframe.NewSeries("a", []string{"x", "y", "z"})
	require.NoError(t, err)
	left, err := dataframe.New([]*dataframe.Series{leftID, leftA})
	require.NoError(t, err)

	rightID, err := dataframe.NewSeries("id", []int64{1, 3})
	require.NoError(t, err)
	rightB, err := dataframe.NewSeries("b", []int64{10, 30})
	require.NoError(t, err)
	right, err := dataframe.New([]*dataframe.Series{rightID, rightB})
	require.NoError(t, err)

	lazy, err := left.Lazy().Join(right.Lazy(), "id", "inner").Collect(ctx)
	require.NoError(t, err)

	eager, err := left.Join(ctx, right, "id", "inner")
	require.NoError(t, err)

	dfEqual(t, eager, lazy, "Join")
}
