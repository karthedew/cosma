package dataframe_test

// Phase 1 edge-case validation tests.
// These probe gaps not covered by the original test suite.
// Run with: go test -run TestEdge ./dataframe/

import (
	"testing"

	"github.com/apache/arrow/go/v18/arrow/array"
	"github.com/apache/arrow/go/v18/arrow/memory"
	"github.com/stretchr/testify/require"

	"github.com/karthedew/cosma/dataframe"
	"github.com/karthedew/cosma/expr"
)

// TestEdge_WithColumnReplacesExisting verifies that WithColumn replaces a
// column in place rather than appending a new one.
// df has columns [a, b]. WithColumn(Col("a").Add(Lit(1))) must produce [a, b],
// not [a, b, a].
func TestEdge_WithColumnReplacesExisting(t *testing.T) {
	t.Parallel()

	a, err := dataframe.NewSeries("a", []int64{1, 2, 3})
	require.NoError(t, err)
	b, err := dataframe.NewSeries("b", []int64{10, 20, 30})
	require.NoError(t, err)
	df, err := dataframe.New([]*dataframe.Series{a, b})
	require.NoError(t, err)

	got, err := df.WithColumn(expr.Col("a").Add(expr.Lit(int64(1))))
	require.NoError(t, err)

	// Column count must remain 2, not grow to 3.
	require.Equal(t, []string{"a", "b"}, got.Columns(),
		"WithColumn on existing column should replace in place, not append")

	// Values of a must be updated.
	require.Equal(t, []int64{2, 3, 4}, int64Col(t, got, "a"))
	// Values of b must be unchanged.
	require.Equal(t, []int64{10, 20, 30}, int64Col(t, got, "b"))
}

// TestEdge_SortNullsFirst verifies that Sort with NullsFirst places nil rows
// before non-nil rows. Uses a nullable int64 column with one null at row 1.
func TestEdge_SortNullsFirst(t *testing.T) {
	t.Parallel()

	// Build a nullable int64 column: [10, null, 5]
	mem := memory.DefaultAllocator
	bldr := array.NewInt64Builder(mem)
	defer bldr.Release()
	bldr.Append(10)
	bldr.AppendNull()
	bldr.Append(5)
	arr := bldr.NewArray()
	defer arr.Release()

	xs, err := dataframe.NewSeriesFromArray("x", arr)
	require.NoError(t, err)
	df, err := dataframe.New([]*dataframe.Series{xs})
	require.NoError(t, err)

	got, err := df.Sort(expr.By("x").WithNullsFirst())
	require.NoError(t, err)
	require.EqualValues(t, 3, got.NumRows())

	// Read the sorted x column, including nulls.
	iter, err := dataframe.NewRecordBatchIter(got)
	require.NoError(t, err)
	rec, ok, err := iter.Next()
	require.NoError(t, err)
	require.True(t, ok)
	defer rec.Release()

	col := rec.Column(0).(*array.Int64)
	require.Equal(t, 3, col.Len())

	// Row 0 should be null (NullsFirst).
	require.True(t, col.IsNull(0), "row 0 should be null when NullsFirst is set")
	// Rows 1 and 2 should be the non-null values in ascending order: 5, 10.
	require.False(t, col.IsNull(1))
	require.False(t, col.IsNull(2))
	require.Equal(t, int64(5), col.Value(1))
	require.Equal(t, int64(10), col.Value(2))
}

// TestEdge_GroupByCountNonNumeric verifies that Count works on a non-numeric
// (string) column. Count is type-agnostic by definition — it counts non-null
// rows, not values.
func TestEdge_GroupByCountNonNumeric(t *testing.T) {
	t.Parallel()

	dept, err := dataframe.NewSeries("dept", []string{"eng", "sales", "eng", "sales", "eng"})
	require.NoError(t, err)
	name, err := dataframe.NewSeries("name", []string{"alice", "bob", "carol", "dan", "eve"})
	require.NoError(t, err)
	df, err := dataframe.New([]*dataframe.Series{dept, name})
	require.NoError(t, err)

	got, err := df.GroupBy("dept").Agg(
		expr.Col("name").Count().As("n"),
	)
	require.NoError(t, err, "Count on a string column should succeed")

	require.Equal(t, []string{"dept", "n"}, got.Columns())
	require.EqualValues(t, 2, got.NumRows())

	// eng appears 3 times, sales 2 times.
	counts := int64Col(t, got, "n")
	// Groups appear in first-appearance order: eng(0), sales(1).
	require.Equal(t, int64(3), counts[0], "eng group should have count 3")
	require.Equal(t, int64(2), counts[1], "sales group should have count 2")
}
