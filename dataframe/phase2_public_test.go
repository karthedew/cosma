package dataframe_test

import (
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/require"

	"github.com/karthedew/cosma/dataframe"
	"github.com/karthedew/cosma/expr"
)

// TB1: range filter with And.
func TestPhase2_FilterAnd(t *testing.T) {
	t.Parallel()
	age, err := dataframe.NewSeries("age", []int64{18, 30, 25, 42, 21})
	require.NoError(t, err)
	df, err := dataframe.New([]*dataframe.Series{age})
	require.NoError(t, err)

	got, err := df.Filter(context.Background(), expr.Col("age").Gt(expr.Lit(int64(20))).And(expr.Col("age").Lt(expr.Lit(int64(40)))))
	require.NoError(t, err)
	require.Equal(t, []int64{30, 25, 21}, int64Col(t, got, "age"))
}

// TB2: filter on a negated boolean column.
func TestPhase2_FilterNot(t *testing.T) {
	t.Parallel()
	active, err := dataframe.NewSeries("active", []bool{true, false, true, false})
	require.NoError(t, err)
	id, err := dataframe.NewSeries("id", []int64{1, 2, 3, 4})
	require.NoError(t, err)
	df, err := dataframe.New([]*dataframe.Series{active, id})
	require.NoError(t, err)

	got, err := df.Filter(context.Background(), expr.Col("active").Not())
	require.NoError(t, err)
	require.Equal(t, []int64{2, 4}, int64Col(t, got, "id"))
}

// TB2: WithColumn with arithmetic negation.
func TestPhase2_WithColumnNeg(t *testing.T) {
	t.Parallel()
	n, err := dataframe.NewSeries("n", []int64{5, -3, 0})
	require.NoError(t, err)
	df, err := dataframe.New([]*dataframe.Series{n})
	require.NoError(t, err)

	got, err := df.WithColumn(expr.Col("n").Neg().As("neg"))
	require.NoError(t, err)
	require.Equal(t, []int64{-5, 3, 0}, int64Col(t, got, "neg"))
}

// TB3: filter on IsNull / IsNotNull over a nullable column.
func TestPhase2_FilterIsNull(t *testing.T) {
	t.Parallel()
	mem := memory.DefaultAllocator
	bld := array.NewInt64Builder(mem)
	defer bld.Release()
	bld.AppendValues([]int64{1, 0, 3}, []bool{true, false, true})
	arr := bld.NewArray()
	defer arr.Release()
	xs, err := dataframe.NewSeriesFromArray("x", arr)
	require.NoError(t, err)
	id, err := dataframe.NewSeries("id", []int64{10, 20, 30})
	require.NoError(t, err)
	df, err := dataframe.New([]*dataframe.Series{xs, id})
	require.NoError(t, err)

	gotNull, err := df.Filter(context.Background(), expr.Col("x").IsNull())
	require.NoError(t, err)
	require.Equal(t, []int64{20}, int64Col(t, gotNull, "id"))

	gotNotNull, err := df.Filter(context.Background(), expr.Col("x").IsNotNull())
	require.NoError(t, err)
	require.Equal(t, []int64{10, 30}, int64Col(t, gotNotNull, "id"))
}

// TB5: WithColumn cast int64 -> float64.
func TestPhase2_WithColumnCast(t *testing.T) {
	t.Parallel()
	age, err := dataframe.NewSeries("age", []int64{1, 2, 3})
	require.NoError(t, err)
	df, err := dataframe.New([]*dataframe.Series{age})
	require.NoError(t, err)

	got, err := df.WithColumn(expr.Col("age").Cast(arrow.PrimitiveTypes.Float64).As("f"))
	require.NoError(t, err)
	field, ok := got.Schema().Field("f")
	require.True(t, ok)
	require.Equal(t, arrow.FLOAT64, field.ArrowType.ID())
}

// TB6: string equality filter and GroupBy Max over a string column.
func TestPhase2_StringFilterAndGroupMax(t *testing.T) {
	t.Parallel()
	name, err := dataframe.NewSeries("name", []string{"Alice", "Bob", "Alice"})
	require.NoError(t, err)
	df, err := dataframe.New([]*dataframe.Series{name})
	require.NoError(t, err)
	got, err := df.Filter(context.Background(), expr.Col("name").Eq(expr.Lit("Alice")))
	require.NoError(t, err)
	require.EqualValues(t, 2, got.NumRows())

	dept, err := dataframe.NewSeries("dept", []string{"eng", "sales", "eng", "sales"})
	require.NoError(t, err)
	who, err := dataframe.NewSeries("name", []string{"amy", "zoe", "bob", "ann"})
	require.NoError(t, err)
	gdf, err := dataframe.New([]*dataframe.Series{dept, who})
	require.NoError(t, err)
	agg, err := gdf.GroupBy("dept").Agg(context.Background(), expr.Col("name").Max().As("m"))
	require.NoError(t, err)
	require.Equal(t, []string{"dept", "m"}, agg.Columns())
	// eng: max(amy,bob)=bob ; sales: max(zoe,ann)=zoe
	require.Equal(t, []string{"bob", "zoe"}, stringColPub(t, agg, "m"))
}
