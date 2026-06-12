package gonum_test

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/require"

	"github.com/karthedew/cosma/dataframe"
	cosmagonum "github.com/karthedew/cosma/gonum"
)

// helpers

func intSeries(t *testing.T, name string, vals []int64, nullAt ...int) *dataframe.Series {
	t.Helper()
	alloc := memory.NewGoAllocator()
	b := array.NewInt64Builder(alloc)
	defer b.Release()
	nullSet := make(map[int]bool, len(nullAt))
	for _, n := range nullAt {
		nullSet[n] = true
	}
	for i, v := range vals {
		if nullSet[i] {
			b.AppendNull()
		} else {
			b.Append(v)
		}
	}
	chunked := arrow.NewChunked(arrow.PrimitiveTypes.Int64, []arrow.Array{b.NewArray()})
	return dataframe.NewSeriesFromChunked(name, chunked)
}

func float64Series(t *testing.T, name string, vals []float64, nullAt ...int) *dataframe.Series {
	t.Helper()
	alloc := memory.NewGoAllocator()
	b := array.NewFloat64Builder(alloc)
	defer b.Release()
	nullSet := make(map[int]bool, len(nullAt))
	for _, n := range nullAt {
		nullSet[n] = true
	}
	for i, v := range vals {
		if nullSet[i] {
			b.AppendNull()
		} else {
			b.Append(v)
		}
	}
	chunked := arrow.NewChunked(arrow.PrimitiveTypes.Float64, []arrow.Array{b.NewArray()})
	return dataframe.NewSeriesFromChunked(name, chunked)
}

func stringSeries(t *testing.T, name string, vals []string) *dataframe.Series {
	t.Helper()
	alloc := memory.NewGoAllocator()
	b := array.NewStringBuilder(alloc)
	defer b.Release()
	b.AppendValues(vals, nil)
	chunked := arrow.NewChunked(arrow.BinaryTypes.String, []arrow.Array{b.NewArray()})
	return dataframe.NewSeriesFromChunked(name, chunked)
}

func mustDF(t *testing.T, series ...*dataframe.Series) *dataframe.DataFrame {
	t.Helper()
	df, err := dataframe.New(series)
	require.NoError(t, err)
	return df
}

// ToVector tests

func TestToVector_Basic(t *testing.T) {
	s := intSeries(t, "x", []int64{1, 2, 3, 4, 5})
	vec, err := cosmagonum.ToVector(s, cosmagonum.VectorOptions{})
	require.NoError(t, err)
	require.Equal(t, 5, vec.Len())
	for i, want := range []float64{1, 2, 3, 4, 5} {
		require.InDelta(t, want, vec.AtVec(i), 1e-12, "row %d", i)
	}
}

func TestToVector_Float64(t *testing.T) {
	s := float64Series(t, "v", []float64{1.5, 2.5, 3.5})
	vec, err := cosmagonum.ToVector(s, cosmagonum.VectorOptions{})
	require.NoError(t, err)
	require.Equal(t, 3, vec.Len())
	require.InDelta(t, 2.5, vec.AtVec(1), 1e-12)
}

func TestToVector_NullError(t *testing.T) {
	s := intSeries(t, "x", []int64{1, 0, 3}, 1)
	_, err := cosmagonum.ToVector(s, cosmagonum.VectorOptions{NullPolicy: cosmagonum.NullError})
	require.Error(t, err)
	require.Contains(t, err.Error(), "null")
}

func TestToVector_NullDrop(t *testing.T) {
	s := intSeries(t, "x", []int64{1, 0, 3}, 1)
	vec, err := cosmagonum.ToVector(s, cosmagonum.VectorOptions{NullPolicy: cosmagonum.NullDrop})
	require.NoError(t, err)
	require.Equal(t, 2, vec.Len())
	require.InDelta(t, 1.0, vec.AtVec(0), 1e-12)
	require.InDelta(t, 3.0, vec.AtVec(1), 1e-12)
}

func TestToVector_NullFill(t *testing.T) {
	s := intSeries(t, "x", []int64{1, 0, 3}, 1)
	vec, err := cosmagonum.ToVector(s, cosmagonum.VectorOptions{NullPolicy: cosmagonum.NullFill, FillValue: -99})
	require.NoError(t, err)
	require.Equal(t, 3, vec.Len())
	require.InDelta(t, -99.0, vec.AtVec(1), 1e-12)
}

func TestToVector_NonNumericError(t *testing.T) {
	s := stringSeries(t, "name", []string{"a", "b"})
	_, err := cosmagonum.ToVector(s, cosmagonum.VectorOptions{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "non-numeric")
}

func TestToVector_Nil(t *testing.T) {
	_, err := cosmagonum.ToVector(nil, cosmagonum.VectorOptions{})
	require.Error(t, err)
}

// ToMatrix tests

func TestToMatrix_BasicShape(t *testing.T) {
	df := mustDF(t,
		intSeries(t, "a", []int64{1, 2, 3}),
		intSeries(t, "b", []int64{4, 5, 6}),
	)
	m, err := cosmagonum.ToMatrix(df, cosmagonum.MatrixOptions{})
	require.NoError(t, err)
	r, c := m.Dims()
	require.Equal(t, 3, r)
	require.Equal(t, 2, c)
}

func TestToMatrix_ColumnOrder(t *testing.T) {
	df := mustDF(t,
		intSeries(t, "a", []int64{1, 2}),
		intSeries(t, "b", []int64{10, 20}),
		intSeries(t, "c", []int64{100, 200}),
	)
	// Explicit reverse order.
	m, err := cosmagonum.ToMatrix(df, cosmagonum.MatrixOptions{Cols: []string{"c", "a"}})
	require.NoError(t, err)
	r, c := m.Dims()
	require.Equal(t, 2, r)
	require.Equal(t, 2, c)
	require.InDelta(t, 100.0, m.At(0, 0), 1e-12, "c col first")
	require.InDelta(t, 1.0, m.At(0, 1), 1e-12, "a col second")
}

func TestToMatrix_SkipsNonNumericByDefault(t *testing.T) {
	df := mustDF(t,
		intSeries(t, "x", []int64{1, 2}),
		stringSeries(t, "label", []string{"foo", "bar"}),
		intSeries(t, "y", []int64{3, 4}),
	)
	m, err := cosmagonum.ToMatrix(df, cosmagonum.MatrixOptions{})
	require.NoError(t, err)
	_, c := m.Dims()
	require.Equal(t, 2, c, "only numeric cols included")
}

func TestToMatrix_NonNumericColError(t *testing.T) {
	df := mustDF(t,
		intSeries(t, "x", []int64{1, 2}),
		stringSeries(t, "label", []string{"foo", "bar"}),
	)
	_, err := cosmagonum.ToMatrix(df, cosmagonum.MatrixOptions{Cols: []string{"label"}})
	require.Error(t, err)
	require.Contains(t, err.Error(), "non-numeric")
}

func TestToMatrix_NullError(t *testing.T) {
	df := mustDF(t,
		intSeries(t, "a", []int64{1, 0, 3}, 1),
		intSeries(t, "b", []int64{4, 5, 6}),
	)
	_, err := cosmagonum.ToMatrix(df, cosmagonum.MatrixOptions{NullPolicy: cosmagonum.NullError})
	require.Error(t, err)
	require.Contains(t, err.Error(), "null")
}

func TestToMatrix_NullDrop(t *testing.T) {
	df := mustDF(t,
		intSeries(t, "a", []int64{1, 0, 3}, 1),
		intSeries(t, "b", []int64{4, 5, 6}),
	)
	m, err := cosmagonum.ToMatrix(df, cosmagonum.MatrixOptions{NullPolicy: cosmagonum.NullDrop})
	require.NoError(t, err)
	r, _ := m.Dims()
	require.Equal(t, 2, r)
	require.InDelta(t, 1.0, m.At(0, 0), 1e-12)
	require.InDelta(t, 3.0, m.At(1, 0), 1e-12)
}

func TestToMatrix_NullFill(t *testing.T) {
	df := mustDF(t,
		intSeries(t, "a", []int64{1, 0, 3}, 1),
		intSeries(t, "b", []int64{4, 5, 6}),
	)
	m, err := cosmagonum.ToMatrix(df, cosmagonum.MatrixOptions{NullPolicy: cosmagonum.NullFill, FillValue: 0})
	require.NoError(t, err)
	r, _ := m.Dims()
	require.Equal(t, 3, r)
	require.InDelta(t, 0.0, m.At(1, 0), 1e-12, "null filled with 0")
	require.InDelta(t, 5.0, m.At(1, 1), 1e-12, "non-null preserved")
}

func TestToMatrix_Values(t *testing.T) {
	df := mustDF(t,
		intSeries(t, "a", []int64{10, 20, 30}),
		float64Series(t, "b", []float64{1.1, 2.2, 3.3}),
	)
	m, err := cosmagonum.ToMatrix(df, cosmagonum.MatrixOptions{})
	require.NoError(t, err)
	require.InDelta(t, 10.0, m.At(0, 0), 1e-12)
	require.InDelta(t, 2.2, m.At(1, 1), 1e-9)
	require.InDelta(t, 30.0, m.At(2, 0), 1e-12)
}

func TestToMatrix_NoNumericColumns(t *testing.T) {
	df := mustDF(t,
		stringSeries(t, "label", []string{"a", "b"}),
	)
	_, err := cosmagonum.ToMatrix(df, cosmagonum.MatrixOptions{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "no numeric")
}

// Benchmarks

func BenchmarkToMatrix_10k(b *testing.B) {
	alloc := memory.NewGoAllocator()
	n := 10_000

	makeIntSeries := func(name string) *dataframe.Series {
		bld := array.NewInt64Builder(alloc)
		defer bld.Release()
		for i := 0; i < n; i++ {
			bld.Append(int64(i))
		}
		chunked := arrow.NewChunked(arrow.PrimitiveTypes.Int64, []arrow.Array{bld.NewArray()})
		return dataframe.NewSeriesFromChunked(name, chunked)
	}

	df, _ := dataframe.New([]*dataframe.Series{makeIntSeries("a"), makeIntSeries("b"), makeIntSeries("c")})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = cosmagonum.ToMatrix(df, cosmagonum.MatrixOptions{})
	}
}

func BenchmarkToVector_10k(b *testing.B) {
	alloc := memory.NewGoAllocator()
	n := 10_000
	bld := array.NewFloat64Builder(alloc)
	defer bld.Release()
	for i := 0; i < n; i++ {
		bld.Append(float64(i) * 1.5)
	}
	chunked := arrow.NewChunked(arrow.PrimitiveTypes.Float64, []arrow.Array{bld.NewArray()})
	s := dataframe.NewSeriesFromChunked("v", chunked)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = cosmagonum.ToVector(s, cosmagonum.VectorOptions{})
	}
}
