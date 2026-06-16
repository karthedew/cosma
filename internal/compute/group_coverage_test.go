package compute

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/stretchr/testify/require"
)

func TestGroupReduce_Errors(t *testing.T) {

	t.Run("nil column", func(t *testing.T) {
		_, err := GroupReduce([]int{0}, 1, nil)
		require.Error(t, err)
	})

	t.Run("group id length mismatch", func(t *testing.T) {
		mem := newGoMem()
		b := array.NewInt64Builder(mem)
		b.AppendValues([]int64{1, 2, 3}, nil)
		ch := chunkedFrom(t, arrow.PrimitiveTypes.Int64, b.NewArray())
		b.Release()
		defer ch.Release()

		_, err := GroupReduce([]int{0, 1}, 2, ch) // 2 ids for 3 rows
		require.Error(t, err)
	})
}

// TestGroupReduce_AcrossNumericDtypes drives the typed dispatch for every
// numeric element type, asserting Sum/Min/Max/Count/Mean per group.
func TestGroupReduce_AcrossNumericDtypes(t *testing.T) {

	groups := []int{0, 1, 0, 1} // values [10,20,30,40] -> g0={10,30}, g1={20,40}
	cases := []struct {
		name  string
		dt    arrow.DataType
		build func() arrow.Array
		sum0  any
		sum1  any
		min0  any
		max1  any
	}{
		{"int8", arrow.PrimitiveTypes.Int8, func() arrow.Array {
			b := array.NewInt8Builder(newGoMem())
			defer b.Release()
			b.AppendValues([]int8{10, 20, 30, 40}, nil)
			return b.NewArray()
		}, int8(40), int8(60), int8(10), int8(40)},
		{"int16", arrow.PrimitiveTypes.Int16, func() arrow.Array {
			b := array.NewInt16Builder(newGoMem())
			defer b.Release()
			b.AppendValues([]int16{10, 20, 30, 40}, nil)
			return b.NewArray()
		}, int16(40), int16(60), int16(10), int16(40)},
		{"int32", arrow.PrimitiveTypes.Int32, func() arrow.Array {
			b := array.NewInt32Builder(newGoMem())
			defer b.Release()
			b.AppendValues([]int32{10, 20, 30, 40}, nil)
			return b.NewArray()
		}, int32(40), int32(60), int32(10), int32(40)},
		{"int64", arrow.PrimitiveTypes.Int64, func() arrow.Array {
			b := array.NewInt64Builder(newGoMem())
			defer b.Release()
			b.AppendValues([]int64{10, 20, 30, 40}, nil)
			return b.NewArray()
		}, int64(40), int64(60), int64(10), int64(40)},
		{"uint8", arrow.PrimitiveTypes.Uint8, func() arrow.Array {
			b := array.NewUint8Builder(newGoMem())
			defer b.Release()
			b.AppendValues([]uint8{10, 20, 30, 40}, nil)
			return b.NewArray()
		}, uint8(40), uint8(60), uint8(10), uint8(40)},
		{"uint16", arrow.PrimitiveTypes.Uint16, func() arrow.Array {
			b := array.NewUint16Builder(newGoMem())
			defer b.Release()
			b.AppendValues([]uint16{10, 20, 30, 40}, nil)
			return b.NewArray()
		}, uint16(40), uint16(60), uint16(10), uint16(40)},
		{"uint32", arrow.PrimitiveTypes.Uint32, func() arrow.Array {
			b := array.NewUint32Builder(newGoMem())
			defer b.Release()
			b.AppendValues([]uint32{10, 20, 30, 40}, nil)
			return b.NewArray()
		}, uint32(40), uint32(60), uint32(10), uint32(40)},
		{"uint64", arrow.PrimitiveTypes.Uint64, func() arrow.Array {
			b := array.NewUint64Builder(newGoMem())
			defer b.Release()
			b.AppendValues([]uint64{10, 20, 30, 40}, nil)
			return b.NewArray()
		}, uint64(40), uint64(60), uint64(10), uint64(40)},
		{"float32", arrow.PrimitiveTypes.Float32, func() arrow.Array {
			b := array.NewFloat32Builder(newGoMem())
			defer b.Release()
			b.AppendValues([]float32{10, 20, 30, 40}, nil)
			return b.NewArray()
		}, float32(40), float32(60), float32(10), float32(40)},
		{"float64", arrow.PrimitiveTypes.Float64, func() arrow.Array {
			b := array.NewFloat64Builder(newGoMem())
			defer b.Release()
			b.AppendValues([]float64{10, 20, 30, 40}, nil)
			return b.NewArray()
		}, float64(40), float64(60), float64(10), float64(40)},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			ch := chunkedFrom(t, tc.dt, tc.build())
			defer ch.Release()

			out, err := GroupReduce(groups, 2, ch)
			require.NoError(t, err)
			require.Len(t, out, 2)

			require.EqualValues(t, 2, out[0].Count)
			require.EqualValues(t, 2, out[1].Count)
			require.Equal(t, tc.sum0, out[0].Sum)
			require.Equal(t, tc.sum1, out[1].Sum)
			require.Equal(t, tc.min0, out[0].Min)
			require.Equal(t, tc.max1, out[1].Max)
			require.InDelta(t, 20.0, out[0].Mean, 1e-9)
			require.InDelta(t, 30.0, out[1].Mean, 1e-9)
		})
	}
}

// TestGroupReduce_LargeString exercises the LARGE_STRING ordered path.
func TestGroupReduce_LargeString(t *testing.T) {
	b := array.NewLargeStringBuilder(newGoMem())
	defer b.Release()
	b.AppendValues([]string{"amy", "zoe", "bob", "ann"}, nil)
	ch := chunkedFrom(t, arrow.BinaryTypes.LargeString, b.NewArray())
	defer ch.Release()

	out, err := GroupReduce([]int{0, 1, 0, 1}, 2, ch)
	require.NoError(t, err)
	require.Equal(t, "amy", out[0].Min)
	require.Equal(t, "bob", out[0].Max)
	require.Nil(t, out[0].Sum)
}

// TestGroupReduce_CountOnlyFallback drives the default branch (groupCount) on a
// boolean column, where only Count is defined.
func TestGroupReduce_CountOnlyFallback(t *testing.T) {
	b := array.NewBooleanBuilder(newGoMem())
	defer b.Release()
	b.AppendValues([]bool{true, false, true, false}, []bool{true, true, false, true}) // row 2 null
	ch := chunkedFrom(t, arrow.FixedWidthTypes.Boolean, b.NewArray())
	defer ch.Release()

	out, err := GroupReduce([]int{0, 0, 1, 1}, 2, ch)
	require.NoError(t, err)
	require.EqualValues(t, 2, out[0].Count) // both rows non-null
	require.EqualValues(t, 1, out[1].Count) // one null in group 1
	require.Nil(t, out[0].Sum)
	require.Nil(t, out[0].Min)
}

func TestBoxedValues_AcrossDtypes(t *testing.T) {

	cases := []struct {
		name  string
		dt    arrow.DataType
		build func() arrow.Array
		want  []any
	}{
		{"int8", arrow.PrimitiveTypes.Int8, func() arrow.Array {
			b := array.NewInt8Builder(newGoMem())
			defer b.Release()
			b.AppendValues([]int8{1, 2}, []bool{true, false})
			return b.NewArray()
		}, []any{int8(1), nil}},
		{"int16", arrow.PrimitiveTypes.Int16, func() arrow.Array {
			b := array.NewInt16Builder(newGoMem())
			defer b.Release()
			b.AppendValues([]int16{1, 2}, nil)
			return b.NewArray()
		}, []any{int16(1), int16(2)}},
		{"int32", arrow.PrimitiveTypes.Int32, func() arrow.Array {
			b := array.NewInt32Builder(newGoMem())
			defer b.Release()
			b.AppendValues([]int32{1, 2}, nil)
			return b.NewArray()
		}, []any{int32(1), int32(2)}},
		{"int64", arrow.PrimitiveTypes.Int64, func() arrow.Array {
			b := array.NewInt64Builder(newGoMem())
			defer b.Release()
			b.AppendValues([]int64{1, 2}, nil)
			return b.NewArray()
		}, []any{int64(1), int64(2)}},
		{"uint8", arrow.PrimitiveTypes.Uint8, func() arrow.Array {
			b := array.NewUint8Builder(newGoMem())
			defer b.Release()
			b.AppendValues([]uint8{1, 2}, nil)
			return b.NewArray()
		}, []any{uint8(1), uint8(2)}},
		{"uint16", arrow.PrimitiveTypes.Uint16, func() arrow.Array {
			b := array.NewUint16Builder(newGoMem())
			defer b.Release()
			b.AppendValues([]uint16{1, 2}, nil)
			return b.NewArray()
		}, []any{uint16(1), uint16(2)}},
		{"uint32", arrow.PrimitiveTypes.Uint32, func() arrow.Array {
			b := array.NewUint32Builder(newGoMem())
			defer b.Release()
			b.AppendValues([]uint32{1, 2}, nil)
			return b.NewArray()
		}, []any{uint32(1), uint32(2)}},
		{"uint64", arrow.PrimitiveTypes.Uint64, func() arrow.Array {
			b := array.NewUint64Builder(newGoMem())
			defer b.Release()
			b.AppendValues([]uint64{1, 2}, nil)
			return b.NewArray()
		}, []any{uint64(1), uint64(2)}},
		{"float32", arrow.PrimitiveTypes.Float32, func() arrow.Array {
			b := array.NewFloat32Builder(newGoMem())
			defer b.Release()
			b.AppendValues([]float32{1, 2}, nil)
			return b.NewArray()
		}, []any{float32(1), float32(2)}},
		{"float64", arrow.PrimitiveTypes.Float64, func() arrow.Array {
			b := array.NewFloat64Builder(newGoMem())
			defer b.Release()
			b.AppendValues([]float64{1, 2}, nil)
			return b.NewArray()
		}, []any{float64(1), float64(2)}},
		{"string", arrow.BinaryTypes.String, func() arrow.Array {
			b := array.NewStringBuilder(newGoMem())
			defer b.Release()
			b.AppendValues([]string{"a", "b"}, nil)
			return b.NewArray()
		}, []any{"a", "b"}},
		{"bool", arrow.FixedWidthTypes.Boolean, func() arrow.Array {
			b := array.NewBooleanBuilder(newGoMem())
			defer b.Release()
			b.AppendValues([]bool{true, false}, nil)
			return b.NewArray()
		}, []any{true, false}},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			ch := chunkedFrom(t, tc.dt, tc.build())
			defer ch.Release()
			got, err := BoxedValues(ch)
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestBoxedValues_Errors(t *testing.T) {

	t.Run("nil", func(t *testing.T) {
		_, err := BoxedValues(nil)
		require.Error(t, err)
	})

	t.Run("unsupported type", func(t *testing.T) {
		dt := &arrow.DurationType{Unit: arrow.Second}
		b := array.NewDurationBuilder(newGoMem(), dt)
		defer b.Release()
		b.AppendValues([]arrow.Duration{1, 2}, nil)
		ch := chunkedFrom(t, dt, b.NewArray())
		defer ch.Release()
		_, err := BoxedValues(ch)
		require.Error(t, err)
	})
}

func TestBuildArray_RoundTrip(t *testing.T) {

	cases := []struct {
		name string
		dt   arrow.DataType
		vals []any
	}{
		{"int8", arrow.PrimitiveTypes.Int8, []any{int8(1), nil, int8(3)}},
		{"int16", arrow.PrimitiveTypes.Int16, []any{int16(1), int16(2)}},
		{"int32", arrow.PrimitiveTypes.Int32, []any{int32(1), int32(2)}},
		{"int64", arrow.PrimitiveTypes.Int64, []any{int64(1), int64(2)}},
		{"uint8", arrow.PrimitiveTypes.Uint8, []any{uint8(1), uint8(2)}},
		{"uint16", arrow.PrimitiveTypes.Uint16, []any{uint16(1), uint16(2)}},
		{"uint32", arrow.PrimitiveTypes.Uint32, []any{uint32(1), uint32(2)}},
		{"uint64", arrow.PrimitiveTypes.Uint64, []any{uint64(1), uint64(2)}},
		{"float32", arrow.PrimitiveTypes.Float32, []any{float32(1), float32(2)}},
		{"float64", arrow.PrimitiveTypes.Float64, []any{float64(1), float64(2)}},
		{"string", arrow.BinaryTypes.String, []any{"a", nil, "c"}},
		{"bool", arrow.FixedWidthTypes.Boolean, []any{true, nil, false}},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			arr, err := BuildArray(tc.dt, tc.vals, newGoMem())
			require.NoError(t, err)
			defer arr.Release()
			require.Equal(t, len(tc.vals), arr.Len())

			ch := arrow.NewChunked(tc.dt, []arrow.Array{arr})
			defer ch.Release()
			boxed, err := BoxedValues(ch)
			require.NoError(t, err)
			require.Equal(t, tc.vals, boxed)
		})
	}
}

func TestBuildArray_Errors(t *testing.T) {

	t.Run("unsupported type", func(t *testing.T) {
		_, err := BuildArray(&arrow.DurationType{Unit: arrow.Second}, []any{nil}, newGoMem())
		require.Error(t, err)
	})

	t.Run("wrong boxed go type", func(t *testing.T) {
		// int64 target but a string value is supplied.
		_, err := BuildArray(arrow.PrimitiveTypes.Int64, []any{"nope"}, newGoMem())
		require.Error(t, err)
	})

	t.Run("nil allocator defaults", func(t *testing.T) {
		arr, err := BuildArray(arrow.PrimitiveTypes.Int64, []any{int64(7)}, nil)
		require.NoError(t, err)
		defer arr.Release()
		require.Equal(t, int64(7), arr.(*array.Int64).Value(0))
	})
}
