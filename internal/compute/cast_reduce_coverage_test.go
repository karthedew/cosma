package compute

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/stretchr/testify/require"

	"github.com/karthedew/cosma/expr"
)

func TestCast_SameTypeNoop(t *testing.T) {
	rec := int64Record(t, "x", []int64{1, 2, 3})
	defer rec.Release()
	// Casting int64 -> int64 should retain and return the same logical values.
	out, err := Eval(expr.Col("x").Cast(arrow.PrimitiveTypes.Int64).Node, rec, newGoMem())
	require.NoError(t, err)
	defer out.Release()
	require.Equal(t, arrow.INT64, out.DataType().ID())
	require.Equal(t, int64(2), out.(*array.Int64).Value(1))
}

func TestCast_NilTargetType(t *testing.T) {
	rec := int64Record(t, "x", []int64{1})
	defer rec.Release()
	// Build a CastNode with a nil target type directly.
	node := expr.CastNode{Inner: expr.Col("x").Node, Type: nil}
	_, err := Eval(node, rec, newGoMem())
	require.Error(t, err)
}

func TestCast_InnerError(t *testing.T) {
	rec := int64Record(t, "x", []int64{1})
	defer rec.Release()
	// Inner references a missing column, so evalCast surfaces the inner error.
	_, err := Eval(expr.Col("missing").Cast(arrow.PrimitiveTypes.Float64).Node, rec, newGoMem())
	require.Error(t, err)
}

func TestCast_Float64PreservesNull(t *testing.T) {
	b := array.NewInt64Builder(newGoMem())
	b.AppendValues([]int64{1, 0, 3}, []bool{true, false, true})
	rec := oneColRecord(t, "x", b.NewArray())
	b.Release()
	defer rec.Release()
	out, err := Eval(expr.Col("x").Cast(arrow.PrimitiveTypes.Float64).Node, rec, newGoMem())
	require.NoError(t, err)
	defer out.Release()
	f := out.(*array.Float64)
	require.Equal(t, float64(1), f.Value(0))
	require.True(t, f.IsNull(1))
}

func TestReduce_Errors(t *testing.T) {

	t.Run("nil", func(t *testing.T) {
		_, err := Reduce(nil)
		require.Error(t, err)
	})

	t.Run("unsupported type", func(t *testing.T) {
		dt := &arrow.DurationType{Unit: arrow.Second}
		b := array.NewDurationBuilder(newGoMem(), dt)
		b.AppendValues([]arrow.Duration{1, 2}, nil)
		ch := chunkedFrom(t, dt, b.NewArray())
		b.Release()
		defer ch.Release()
		_, err := Reduce(ch)
		require.Error(t, err)
	})
}

// TestReduce_AcrossDtypes drives Reduce for every numeric element type plus the
// string ordered paths.
func TestReduce_AcrossDtypes(t *testing.T) {

	numCases := []struct {
		name  string
		dt    arrow.DataType
		build func() arrow.Array
		sum   any
		min   any
		max   any
	}{
		{"int8", arrow.PrimitiveTypes.Int8, func() arrow.Array {
			b := array.NewInt8Builder(newGoMem())
			defer b.Release()
			b.AppendValues([]int8{1, 2, 3}, nil)
			return b.NewArray()
		}, int8(6), int8(1), int8(3)},
		{"int16", arrow.PrimitiveTypes.Int16, func() arrow.Array {
			b := array.NewInt16Builder(newGoMem())
			defer b.Release()
			b.AppendValues([]int16{1, 2, 3}, nil)
			return b.NewArray()
		}, int16(6), int16(1), int16(3)},
		{"int32", arrow.PrimitiveTypes.Int32, func() arrow.Array {
			b := array.NewInt32Builder(newGoMem())
			defer b.Release()
			b.AppendValues([]int32{1, 2, 3}, nil)
			return b.NewArray()
		}, int32(6), int32(1), int32(3)},
		{"uint8", arrow.PrimitiveTypes.Uint8, func() arrow.Array {
			b := array.NewUint8Builder(newGoMem())
			defer b.Release()
			b.AppendValues([]uint8{1, 2, 3}, nil)
			return b.NewArray()
		}, uint8(6), uint8(1), uint8(3)},
		{"uint16", arrow.PrimitiveTypes.Uint16, func() arrow.Array {
			b := array.NewUint16Builder(newGoMem())
			defer b.Release()
			b.AppendValues([]uint16{1, 2, 3}, nil)
			return b.NewArray()
		}, uint16(6), uint16(1), uint16(3)},
		{"uint32", arrow.PrimitiveTypes.Uint32, func() arrow.Array {
			b := array.NewUint32Builder(newGoMem())
			defer b.Release()
			b.AppendValues([]uint32{1, 2, 3}, nil)
			return b.NewArray()
		}, uint32(6), uint32(1), uint32(3)},
		{"uint64", arrow.PrimitiveTypes.Uint64, func() arrow.Array {
			b := array.NewUint64Builder(newGoMem())
			defer b.Release()
			b.AppendValues([]uint64{1, 2, 3}, nil)
			return b.NewArray()
		}, uint64(6), uint64(1), uint64(3)},
		{"float32", arrow.PrimitiveTypes.Float32, func() arrow.Array {
			b := array.NewFloat32Builder(newGoMem())
			defer b.Release()
			b.AppendValues([]float32{1, 2, 3}, nil)
			return b.NewArray()
		}, float32(6), float32(1), float32(3)},
		{"float64", arrow.PrimitiveTypes.Float64, func() arrow.Array {
			b := array.NewFloat64Builder(newGoMem())
			defer b.Release()
			b.AppendValues([]float64{1, 2, 3}, nil)
			return b.NewArray()
		}, float64(6), float64(1), float64(3)},
	}

	for _, tc := range numCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			ch := chunkedFrom(t, tc.dt, tc.build())
			defer ch.Release()
			ag, err := Reduce(ch)
			require.NoError(t, err)
			require.EqualValues(t, 3, ag.Count)
			require.Equal(t, tc.sum, ag.Sum)
			require.Equal(t, tc.min, ag.Min)
			require.Equal(t, tc.max, ag.Max)
			require.InDelta(t, 2.0, ag.Mean, 1e-9)
		})
	}

	t.Run("string ordered", func(t *testing.T) {
		b := array.NewStringBuilder(newGoMem())
		b.AppendValues([]string{"banana", "apple", "cherry"}, nil)
		ch := chunkedFrom(t, arrow.BinaryTypes.String, b.NewArray())
		b.Release()
		defer ch.Release()
		ag, err := Reduce(ch)
		require.NoError(t, err)
		require.EqualValues(t, 3, ag.Count)
		require.Equal(t, "apple", ag.Min)
		require.Equal(t, "cherry", ag.Max)
		require.Nil(t, ag.Sum)
	})

	t.Run("large string ordered", func(t *testing.T) {
		b := array.NewLargeStringBuilder(newGoMem())
		b.AppendValues([]string{"banana", "apple", "cherry"}, nil)
		ch := chunkedFrom(t, arrow.BinaryTypes.LargeString, b.NewArray())
		b.Release()
		defer ch.Release()
		ag, err := Reduce(ch)
		require.NoError(t, err)
		require.Equal(t, "apple", ag.Min)
		require.Equal(t, "cherry", ag.Max)
	})

	t.Run("string all null", func(t *testing.T) {
		b := array.NewStringBuilder(newGoMem())
		b.AppendNulls(2)
		ch := chunkedFrom(t, arrow.BinaryTypes.String, b.NewArray())
		b.Release()
		defer ch.Release()
		ag, err := Reduce(ch)
		require.NoError(t, err)
		require.EqualValues(t, 0, ag.Count)
		require.Nil(t, ag.Min)
		require.Nil(t, ag.Max)
	})
}
