package compute

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/stretchr/testify/require"
)

func TestTake_Errors(t *testing.T) {

	t.Run("nil column", func(t *testing.T) {
		_, err := Take(nil, []int64{0}, newGoMem())
		require.Error(t, err)
	})

	t.Run("unsupported type", func(t *testing.T) {
		dt := &arrow.DurationType{Unit: arrow.Second}
		b := array.NewDurationBuilder(newGoMem(), dt)
		b.AppendValues([]arrow.Duration{1, 2}, nil)
		ch := chunkedFrom(t, dt, b.NewArray())
		b.Release()
		defer ch.Release()
		_, err := Take(ch, []int64{0}, newGoMem())
		require.Error(t, err)
	})

	t.Run("nil allocator defaults", func(t *testing.T) {
		b := array.NewInt64Builder(newGoMem())
		b.AppendValues([]int64{7, 8}, nil)
		ch := chunkedFrom(t, arrow.PrimitiveTypes.Int64, b.NewArray())
		b.Release()
		defer ch.Release()
		out, err := Take(ch, []int64{1, 0}, nil)
		require.NoError(t, err)
		defer out.Release()
		require.Equal(t, int64(8), out.(*array.Int64).Value(0))
	})
}

// TestTake_AcrossDtypes drives the type dispatch for every supported element
// type with a simple reverse permutation.
func TestTake_AcrossDtypes(t *testing.T) {

	cases := []struct {
		name  string
		dt    arrow.DataType
		build func() arrow.Array
		want0 any
	}{
		{"int8", arrow.PrimitiveTypes.Int8, func() arrow.Array {
			b := array.NewInt8Builder(newGoMem())
			defer b.Release()
			b.AppendValues([]int8{1, 2, 3}, nil)
			return b.NewArray()
		}, int8(3)},
		{"int16", arrow.PrimitiveTypes.Int16, func() arrow.Array {
			b := array.NewInt16Builder(newGoMem())
			defer b.Release()
			b.AppendValues([]int16{1, 2, 3}, nil)
			return b.NewArray()
		}, int16(3)},
		{"int32", arrow.PrimitiveTypes.Int32, func() arrow.Array {
			b := array.NewInt32Builder(newGoMem())
			defer b.Release()
			b.AppendValues([]int32{1, 2, 3}, nil)
			return b.NewArray()
		}, int32(3)},
		{"uint8", arrow.PrimitiveTypes.Uint8, func() arrow.Array {
			b := array.NewUint8Builder(newGoMem())
			defer b.Release()
			b.AppendValues([]uint8{1, 2, 3}, nil)
			return b.NewArray()
		}, uint8(3)},
		{"uint16", arrow.PrimitiveTypes.Uint16, func() arrow.Array {
			b := array.NewUint16Builder(newGoMem())
			defer b.Release()
			b.AppendValues([]uint16{1, 2, 3}, nil)
			return b.NewArray()
		}, uint16(3)},
		{"uint32", arrow.PrimitiveTypes.Uint32, func() arrow.Array {
			b := array.NewUint32Builder(newGoMem())
			defer b.Release()
			b.AppendValues([]uint32{1, 2, 3}, nil)
			return b.NewArray()
		}, uint32(3)},
		{"uint64", arrow.PrimitiveTypes.Uint64, func() arrow.Array {
			b := array.NewUint64Builder(newGoMem())
			defer b.Release()
			b.AppendValues([]uint64{1, 2, 3}, nil)
			return b.NewArray()
		}, uint64(3)},
		{"float32", arrow.PrimitiveTypes.Float32, func() arrow.Array {
			b := array.NewFloat32Builder(newGoMem())
			defer b.Release()
			b.AppendValues([]float32{1, 2, 3}, nil)
			return b.NewArray()
		}, float32(3)},
		{"float64", arrow.PrimitiveTypes.Float64, func() arrow.Array {
			b := array.NewFloat64Builder(newGoMem())
			defer b.Release()
			b.AppendValues([]float64{1, 2, 3}, nil)
			return b.NewArray()
		}, float64(3)},
		{"string", arrow.BinaryTypes.String, func() arrow.Array {
			b := array.NewStringBuilder(newGoMem())
			defer b.Release()
			b.AppendValues([]string{"a", "b", "c"}, nil)
			return b.NewArray()
		}, "c"},
		{"largestring", arrow.BinaryTypes.LargeString, func() arrow.Array {
			b := array.NewLargeStringBuilder(newGoMem())
			defer b.Release()
			b.AppendValues([]string{"a", "b", "c"}, nil)
			return b.NewArray()
		}, "c"},
		{"bool", arrow.FixedWidthTypes.Boolean, func() arrow.Array {
			b := array.NewBooleanBuilder(newGoMem())
			defer b.Release()
			b.AppendValues([]bool{false, false, true}, nil)
			return b.NewArray()
		}, true},
	}

	valAt := func(a arrow.Array, i int) any {
		switch v := a.(type) {
		case *array.Int8:
			return v.Value(i)
		case *array.Int16:
			return v.Value(i)
		case *array.Int32:
			return v.Value(i)
		case *array.Uint8:
			return v.Value(i)
		case *array.Uint16:
			return v.Value(i)
		case *array.Uint32:
			return v.Value(i)
		case *array.Uint64:
			return v.Value(i)
		case *array.Float32:
			return v.Value(i)
		case *array.Float64:
			return v.Value(i)
		case *array.String:
			return v.Value(i)
		case *array.LargeString:
			return v.Value(i)
		case *array.Boolean:
			return v.Value(i)
		}
		return nil
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			ch := chunkedFrom(t, tc.dt, tc.build())
			defer ch.Release()
			out, err := Take(ch, []int64{2, 1, 0}, newGoMem())
			require.NoError(t, err)
			defer out.Release()
			require.Equal(t, tc.want0, valAt(out, 0))
		})
	}
}

func TestFilterRecord_Errors(t *testing.T) {

	t.Run("nil record", func(t *testing.T) {
		b := array.NewBooleanBuilder(newGoMem())
		b.AppendValues([]bool{true}, nil)
		mask := b.NewArray()
		b.Release()
		defer mask.Release()
		_, err := FilterRecord(nil, mask, newGoMem())
		require.Error(t, err)
	})

	t.Run("mask length mismatch", func(t *testing.T) {
		rec := int64Record(t, "a", []int64{1, 2, 3})
		defer rec.Release()
		b := array.NewBooleanBuilder(newGoMem())
		b.AppendValues([]bool{true, false}, nil) // length 2 != 3
		mask := b.NewArray()
		b.Release()
		defer mask.Release()
		_, err := FilterRecord(rec, mask, newGoMem())
		require.Error(t, err)
	})

	t.Run("unsupported column type", func(t *testing.T) {
		dt := &arrow.DurationType{Unit: arrow.Second}
		db := array.NewDurationBuilder(newGoMem(), dt)
		db.AppendValues([]arrow.Duration{1, 2}, nil)
		col := db.NewArray()
		db.Release()
		rec := oneColRecord(t, "d", col)
		defer rec.Release()
		mb := array.NewBooleanBuilder(newGoMem())
		mb.AppendValues([]bool{true, false}, nil)
		mask := mb.NewArray()
		mb.Release()
		defer mask.Release()
		_, err := FilterRecord(rec, mask, newGoMem())
		require.Error(t, err)
	})
}

// TestFilterRecord_AcrossDtypes drives the gather dispatch for each supported
// column type, keeping rows 0 and 2.
func TestFilterRecord_AcrossDtypes(t *testing.T) {

	cases := []struct {
		name  string
		build func() arrow.Array
		want0 any
	}{
		{"int8", func() arrow.Array {
			b := array.NewInt8Builder(newGoMem())
			defer b.Release()
			b.AppendValues([]int8{10, 20, 30}, nil)
			return b.NewArray()
		}, int8(10)},
		{"int16", func() arrow.Array {
			b := array.NewInt16Builder(newGoMem())
			defer b.Release()
			b.AppendValues([]int16{10, 20, 30}, nil)
			return b.NewArray()
		}, int16(10)},
		{"int32", func() arrow.Array {
			b := array.NewInt32Builder(newGoMem())
			defer b.Release()
			b.AppendValues([]int32{10, 20, 30}, nil)
			return b.NewArray()
		}, int32(10)},
		{"uint8", func() arrow.Array {
			b := array.NewUint8Builder(newGoMem())
			defer b.Release()
			b.AppendValues([]uint8{10, 20, 30}, nil)
			return b.NewArray()
		}, uint8(10)},
		{"uint16", func() arrow.Array {
			b := array.NewUint16Builder(newGoMem())
			defer b.Release()
			b.AppendValues([]uint16{10, 20, 30}, nil)
			return b.NewArray()
		}, uint16(10)},
		{"uint32", func() arrow.Array {
			b := array.NewUint32Builder(newGoMem())
			defer b.Release()
			b.AppendValues([]uint32{10, 20, 30}, nil)
			return b.NewArray()
		}, uint32(10)},
		{"uint64", func() arrow.Array {
			b := array.NewUint64Builder(newGoMem())
			defer b.Release()
			b.AppendValues([]uint64{10, 20, 30}, nil)
			return b.NewArray()
		}, uint64(10)},
		{"float32", func() arrow.Array {
			b := array.NewFloat32Builder(newGoMem())
			defer b.Release()
			b.AppendValues([]float32{10, 20, 30}, nil)
			return b.NewArray()
		}, float32(10)},
		{"float64", func() arrow.Array {
			b := array.NewFloat64Builder(newGoMem())
			defer b.Release()
			b.AppendValues([]float64{10, 20, 30}, nil)
			return b.NewArray()
		}, float64(10)},
		{"string", func() arrow.Array {
			b := array.NewStringBuilder(newGoMem())
			defer b.Release()
			b.AppendValues([]string{"a", "b", "c"}, nil)
			return b.NewArray()
		}, "a"},
		{"bool", func() arrow.Array {
			b := array.NewBooleanBuilder(newGoMem())
			defer b.Release()
			b.AppendValues([]bool{true, false, true}, nil)
			return b.NewArray()
		}, true},
	}

	valAt := func(a arrow.Array, i int) any {
		switch v := a.(type) {
		case *array.Int8:
			return v.Value(i)
		case *array.Int16:
			return v.Value(i)
		case *array.Int32:
			return v.Value(i)
		case *array.Uint8:
			return v.Value(i)
		case *array.Uint16:
			return v.Value(i)
		case *array.Uint32:
			return v.Value(i)
		case *array.Uint64:
			return v.Value(i)
		case *array.Float32:
			return v.Value(i)
		case *array.Float64:
			return v.Value(i)
		case *array.String:
			return v.Value(i)
		case *array.Boolean:
			return v.Value(i)
		}
		return nil
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			rec := oneColRecord(t, "c", tc.build())
			defer rec.Release()
			mb := array.NewBooleanBuilder(newGoMem())
			mb.AppendValues([]bool{true, false, true}, nil)
			mask := mb.NewArray()
			mb.Release()
			defer mask.Release()

			out, err := FilterRecord(rec, mask, newGoMem())
			require.NoError(t, err)
			defer out.Release()
			require.EqualValues(t, 2, out.NumRows())
			require.Equal(t, tc.want0, valAt(out.Column(0), 0))
		})
	}
}

// TestFilterRecord_NullMaskDropsRow confirms a null mask entry drops the row.
func TestFilterRecord_NullMaskDropsRow(t *testing.T) {
	rec := int64Record(t, "a", []int64{1, 2, 3})
	defer rec.Release()
	mb := array.NewBooleanBuilder(newGoMem())
	mb.AppendValues([]bool{true, false, false}, []bool{true, true, false}) // row 2 null mask
	mask := mb.NewArray()
	mb.Release()
	defer mask.Release()
	out, err := FilterRecord(rec, mask, newGoMem())
	require.NoError(t, err)
	defer out.Release()
	require.EqualValues(t, 1, out.NumRows())
	require.Equal(t, int64(1), out.Column(0).(*array.Int64).Value(0))
}
