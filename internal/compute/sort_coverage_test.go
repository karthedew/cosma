package compute

import (
	"reflect"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/stretchr/testify/require"
)

func TestSortIndices_Errors(t *testing.T) {

	t.Run("nil column", func(t *testing.T) {
		_, err := SortIndices(nil, false, false)
		require.Error(t, err)
	})

	t.Run("unsupported type", func(t *testing.T) {
		dt := &arrow.DurationType{Unit: arrow.Second}
		b := array.NewDurationBuilder(newGoMem(), dt)
		b.AppendValues([]arrow.Duration{1, 2}, nil)
		ch := chunkedFrom(t, dt, b.NewArray())
		b.Release()
		defer ch.Release()
		_, err := SortIndices(ch, false, false)
		require.Error(t, err)
	})
}

// TestSortIndices_Descending and NullsFirst exercise the remaining branches of
// rowCmp.
func TestSortIndices_DirectionAndNulls(t *testing.T) {
	b := array.NewInt64Builder(newGoMem())
	b.Append(3)
	b.AppendNull()
	b.Append(1)
	b.Append(2)
	ch := chunkedFrom(t, arrow.PrimitiveTypes.Int64, b.NewArray())
	b.Release()
	defer ch.Release()

	desc, err := SortIndices(ch, true, false) // descending, nulls last
	require.NoError(t, err)
	require.Equal(t, []int64{0, 3, 2, 1}, desc)

	nf, err := SortIndices(ch, false, true) // ascending, nulls first
	require.NoError(t, err)
	require.Equal(t, []int64{1, 2, 3, 0}, nf)
}

// TestSortIndices_StringDispatch drives the STRING comparator branch.
func TestSortIndices_StringDispatch(t *testing.T) {
	b := array.NewStringBuilder(newGoMem())
	b.AppendValues([]string{"c", "a", "b"}, nil)
	ch := chunkedFrom(t, arrow.BinaryTypes.String, b.NewArray())
	b.Release()
	defer ch.Release()
	idx, err := SortIndices(ch, false, false)
	require.NoError(t, err)
	require.Equal(t, []int64{1, 2, 0}, idx)
}

func TestSortIndicesMulti(t *testing.T) {

	// key0 (asc): [1,1,2,2]; key1 (desc): [5,9,1,3]
	// expected order: rows grouped by key0 then key1 desc within.
	mkKey0 := func() *arrow.Chunked {
		b := array.NewInt64Builder(newGoMem())
		b.AppendValues([]int64{1, 1, 2, 2}, nil)
		ch := chunkedFrom(t, arrow.PrimitiveTypes.Int64, b.NewArray())
		b.Release()
		return ch
	}
	mkKey1 := func() *arrow.Chunked {
		b := array.NewInt64Builder(newGoMem())
		b.AppendValues([]int64{5, 9, 1, 3}, nil)
		ch := chunkedFrom(t, arrow.PrimitiveTypes.Int64, b.NewArray())
		b.Release()
		return ch
	}

	t.Run("two keys", func(t *testing.T) {
		k0, k1 := mkKey0(), mkKey1()
		defer k0.Release()
		defer k1.Release()
		idx, err := SortIndicesMulti([]SortKey{
			{Column: k0, Descending: false},
			{Column: k1, Descending: true},
		})
		require.NoError(t, err)
		// key0=1: rows 0(5),1(9) -> desc: 1,0 ; key0=2: rows 2(1),3(3) -> desc: 3,2
		require.Equal(t, []int64{1, 0, 3, 2}, idx)
	})

	t.Run("no keys", func(t *testing.T) {
		_, err := SortIndicesMulti(nil)
		require.Error(t, err)
	})

	t.Run("nil column key", func(t *testing.T) {
		_, err := SortIndicesMulti([]SortKey{{Column: nil}})
		require.Error(t, err)
	})

	t.Run("length mismatch", func(t *testing.T) {
		k0 := mkKey0()
		defer k0.Release()
		b := array.NewInt64Builder(newGoMem())
		b.AppendValues([]int64{1, 2}, nil) // length 2 != 4
		short := chunkedFrom(t, arrow.PrimitiveTypes.Int64, b.NewArray())
		b.Release()
		defer short.Release()
		_, err := SortIndicesMulti([]SortKey{{Column: k0}, {Column: short}})
		require.Error(t, err)
	})

	t.Run("unsupported key type", func(t *testing.T) {
		k0 := mkKey0()
		defer k0.Release()
		dt := &arrow.DurationType{Unit: arrow.Second}
		b := array.NewDurationBuilder(newGoMem(), dt)
		b.AppendValues([]arrow.Duration{1, 2, 3, 4}, nil)
		bad := chunkedFrom(t, dt, b.NewArray())
		b.Release()
		defer bad.Release()
		_, err := SortIndicesMulti([]SortKey{{Column: k0}, {Column: bad}})
		require.Error(t, err)
	})
}

// TestComparatorFor_AcrossDtypes ensures every comparator branch is reachable
// via SortIndices.
func TestComparatorFor_AcrossDtypes(t *testing.T) {

	cases := []struct {
		name  string
		dt    arrow.DataType
		build func() arrow.Array
	}{
		{"int8", arrow.PrimitiveTypes.Int8, func() arrow.Array {
			b := array.NewInt8Builder(newGoMem())
			defer b.Release()
			b.AppendValues([]int8{3, 1, 2}, nil)
			return b.NewArray()
		}},
		{"int16", arrow.PrimitiveTypes.Int16, func() arrow.Array {
			b := array.NewInt16Builder(newGoMem())
			defer b.Release()
			b.AppendValues([]int16{3, 1, 2}, nil)
			return b.NewArray()
		}},
		{"int32", arrow.PrimitiveTypes.Int32, func() arrow.Array {
			b := array.NewInt32Builder(newGoMem())
			defer b.Release()
			b.AppendValues([]int32{3, 1, 2}, nil)
			return b.NewArray()
		}},
		{"uint8", arrow.PrimitiveTypes.Uint8, func() arrow.Array {
			b := array.NewUint8Builder(newGoMem())
			defer b.Release()
			b.AppendValues([]uint8{3, 1, 2}, nil)
			return b.NewArray()
		}},
		{"uint16", arrow.PrimitiveTypes.Uint16, func() arrow.Array {
			b := array.NewUint16Builder(newGoMem())
			defer b.Release()
			b.AppendValues([]uint16{3, 1, 2}, nil)
			return b.NewArray()
		}},
		{"uint32", arrow.PrimitiveTypes.Uint32, func() arrow.Array {
			b := array.NewUint32Builder(newGoMem())
			defer b.Release()
			b.AppendValues([]uint32{3, 1, 2}, nil)
			return b.NewArray()
		}},
		{"uint64", arrow.PrimitiveTypes.Uint64, func() arrow.Array {
			b := array.NewUint64Builder(newGoMem())
			defer b.Release()
			b.AppendValues([]uint64{3, 1, 2}, nil)
			return b.NewArray()
		}},
		{"float32", arrow.PrimitiveTypes.Float32, func() arrow.Array {
			b := array.NewFloat32Builder(newGoMem())
			defer b.Release()
			b.AppendValues([]float32{3, 1, 2}, nil)
			return b.NewArray()
		}},
		{"float64", arrow.PrimitiveTypes.Float64, func() arrow.Array {
			b := array.NewFloat64Builder(newGoMem())
			defer b.Release()
			b.AppendValues([]float64{3, 1, 2}, nil)
			return b.NewArray()
		}},
		{"largestring", arrow.BinaryTypes.LargeString, func() arrow.Array {
			b := array.NewLargeStringBuilder(newGoMem())
			defer b.Release()
			b.AppendValues([]string{"c", "a", "b"}, nil)
			return b.NewArray()
		}},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			ch := chunkedFrom(t, tc.dt, tc.build())
			defer ch.Release()
			idx, err := SortIndices(ch, false, false)
			require.NoError(t, err)
			require.True(t, reflect.DeepEqual(idx, []int64{1, 2, 0}), "got %v", idx)
		})
	}
}
