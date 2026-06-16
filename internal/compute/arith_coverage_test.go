package compute

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/stretchr/testify/require"

	"github.com/karthedew/cosma/expr"
)

// TestArith_AcrossDtypes drives the arithmetic kernel for every numeric element
// type via add/sub/mul.
func TestArith_AcrossDtypes(t *testing.T) {

	cases := []struct {
		name  string
		left  func() arrow.Array
		right func() arrow.Array
		add0  any
		mul1  any
	}{
		{"int8", func() arrow.Array {
			b := array.NewInt8Builder(newGoMem())
			defer b.Release()
			b.AppendValues([]int8{2, 3}, nil)
			return b.NewArray()
		}, func() arrow.Array {
			b := array.NewInt8Builder(newGoMem())
			defer b.Release()
			b.AppendValues([]int8{4, 5}, nil)
			return b.NewArray()
		}, int8(6), int8(15)},
		{"int16", func() arrow.Array {
			b := array.NewInt16Builder(newGoMem())
			defer b.Release()
			b.AppendValues([]int16{2, 3}, nil)
			return b.NewArray()
		}, func() arrow.Array {
			b := array.NewInt16Builder(newGoMem())
			defer b.Release()
			b.AppendValues([]int16{4, 5}, nil)
			return b.NewArray()
		}, int16(6), int16(15)},
		{"int32", func() arrow.Array {
			b := array.NewInt32Builder(newGoMem())
			defer b.Release()
			b.AppendValues([]int32{2, 3}, nil)
			return b.NewArray()
		}, func() arrow.Array {
			b := array.NewInt32Builder(newGoMem())
			defer b.Release()
			b.AppendValues([]int32{4, 5}, nil)
			return b.NewArray()
		}, int32(6), int32(15)},
		{"uint8", func() arrow.Array {
			b := array.NewUint8Builder(newGoMem())
			defer b.Release()
			b.AppendValues([]uint8{2, 3}, nil)
			return b.NewArray()
		}, func() arrow.Array {
			b := array.NewUint8Builder(newGoMem())
			defer b.Release()
			b.AppendValues([]uint8{4, 5}, nil)
			return b.NewArray()
		}, uint8(6), uint8(15)},
		{"uint16", func() arrow.Array {
			b := array.NewUint16Builder(newGoMem())
			defer b.Release()
			b.AppendValues([]uint16{2, 3}, nil)
			return b.NewArray()
		}, func() arrow.Array {
			b := array.NewUint16Builder(newGoMem())
			defer b.Release()
			b.AppendValues([]uint16{4, 5}, nil)
			return b.NewArray()
		}, uint16(6), uint16(15)},
		{"uint32", func() arrow.Array {
			b := array.NewUint32Builder(newGoMem())
			defer b.Release()
			b.AppendValues([]uint32{2, 3}, nil)
			return b.NewArray()
		}, func() arrow.Array {
			b := array.NewUint32Builder(newGoMem())
			defer b.Release()
			b.AppendValues([]uint32{4, 5}, nil)
			return b.NewArray()
		}, uint32(6), uint32(15)},
		{"uint64", func() arrow.Array {
			b := array.NewUint64Builder(newGoMem())
			defer b.Release()
			b.AppendValues([]uint64{2, 3}, nil)
			return b.NewArray()
		}, func() arrow.Array {
			b := array.NewUint64Builder(newGoMem())
			defer b.Release()
			b.AppendValues([]uint64{4, 5}, nil)
			return b.NewArray()
		}, uint64(6), uint64(15)},
		{"float32", func() arrow.Array {
			b := array.NewFloat32Builder(newGoMem())
			defer b.Release()
			b.AppendValues([]float32{2, 3}, nil)
			return b.NewArray()
		}, func() arrow.Array {
			b := array.NewFloat32Builder(newGoMem())
			defer b.Release()
			b.AppendValues([]float32{4, 5}, nil)
			return b.NewArray()
		}, float32(6), float32(15)},
		{"float64", func() arrow.Array {
			b := array.NewFloat64Builder(newGoMem())
			defer b.Release()
			b.AppendValues([]float64{2, 3}, nil)
			return b.NewArray()
		}, func() arrow.Array {
			b := array.NewFloat64Builder(newGoMem())
			defer b.Release()
			b.AppendValues([]float64{4, 5}, nil)
			return b.NewArray()
		}, float64(6), float64(15)},
	}

	valAt := func(t *testing.T, a arrow.Array, i int) any {
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
		default:
			t.Fatalf("unexpected array type %T", a)
			return nil
		}
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			rec := twoColRecord(t, "l", tc.left(), "r", tc.right())
			defer rec.Release()

			add, err := Eval(expr.Col("l").Add(expr.Col("r")).Node, rec, newGoMem())
			require.NoError(t, err)
			require.Equal(t, tc.add0, valAt(t, add, 0))
			add.Release()

			mul, err := Eval(expr.Col("l").Mul(expr.Col("r")).Node, rec, newGoMem())
			require.NoError(t, err)
			require.Equal(t, tc.mul1, valAt(t, mul, 1))
			mul.Release()
		})
	}
}

func TestArith_SubAndNullPropagation(t *testing.T) {
	lb := array.NewInt64Builder(newGoMem())
	lb.AppendValues([]int64{10, 20}, []bool{true, false}) // row 1 null
	la := lb.NewArray()
	lb.Release()
	rb := array.NewInt64Builder(newGoMem())
	rb.AppendValues([]int64{3, 4}, nil)
	ra := rb.NewArray()
	rb.Release()
	rec := twoColRecord(t, "l", la, "r", ra)
	defer rec.Release()

	out, err := Eval(expr.Col("l").Sub(expr.Col("r")).Node, rec, newGoMem())
	require.NoError(t, err)
	defer out.Release()
	res := out.(*array.Int64)
	require.Equal(t, int64(7), res.Value(0))
	require.True(t, res.IsNull(1))
}

func TestArith_Errors(t *testing.T) {

	t.Run("type mismatch", func(t *testing.T) {
		ib := array.NewInt64Builder(newGoMem())
		ib.AppendValues([]int64{1, 2}, nil)
		ia := ib.NewArray()
		ib.Release()
		fb := array.NewFloat64Builder(newGoMem())
		fb.AppendValues([]float64{1, 2}, nil)
		fa := fb.NewArray()
		fb.Release()
		rec := twoColRecord(t, "l", ia, "r", fa)
		defer rec.Release()
		_, err := Eval(expr.Col("l").Add(expr.Col("r")).Node, rec, newGoMem())
		require.Error(t, err)
	})

	t.Run("unsupported arith type", func(t *testing.T) {
		// String add is not an arithmetic kernel and has no registered kernel.
		sb := array.NewStringBuilder(newGoMem())
		sb.AppendValues([]string{"a", "b"}, nil)
		sa := sb.NewArray()
		sb.Release()
		sb2 := array.NewStringBuilder(newGoMem())
		sb2.AppendValues([]string{"c", "d"}, nil)
		sa2 := sb2.NewArray()
		sb2.Release()
		rec := twoColRecord(t, "l", sa, "r", sa2)
		defer rec.Release()
		_, err := Eval(expr.Col("l").Add(expr.Col("r")).Node, rec, newGoMem())
		require.Error(t, err)
	})
}
