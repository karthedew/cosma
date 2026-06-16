package compute

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/stretchr/testify/require"

	"github.com/karthedew/cosma/expr"
)

// TestNeg_AcrossSignedDtypes drives negKernel for every signed numeric type and
// asserts null propagation.
func TestNeg_AcrossSignedDtypes(t *testing.T) {

	cases := []struct {
		name  string
		build func() arrow.Array
		neg0  any
	}{
		{"int8", func() arrow.Array {
			b := array.NewInt8Builder(newGoMem())
			defer b.Release()
			b.AppendValues([]int8{5, 0}, []bool{true, false})
			return b.NewArray()
		}, int8(-5)},
		{"int16", func() arrow.Array {
			b := array.NewInt16Builder(newGoMem())
			defer b.Release()
			b.AppendValues([]int16{5, 0}, []bool{true, false})
			return b.NewArray()
		}, int16(-5)},
		{"int32", func() arrow.Array {
			b := array.NewInt32Builder(newGoMem())
			defer b.Release()
			b.AppendValues([]int32{5, 0}, []bool{true, false})
			return b.NewArray()
		}, int32(-5)},
		{"int64", func() arrow.Array {
			b := array.NewInt64Builder(newGoMem())
			defer b.Release()
			b.AppendValues([]int64{5, 0}, []bool{true, false})
			return b.NewArray()
		}, int64(-5)},
		{"float32", func() arrow.Array {
			b := array.NewFloat32Builder(newGoMem())
			defer b.Release()
			b.AppendValues([]float32{5, 0}, []bool{true, false})
			return b.NewArray()
		}, float32(-5)},
		{"float64", func() arrow.Array {
			b := array.NewFloat64Builder(newGoMem())
			defer b.Release()
			b.AppendValues([]float64{5, 0}, []bool{true, false})
			return b.NewArray()
		}, float64(-5)},
	}

	valAt := func(a arrow.Array, i int) any {
		switch v := a.(type) {
		case *array.Int8:
			return v.Value(i)
		case *array.Int16:
			return v.Value(i)
		case *array.Int32:
			return v.Value(i)
		case *array.Int64:
			return v.Value(i)
		case *array.Float32:
			return v.Value(i)
		case *array.Float64:
			return v.Value(i)
		}
		return nil
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			rec := oneColRecord(t, "x", tc.build())
			defer rec.Release()
			out, err := Eval(expr.Col("x").Neg().Node, rec, newGoMem())
			require.NoError(t, err)
			defer out.Release()
			require.Equal(t, tc.neg0, valAt(out, 0))
			require.True(t, out.IsNull(1))
		})
	}
}

// TestNeg_UnsignedRejected confirms negation of an unsigned column errors (no
// in-type result and no registered kernel).
func TestNeg_UnsignedRejected(t *testing.T) {
	b := array.NewUint32Builder(newGoMem())
	b.AppendValues([]uint32{1, 2}, nil)
	rec := oneColRecord(t, "x", b.NewArray())
	b.Release()
	defer rec.Release()
	_, err := Eval(expr.Col("x").Neg().Node, rec, newGoMem())
	require.Error(t, err)
}

// TestNullMask_IsNullIsNotNull drives nullMask in both directions.
func TestNullMask_IsNullIsNotNull(t *testing.T) {
	b := array.NewInt64Builder(newGoMem())
	b.AppendValues([]int64{1, 0, 3}, []bool{true, false, true})
	rec := oneColRecord(t, "x", b.NewArray())
	b.Release()
	defer rec.Release()

	isNull, err := Eval(expr.Col("x").IsNull().Node, rec, newGoMem())
	require.NoError(t, err)
	defer isNull.Release()
	m := isNull.(*array.Boolean)
	require.Equal(t, []bool{false, true, false}, []bool{m.Value(0), m.Value(1), m.Value(2)})
	require.Equal(t, 0, m.NullN())

	isNotNull, err := Eval(expr.Col("x").IsNotNull().Node, rec, newGoMem())
	require.NoError(t, err)
	defer isNotNull.Release()
	m2 := isNotNull.(*array.Boolean)
	require.Equal(t, []bool{true, false, true}, []bool{m2.Value(0), m2.Value(1), m2.Value(2)})
}

// TestNot_RequiresBool rejects Not on a non-bool operand.
func TestNot_RequiresBool(t *testing.T) {
	rec := int64Record(t, "x", []int64{1, 2})
	defer rec.Release()
	_, err := Eval(expr.Col("x").Not().Node, rec, newGoMem())
	require.Error(t, err)
}

// TestNot_PropagatesNull drives notKernel including null propagation.
func TestNot_PropagatesNull(t *testing.T) {
	b := array.NewBooleanBuilder(newGoMem())
	b.AppendValues([]bool{true, false, false}, []bool{true, true, false}) // row 2 null
	rec := oneColRecord(t, "x", b.NewArray())
	b.Release()
	defer rec.Release()
	out, err := Eval(expr.Col("x").Not().Node, rec, newGoMem())
	require.NoError(t, err)
	defer out.Release()
	res := out.(*array.Boolean)
	require.False(t, res.Value(0))
	require.True(t, res.Value(1))
	require.True(t, res.IsNull(2))
}

// TestLogical_RequiresBoolOperands rejects AND with a non-bool left/right.
func TestLogical_RequiresBoolOperands(t *testing.T) {

	t.Run("non-bool left", func(t *testing.T) {
		ib := array.NewInt64Builder(newGoMem())
		ib.AppendValues([]int64{1}, nil)
		ia := ib.NewArray()
		ib.Release()
		bb := array.NewBooleanBuilder(newGoMem())
		bb.AppendValues([]bool{true}, nil)
		ba := bb.NewArray()
		bb.Release()
		rec := twoColRecord(t, "l", ia, "r", ba)
		defer rec.Release()
		_, err := Eval(expr.Col("l").And(expr.Col("r")).Node, rec, newGoMem())
		require.Error(t, err)
	})

	t.Run("non-bool right", func(t *testing.T) {
		bb := array.NewBooleanBuilder(newGoMem())
		bb.AppendValues([]bool{true}, nil)
		ba := bb.NewArray()
		bb.Release()
		ib := array.NewInt64Builder(newGoMem())
		ib.AppendValues([]int64{1}, nil)
		ia := ib.NewArray()
		ib.Release()
		rec := twoColRecord(t, "l", ba, "r", ia)
		defer rec.Release()
		_, err := Eval(expr.Col("l").Or(expr.Col("r")).Node, rec, newGoMem())
		require.Error(t, err)
	})
}
