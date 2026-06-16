package compute

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/stretchr/testify/require"

	"github.com/karthedew/cosma/expr"
)

func TestEval_NilInputs(t *testing.T) {

	t.Run("nil expr", func(t *testing.T) {
		rec := int64Record(t, "a", []int64{1})
		defer rec.Release()
		_, err := Eval(nil, rec, newGoMem())
		require.Error(t, err)
	})

	t.Run("nil record", func(t *testing.T) {
		_, err := Eval(expr.Col("a").Node, nil, newGoMem())
		require.Error(t, err)
	})

	t.Run("unknown column", func(t *testing.T) {
		rec := int64Record(t, "a", []int64{1})
		defer rec.Release()
		_, err := Eval(expr.Col("missing").Node, rec, newGoMem())
		require.Error(t, err)
	})
}

// TestEval_Alias verifies AliasNode transparently evaluates its inner expr.
func TestEval_Alias(t *testing.T) {
	rec := int64Record(t, "a", []int64{5, 6})
	defer rec.Release()
	out, err := Eval(expr.Col("a").As("renamed").Node, rec, newGoMem())
	require.NoError(t, err)
	defer out.Release()
	require.Equal(t, int64(5), out.(*array.Int64).Value(0))
}

// TestEval_NilAllocatorDefaults ensures a nil allocator falls back to the
// default rather than panicking.
func TestEval_NilAllocatorDefaults(t *testing.T) {
	rec := int64Record(t, "a", []int64{1, 2})
	defer rec.Release()
	out, err := Eval(expr.Col("a").Add(expr.Lit(int64(1))).Node, rec, nil)
	require.NoError(t, err)
	defer out.Release()
	require.Equal(t, int64(2), out.(*array.Int64).Value(0))
}

// TestCompare_AcrossDtypes drives compareKernel for the numeric element types
// not already exercised, asserting the boolean result of Lt.
func TestCompare_AcrossDtypes(t *testing.T) {

	cases := []struct {
		name  string
		left  func() arrow.Array
		right func() arrow.Array
		want  []bool // left < right
	}{
		{"int8", func() arrow.Array {
			b := array.NewInt8Builder(newGoMem())
			defer b.Release()
			b.AppendValues([]int8{1, 5}, nil)
			return b.NewArray()
		}, func() arrow.Array {
			b := array.NewInt8Builder(newGoMem())
			defer b.Release()
			b.AppendValues([]int8{2, 2}, nil)
			return b.NewArray()
		}, []bool{true, false}},
		{"int16", func() arrow.Array {
			b := array.NewInt16Builder(newGoMem())
			defer b.Release()
			b.AppendValues([]int16{1, 5}, nil)
			return b.NewArray()
		}, func() arrow.Array {
			b := array.NewInt16Builder(newGoMem())
			defer b.Release()
			b.AppendValues([]int16{2, 2}, nil)
			return b.NewArray()
		}, []bool{true, false}},
		{"int32", func() arrow.Array {
			b := array.NewInt32Builder(newGoMem())
			defer b.Release()
			b.AppendValues([]int32{1, 5}, nil)
			return b.NewArray()
		}, func() arrow.Array {
			b := array.NewInt32Builder(newGoMem())
			defer b.Release()
			b.AppendValues([]int32{2, 2}, nil)
			return b.NewArray()
		}, []bool{true, false}},
		{"uint8", func() arrow.Array {
			b := array.NewUint8Builder(newGoMem())
			defer b.Release()
			b.AppendValues([]uint8{1, 5}, nil)
			return b.NewArray()
		}, func() arrow.Array {
			b := array.NewUint8Builder(newGoMem())
			defer b.Release()
			b.AppendValues([]uint8{2, 2}, nil)
			return b.NewArray()
		}, []bool{true, false}},
		{"uint16", func() arrow.Array {
			b := array.NewUint16Builder(newGoMem())
			defer b.Release()
			b.AppendValues([]uint16{1, 5}, nil)
			return b.NewArray()
		}, func() arrow.Array {
			b := array.NewUint16Builder(newGoMem())
			defer b.Release()
			b.AppendValues([]uint16{2, 2}, nil)
			return b.NewArray()
		}, []bool{true, false}},
		{"uint32", func() arrow.Array {
			b := array.NewUint32Builder(newGoMem())
			defer b.Release()
			b.AppendValues([]uint32{1, 5}, nil)
			return b.NewArray()
		}, func() arrow.Array {
			b := array.NewUint32Builder(newGoMem())
			defer b.Release()
			b.AppendValues([]uint32{2, 2}, nil)
			return b.NewArray()
		}, []bool{true, false}},
		{"uint64", func() arrow.Array {
			b := array.NewUint64Builder(newGoMem())
			defer b.Release()
			b.AppendValues([]uint64{1, 5}, nil)
			return b.NewArray()
		}, func() arrow.Array {
			b := array.NewUint64Builder(newGoMem())
			defer b.Release()
			b.AppendValues([]uint64{2, 2}, nil)
			return b.NewArray()
		}, []bool{true, false}},
		{"float32", func() arrow.Array {
			b := array.NewFloat32Builder(newGoMem())
			defer b.Release()
			b.AppendValues([]float32{1, 5}, nil)
			return b.NewArray()
		}, func() arrow.Array {
			b := array.NewFloat32Builder(newGoMem())
			defer b.Release()
			b.AppendValues([]float32{2, 2}, nil)
			return b.NewArray()
		}, []bool{true, false}},
		{"largestring", func() arrow.Array {
			b := array.NewLargeStringBuilder(newGoMem())
			defer b.Release()
			b.AppendValues([]string{"a", "z"}, nil)
			return b.NewArray()
		}, func() arrow.Array {
			b := array.NewLargeStringBuilder(newGoMem())
			defer b.Release()
			b.AppendValues([]string{"b", "b"}, nil)
			return b.NewArray()
		}, []bool{true, false}},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			rec := twoColRecord(t, "l", tc.left(), "r", tc.right())
			defer rec.Release()
			out, err := Eval(expr.Col("l").Lt(expr.Col("r")).Node, rec, newGoMem())
			require.NoError(t, err)
			defer out.Release()
			res := out.(*array.Boolean)
			for i, w := range tc.want {
				require.Equal(t, w, res.Value(i), "row %d", i)
			}
		})
	}
}

// TestCompare_Binary exercises the BINARY / LARGE_BINARY bytesCompare path.
func TestCompare_Binary(t *testing.T) {

	build := func(b *array.BinaryBuilder, vals [][]byte) arrow.Array {
		defer b.Release()
		for _, v := range vals {
			b.Append(v)
		}
		return b.NewArray()
	}

	t.Run("binary lt", func(t *testing.T) {
		l := build(array.NewBinaryBuilder(newGoMem(), arrow.BinaryTypes.Binary), [][]byte{[]byte("aa"), []byte("zz")})
		r := build(array.NewBinaryBuilder(newGoMem(), arrow.BinaryTypes.Binary), [][]byte{[]byte("ab"), []byte("ab")})
		rec := twoColRecord(t, "l", l, "r", r)
		defer rec.Release()
		out, err := Eval(expr.Col("l").Lt(expr.Col("r")).Node, rec, newGoMem())
		require.NoError(t, err)
		defer out.Release()
		res := out.(*array.Boolean)
		require.True(t, res.Value(0))
		require.False(t, res.Value(1))
	})

	t.Run("binary eq with null", func(t *testing.T) {
		lb := array.NewBinaryBuilder(newGoMem(), arrow.BinaryTypes.Binary)
		lb.Append([]byte("x"))
		lb.AppendNull()
		la := lb.NewArray()
		lb.Release()
		rb := array.NewBinaryBuilder(newGoMem(), arrow.BinaryTypes.Binary)
		rb.Append([]byte("x"))
		rb.Append([]byte("y"))
		ra := rb.NewArray()
		rb.Release()
		rec := twoColRecord(t, "l", la, "r", ra)
		defer rec.Release()
		out, err := Eval(expr.Col("l").Eq(expr.Col("r")).Node, rec, newGoMem())
		require.NoError(t, err)
		defer out.Release()
		res := out.(*array.Boolean)
		require.True(t, res.Value(0))
		require.True(t, res.IsNull(1)) // null propagates
	})
}

// TestCompare_BoolEqNeq drives the boolCompare path for the two supported ops
// and rejects an ordered op on bool.
func TestCompare_BoolEqNeq(t *testing.T) {

	mkBoolRec := func() arrow.Record {
		la := array.NewBooleanBuilder(newGoMem())
		la.AppendValues([]bool{true, false}, nil)
		ra := array.NewBooleanBuilder(newGoMem())
		ra.AppendValues([]bool{true, true}, nil)
		l, r := la.NewArray(), ra.NewArray()
		la.Release()
		ra.Release()
		return twoColRecord(t, "l", l, "r", r)
	}

	t.Run("eq", func(t *testing.T) {
		rec := mkBoolRec()
		defer rec.Release()
		out, err := Eval(expr.Col("l").Eq(expr.Col("r")).Node, rec, newGoMem())
		require.NoError(t, err)
		defer out.Release()
		res := out.(*array.Boolean)
		require.True(t, res.Value(0))
		require.False(t, res.Value(1))
	})

	t.Run("neq", func(t *testing.T) {
		rec := mkBoolRec()
		defer rec.Release()
		out, err := Eval(expr.Col("l").Neq(expr.Col("r")).Node, rec, newGoMem())
		require.NoError(t, err)
		defer out.Release()
		res := out.(*array.Boolean)
		require.False(t, res.Value(0))
		require.True(t, res.Value(1))
	})

	t.Run("ordered op on bool rejected", func(t *testing.T) {
		rec := mkBoolRec()
		defer rec.Release()
		_, err := Eval(expr.Col("l").Lt(expr.Col("r")).Node, rec, newGoMem())
		require.Error(t, err)
	})
}

func TestCompare_Errors(t *testing.T) {

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
		_, err := Eval(expr.Col("l").Eq(expr.Col("r")).Node, rec, newGoMem())
		require.Error(t, err)
	})

	t.Run("unsupported type for op", func(t *testing.T) {
		// Duration is comparable, but comparing it under arith op path is not.
		// Instead use a list-of type that has no compare kernel registered.
		dt := arrow.ListOf(arrow.PrimitiveTypes.Int64)
		lb := array.NewListBuilder(newGoMem(), arrow.PrimitiveTypes.Int64)
		lb.Append(true)
		lb.ValueBuilder().(*array.Int64Builder).Append(1)
		la := lb.NewArray()
		lb.Release()
		lb2 := array.NewListBuilder(newGoMem(), arrow.PrimitiveTypes.Int64)
		lb2.Append(true)
		lb2.ValueBuilder().(*array.Int64Builder).Append(1)
		ra := lb2.NewArray()
		lb2.Release()
		_ = dt
		rec := twoColRecord(t, "l", la, "r", ra)
		defer rec.Release()
		_, err := Eval(expr.Col("l").Eq(expr.Col("r")).Node, rec, newGoMem())
		require.Error(t, err)
	})
}

// TestEvalLiteral_Coercion drives every literal broadcast type, including the
// typed constructors and the untyped widening paths.
func TestEvalLiteral_Coercion(t *testing.T) {
	rec := int64Record(t, "a", []int64{0, 0, 0}) // length 3 carrier
	defer rec.Release()

	cases := []struct {
		name string
		lit  expr.Expr
		id   arrow.Type
		want any // value at row 0
	}{
		{"int8", expr.Int8(7), arrow.INT8, int8(7)},
		{"int16", expr.Int16(7), arrow.INT16, int16(7)},
		{"int32", expr.Int32(7), arrow.INT32, int32(7)},
		{"int64 typed", expr.Int64(7), arrow.INT64, int64(7)},
		{"int64 untyped", expr.Lit(7), arrow.INT64, int64(7)},
		{"uint8", expr.Uint8(7), arrow.UINT8, uint8(7)},
		{"uint16", expr.Uint16(7), arrow.UINT16, uint16(7)},
		{"uint32", expr.Uint32(7), arrow.UINT32, uint32(7)},
		{"uint64", expr.Uint64(7), arrow.UINT64, uint64(7)},
		{"float32", expr.Float32(2.5), arrow.FLOAT32, float32(2.5)},
		{"float64 untyped", expr.Lit(2.5), arrow.FLOAT64, float64(2.5)},
		{"string", expr.Lit("hi"), arrow.STRING, "hi"},
		{"bool", expr.Lit(true), arrow.BOOL, true},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			out, err := Eval(tc.lit.Node, rec, newGoMem())
			require.NoError(t, err)
			defer out.Release()
			require.Equal(t, tc.id, out.DataType().ID())
			require.Equal(t, 3, out.Len())
			switch v := tc.want.(type) {
			case int8:
				require.Equal(t, v, out.(*array.Int8).Value(0))
			case int16:
				require.Equal(t, v, out.(*array.Int16).Value(0))
			case int32:
				require.Equal(t, v, out.(*array.Int32).Value(0))
			case int64:
				require.Equal(t, v, out.(*array.Int64).Value(0))
			case uint8:
				require.Equal(t, v, out.(*array.Uint8).Value(0))
			case uint16:
				require.Equal(t, v, out.(*array.Uint16).Value(0))
			case uint32:
				require.Equal(t, v, out.(*array.Uint32).Value(0))
			case uint64:
				require.Equal(t, v, out.(*array.Uint64).Value(0))
			case float32:
				require.Equal(t, v, out.(*array.Float32).Value(0))
			case float64:
				require.Equal(t, v, out.(*array.Float64).Value(0))
			case string:
				require.Equal(t, v, out.(*array.String).Value(0))
			case bool:
				require.Equal(t, v, out.(*array.Boolean).Value(0))
			}
		})
	}
}
