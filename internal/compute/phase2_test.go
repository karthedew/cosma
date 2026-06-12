package compute

import (
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/require"

	"github.com/karthedew/cosma/expr"
)

// boolRecord builds a one-column nullable boolean record. valid[i]==false marks
// row i as null.
func boolRecord(t *testing.T, name string, vals, valid []bool) arrow.Record {
	t.Helper()
	bld := array.NewBooleanBuilder(memory.DefaultAllocator)
	defer bld.Release()
	bld.AppendValues(vals, valid)
	arr := bld.NewArray()
	defer arr.Release()
	schema := arrow.NewSchema([]arrow.Field{{Name: name, Type: arrow.FixedWidthTypes.Boolean}}, nil)
	return array.NewRecord(schema, []arrow.Array{arr}, int64(len(vals)))
}

// boolArr evaluates e against rec and returns the boolean result with a parallel
// "valid" slice (false == null).
func boolArr(t *testing.T, e expr.Expr, rec arrow.Record) ([]bool, []bool) {
	t.Helper()
	out, err := Eval(e.Node, rec, memory.DefaultAllocator)
	require.NoError(t, err)
	defer out.Release()
	b, ok := out.(*array.Boolean)
	require.True(t, ok, "result type = %T, want *array.Boolean", out)
	vals := make([]bool, b.Len())
	valid := make([]bool, b.Len())
	for i := range vals {
		valid[i] = !b.IsNull(i)
		if valid[i] {
			vals[i] = b.Value(i)
		}
	}
	return vals, valid
}

// TB1: comparison conjunction + Kleene AND null behavior.
func TestTB1_AndKleene(t *testing.T) {
	t.Parallel()
	// a AND b across the Kleene truth table; row order:
	// (T,T)=T (T,F)=F (F,T)=F (F,F)=F (NULL,F)=F (NULL,T)=NULL (T,NULL)=NULL (NULL,NULL)=NULL
	a := boolRecordTwo(t,
		[]bool{true, true, false, false, false, false, true, false},
		[]bool{true, true, true, true, false, false, true, false},
		[]bool{true, false, true, false, false, true, false, false},
		[]bool{true, true, true, true, true, true, false, false},
	)
	defer a.Release()

	vals, valid := boolArr(t, expr.Col("a").And(expr.Col("b")), a)
	require.Equal(t, []bool{true, false, false, false, false, false, false, false}, vals)
	require.Equal(t, []bool{true, true, true, true, true, false, false, false}, valid)
}

// TB4: Kleene OR null behavior.
func TestTB4_OrKleene(t *testing.T) {
	t.Parallel()
	// (T,T)=T (F,F)=F (NULL,T)=T (NULL,F)=NULL (T,NULL)=T (F,NULL)=NULL (NULL,NULL)=NULL
	a := boolRecordTwo(t,
		[]bool{true, false, false, false, true, false, false},
		[]bool{true, true, false, false, true, false, false},
		[]bool{true, false, true, false, false, false, false},
		[]bool{true, true, true, true, false, false, false},
	)
	defer a.Release()

	vals, valid := boolArr(t, expr.Col("a").Or(expr.Col("b")), a)
	require.Equal(t, []bool{true, false, true, false, true, false, false}, vals)
	require.Equal(t, []bool{true, true, true, false, true, false, false}, valid)
}

// boolRecordTwo builds a two-column nullable boolean record (a, b).
func boolRecordTwo(t *testing.T, aVals, aValid, bVals, bValid []bool) arrow.Record {
	t.Helper()
	mem := memory.DefaultAllocator
	ab := array.NewBooleanBuilder(mem)
	defer ab.Release()
	ab.AppendValues(aVals, aValid)
	aArr := ab.NewArray()
	defer aArr.Release()
	bb := array.NewBooleanBuilder(mem)
	defer bb.Release()
	bb.AppendValues(bVals, bValid)
	bArr := bb.NewArray()
	defer bArr.Release()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "a", Type: arrow.FixedWidthTypes.Boolean},
		{Name: "b", Type: arrow.FixedWidthTypes.Boolean},
	}, nil)
	return array.NewRecord(schema, []arrow.Array{aArr, bArr}, int64(len(aVals)))
}

// TB2: logical NOT (null-propagating) and arithmetic negation.
func TestTB2_NotAndNeg(t *testing.T) {
	t.Parallel()

	rec := boolRecord(t, "active", []bool{true, false, false}, []bool{true, true, false})
	defer rec.Release()
	vals, valid := boolArr(t, expr.Col("active").Not(), rec)
	require.Equal(t, []bool{false, true, false}, vals)
	require.Equal(t, []bool{true, true, false}, valid) // null stays null

	irec := int64Record(t, "n", []int64{5, -3, 0})
	defer irec.Release()
	out, err := Eval(expr.Col("n").Neg().Node, irec, memory.DefaultAllocator)
	require.NoError(t, err)
	defer out.Release()
	got := out.(*array.Int64)
	require.Equal(t, int64(-5), got.Value(0))
	require.Equal(t, int64(3), got.Value(1))
	require.Equal(t, int64(0), got.Value(2))
}

// TB3: IsNull / IsNotNull on a nullable column.
func TestTB3_NullTests(t *testing.T) {
	t.Parallel()
	mem := memory.DefaultAllocator
	bld := array.NewInt64Builder(mem)
	defer bld.Release()
	bld.AppendValues([]int64{1, 0, 3}, []bool{true, false, true})
	arr := bld.NewArray()
	defer arr.Release()
	schema := arrow.NewSchema([]arrow.Field{{Name: "x", Type: arrow.PrimitiveTypes.Int64}}, nil)
	rec := array.NewRecord(schema, []arrow.Array{arr}, 3)
	defer rec.Release()

	isNull, vN := boolArr(t, expr.Col("x").IsNull(), rec)
	require.Equal(t, []bool{false, true, false}, isNull)
	require.Equal(t, []bool{true, true, true}, vN) // never null

	isNot, vNN := boolArr(t, expr.Col("x").IsNotNull(), rec)
	require.Equal(t, []bool{true, false, true}, isNot)
	require.Equal(t, []bool{true, true, true}, vNN)
}

// TB5: Cast int64 -> float64.
func TestTB5_Cast(t *testing.T) {
	t.Parallel()
	rec := int64Record(t, "age", []int64{1, 2, 3})
	defer rec.Release()
	out, err := Eval(expr.Col("age").Cast(arrow.PrimitiveTypes.Float64).Node, rec, memory.DefaultAllocator)
	require.NoError(t, err)
	defer out.Release()
	f, ok := out.(*array.Float64)
	require.True(t, ok, "type = %T", out)
	require.Equal(t, []float64{1, 2, 3}, f.Float64Values())
}

// TB6: string comparison.
func TestTB6_StringCompare(t *testing.T) {
	t.Parallel()
	mem := memory.DefaultAllocator
	bld := array.NewStringBuilder(mem)
	defer bld.Release()
	bld.AppendValues([]string{"Alice", "Bob", "Alice"}, nil)
	arr := bld.NewArray()
	defer arr.Release()
	schema := arrow.NewSchema([]arrow.Field{{Name: "name", Type: arrow.BinaryTypes.String}}, nil)
	rec := array.NewRecord(schema, []arrow.Array{arr}, 3)
	defer rec.Release()

	vals, _ := boolArr(t, expr.Col("name").Eq(expr.Lit("Alice")), rec)
	require.Equal(t, []bool{true, false, true}, vals)
}

// TB7: LitTimestamp comparison on a timestamp column.
func TestTB7_TimestampCompare(t *testing.T) {
	t.Parallel()
	mem := memory.DefaultAllocator
	tsType := &arrow.TimestampType{Unit: arrow.Nanosecond, TimeZone: "UTC"}
	bld := array.NewTimestampBuilder(mem, tsType)
	defer bld.Release()
	base := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	bld.AppendValues([]arrow.Timestamp{
		arrow.Timestamp(base.UnixNano()),
		arrow.Timestamp(base.Add(48 * time.Hour).UnixNano()),
	}, nil)
	arr := bld.NewArray()
	defer arr.Release()
	schema := arrow.NewSchema([]arrow.Field{{Name: "ts", Type: tsType}}, nil)
	rec := array.NewRecord(schema, []arrow.Array{arr}, 2)
	defer rec.Release()

	cut := base.Add(24 * time.Hour)
	vals, _ := boolArr(t, expr.Col("ts").Gt(expr.LitTimestamp(cut, "UTC")), rec)
	require.Equal(t, []bool{false, true}, vals)
}

// TB7b: Timestamp - Timestamp -> Duration, Timestamp + Duration -> Timestamp.
func TestTB7_TemporalArith(t *testing.T) {
	t.Parallel()
	mem := memory.DefaultAllocator
	tsType := &arrow.TimestampType{Unit: arrow.Nanosecond}
	mk := func(name string, vals []arrow.Timestamp) arrow.Array {
		b := array.NewTimestampBuilder(mem, tsType)
		defer b.Release()
		b.AppendValues(vals, nil)
		return b.NewArray()
	}
	a := mk("a", []arrow.Timestamp{100, 200})
	defer a.Release()
	bArr := mk("b", []arrow.Timestamp{40, 50})
	defer bArr.Release()

	out, err := arithKernel(expr.BinaryOpSub, a, bArr, mem)
	require.NoError(t, err)
	defer out.Release()
	dur, ok := out.(*array.Duration)
	require.True(t, ok, "type = %T", out)
	require.Equal(t, arrow.Duration(60), dur.Value(0))
	require.Equal(t, arrow.Duration(150), dur.Value(1))

	// Timestamp + Duration -> Timestamp
	db := array.NewDurationBuilder(mem, &arrow.DurationType{Unit: arrow.Nanosecond})
	defer db.Release()
	db.AppendValues([]arrow.Duration{5, 5}, nil)
	d := db.NewArray()
	defer d.Release()
	out2, err := arithKernel(expr.BinaryOpAdd, a, d, mem)
	require.NoError(t, err)
	defer out2.Release()
	ts := out2.(*array.Timestamp)
	require.Equal(t, arrow.Timestamp(105), ts.Value(0))
}

// TB8: registered kernels are dispatched for their type ID.
func TestTB8_KernelRegistration(t *testing.T) {
	t.Parallel()
	// EXTENSION-typed arrays are hard to fabricate here; instead register a
	// kernel for a built-in type whose evalBinary path falls through to the
	// registry only when the built-in returns errNoBinaryKernel. We verify the
	// registry lookup wiring directly.
	called := false
	RegisterBinaryKernel(arrow.EXTENSION, func(op expr.BinaryOp, l, r arrow.Array, mem memory.Allocator) (arrow.Array, error) {
		called = true
		return nil, nil
	})
	k, ok := lookupBinaryKernel(arrow.EXTENSION)
	require.True(t, ok)
	_, _ = k(expr.BinaryOpAdd, nil, nil, memory.DefaultAllocator)
	require.True(t, called)

	RegisterUnaryKernel(arrow.EXTENSION, func(op expr.UnaryOp, in arrow.Array, mem memory.Allocator) (arrow.Array, error) {
		return nil, nil
	})
	_, ok = lookupUnaryKernel(arrow.EXTENSION)
	require.True(t, ok)
}

// TB8b: an unsupported built-in type yields a clean "type X not supported for op
// Y" error from the dispatch fallback, never a placeholder.
func TestTB8_DispatchCleanError(t *testing.T) {
	t.Parallel()
	mem := memory.DefaultAllocator
	// bool does not support arithmetic; arithKernel returns errNoBinaryKernel
	// and, with no registered kernel, dispatchBinary surfaces a clean error.
	b := array.NewBooleanBuilder(mem)
	defer b.Release()
	b.AppendValues([]bool{true, false}, nil)
	arr := b.NewArray()
	defer arr.Release()
	schema := arrow.NewSchema([]arrow.Field{{Name: "x", Type: arrow.FixedWidthTypes.Boolean}}, nil)
	rec := array.NewRecord(schema, []arrow.Array{arr}, 2)
	defer rec.Release()

	_, err := Eval(expr.Col("x").Add(expr.Col("x")).Node, rec, mem)
	require.Error(t, err)
	require.Contains(t, err.Error(), "not supported for op")
	require.NotContains(t, err.Error(), "not yet implemented")
}
