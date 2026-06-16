package compute

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/require"

	"github.com/karthedew/cosma/expr"
)

func tsArray(t *testing.T, mem memory.Allocator, unit arrow.TimeUnit, tz string, vals []arrow.Timestamp, valid []bool) arrow.Array {
	t.Helper()
	dt := &arrow.TimestampType{Unit: unit, TimeZone: tz}
	b := array.NewTimestampBuilder(mem, dt)
	defer b.Release()
	b.AppendValues(vals, valid)
	return b.NewArray()
}

func durArray(t *testing.T, mem memory.Allocator, unit arrow.TimeUnit, vals []arrow.Duration, valid []bool) arrow.Array {
	t.Helper()
	dt := &arrow.DurationType{Unit: unit}
	b := array.NewDurationBuilder(mem, dt)
	defer b.Release()
	b.AppendValues(vals, valid)
	return b.NewArray()
}

func TestTemporal_TimestampMinusTimestamp(t *testing.T) {
	mem := newGoMem()
	l := tsArray(t, mem, arrow.Second, "UTC", []arrow.Timestamp{100, 50}, []bool{true, false})
	r := tsArray(t, mem, arrow.Second, "UTC", []arrow.Timestamp{40, 10}, nil)
	rec := twoColRecord(t, "l", l, "r", r)
	defer rec.Release()

	out, err := Eval(expr.Col("l").Sub(expr.Col("r")).Node, rec, mem)
	require.NoError(t, err)
	defer out.Release()
	require.Equal(t, arrow.DURATION, out.DataType().ID())
	d := out.(*array.Duration)
	require.Equal(t, arrow.Duration(60), d.Value(0))
	require.True(t, d.IsNull(1)) // null propagates
}

func TestTemporal_TimestampPlusMinusDuration(t *testing.T) {
	mem := newGoMem()

	t.Run("add", func(t *testing.T) {
		l := tsArray(t, mem, arrow.Second, "UTC", []arrow.Timestamp{100}, nil)
		r := durArray(t, mem, arrow.Second, []arrow.Duration{5}, nil)
		rec := twoColRecord(t, "l", l, "r", r)
		defer rec.Release()
		out, err := Eval(expr.Col("l").Add(expr.Col("r")).Node, rec, mem)
		require.NoError(t, err)
		defer out.Release()
		require.Equal(t, arrow.TIMESTAMP, out.DataType().ID())
		require.Equal(t, arrow.Timestamp(105), out.(*array.Timestamp).Value(0))
	})

	t.Run("sub", func(t *testing.T) {
		l := tsArray(t, mem, arrow.Second, "UTC", []arrow.Timestamp{100}, nil)
		r := durArray(t, mem, arrow.Second, []arrow.Duration{30}, nil)
		rec := twoColRecord(t, "l", l, "r", r)
		defer rec.Release()
		out, err := Eval(expr.Col("l").Sub(expr.Col("r")).Node, rec, mem)
		require.NoError(t, err)
		defer out.Release()
		require.Equal(t, arrow.Timestamp(70), out.(*array.Timestamp).Value(0))
	})
}

func TestTemporal_DurationPlusTimestamp(t *testing.T) {
	mem := newGoMem()
	l := durArray(t, mem, arrow.Second, []arrow.Duration{5}, nil)
	r := tsArray(t, mem, arrow.Second, "UTC", []arrow.Timestamp{100}, nil)
	rec := twoColRecord(t, "l", l, "r", r)
	defer rec.Release()
	out, err := Eval(expr.Col("l").Add(expr.Col("r")).Node, rec, mem)
	require.NoError(t, err)
	defer out.Release()
	require.Equal(t, arrow.Timestamp(105), out.(*array.Timestamp).Value(0))
}

func TestTemporal_Errors(t *testing.T) {
	mem := newGoMem()

	t.Run("ts plus ts rejected", func(t *testing.T) {
		l := tsArray(t, mem, arrow.Second, "UTC", []arrow.Timestamp{1}, nil)
		r := tsArray(t, mem, arrow.Second, "UTC", []arrow.Timestamp{1}, nil)
		rec := twoColRecord(t, "l", l, "r", r)
		defer rec.Release()
		_, err := Eval(expr.Col("l").Add(expr.Col("r")).Node, rec, mem)
		require.Error(t, err)
	})

	t.Run("ts/ts unit mismatch", func(t *testing.T) {
		l := tsArray(t, mem, arrow.Second, "UTC", []arrow.Timestamp{1}, nil)
		r := tsArray(t, mem, arrow.Millisecond, "UTC", []arrow.Timestamp{1}, nil)
		rec := twoColRecord(t, "l", l, "r", r)
		defer rec.Release()
		_, err := Eval(expr.Col("l").Sub(expr.Col("r")).Node, rec, mem)
		require.Error(t, err)
	})

	t.Run("ts/dur mul rejected", func(t *testing.T) {
		l := tsArray(t, mem, arrow.Second, "UTC", []arrow.Timestamp{1}, nil)
		r := durArray(t, mem, arrow.Second, []arrow.Duration{1}, nil)
		rec := twoColRecord(t, "l", l, "r", r)
		defer rec.Release()
		_, err := Eval(expr.Col("l").Mul(expr.Col("r")).Node, rec, mem)
		require.Error(t, err)
	})

	t.Run("ts/dur unit mismatch", func(t *testing.T) {
		l := tsArray(t, mem, arrow.Second, "UTC", []arrow.Timestamp{1}, nil)
		r := durArray(t, mem, arrow.Millisecond, []arrow.Duration{1}, nil)
		rec := twoColRecord(t, "l", l, "r", r)
		defer rec.Release()
		_, err := Eval(expr.Col("l").Add(expr.Col("r")).Node, rec, mem)
		require.Error(t, err)
	})

	t.Run("dur plus ts subtraction rejected", func(t *testing.T) {
		l := durArray(t, mem, arrow.Second, []arrow.Duration{1}, nil)
		r := tsArray(t, mem, arrow.Second, "UTC", []arrow.Timestamp{1}, nil)
		rec := twoColRecord(t, "l", l, "r", r)
		defer rec.Release()
		_, err := Eval(expr.Col("l").Sub(expr.Col("r")).Node, rec, mem)
		require.Error(t, err)
	})

	t.Run("dur/ts unit mismatch", func(t *testing.T) {
		l := durArray(t, mem, arrow.Second, []arrow.Duration{1}, nil)
		r := tsArray(t, mem, arrow.Millisecond, "UTC", []arrow.Timestamp{1}, nil)
		rec := twoColRecord(t, "l", l, "r", r)
		defer rec.Release()
		_, err := Eval(expr.Col("l").Add(expr.Col("r")).Node, rec, mem)
		require.Error(t, err)
	})
}

func TestIsTemporalArith(t *testing.T) {
	ts := &arrow.TimestampType{Unit: arrow.Second}
	dur := &arrow.DurationType{Unit: arrow.Second}
	i64 := arrow.PrimitiveTypes.Int64

	require.True(t, isTemporalArith(ts, ts))
	require.True(t, isTemporalArith(ts, dur))
	require.True(t, isTemporalArith(dur, ts))
	require.False(t, isTemporalArith(dur, dur))
	require.False(t, isTemporalArith(i64, i64))
	require.False(t, isTemporalArith(ts, i64))
}
