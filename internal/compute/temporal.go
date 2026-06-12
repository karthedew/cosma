package compute

import (
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/karthedew/cosma/expr"
)

// isTemporalArith reports whether (l, r) is a supported temporal arithmetic
// operand pair: timestamp/timestamp or timestamp/duration in either order.
func isTemporalArith(l, r arrow.DataType) bool {
	lt, rt := l.ID(), r.ID()
	switch {
	case lt == arrow.TIMESTAMP && rt == arrow.TIMESTAMP:
		return true
	case lt == arrow.TIMESTAMP && rt == arrow.DURATION:
		return true
	case lt == arrow.DURATION && rt == arrow.TIMESTAMP:
		return true
	default:
		return false
	}
}

// temporalArith implements the supported temporal arithmetic:
//
//	Timestamp - Timestamp -> Duration
//	Timestamp + Duration  -> Timestamp
//	Timestamp - Duration  -> Timestamp
//	Duration  + Timestamp -> Timestamp
//
// Both operands must share a time unit; the result carries the timestamp's unit
// (and timezone, for timestamp results). Nulls propagate.
func temporalArith(op expr.BinaryOp, l, r arrow.Array, mem memory.Allocator) (arrow.Array, error) {
	lt, rt := l.DataType().ID(), r.DataType().ID()

	switch {
	case lt == arrow.TIMESTAMP && rt == arrow.TIMESTAMP:
		if op != expr.BinaryOpSub {
			return nil, fmt.Errorf("compute.arith: only subtraction is defined for timestamp and timestamp, got %s", op)
		}
		lts := l.DataType().(*arrow.TimestampType)
		rts := r.DataType().(*arrow.TimestampType)
		if lts.Unit != rts.Unit {
			return nil, fmt.Errorf("compute.arith: timestamp unit mismatch (%s vs %s)", lts.Unit, rts.Unit)
		}
		la := l.(*array.Timestamp)
		ra := r.(*array.Timestamp)
		out := &arrow.DurationType{Unit: lts.Unit}
		return foldInt64(la.Len(), out, mem,
			func(i int) bool { return la.IsNull(i) || ra.IsNull(i) },
			func(i int) int64 { return int64(la.Value(i)) - int64(ra.Value(i)) }), nil

	case lt == arrow.TIMESTAMP && rt == arrow.DURATION:
		if op != expr.BinaryOpAdd && op != expr.BinaryOpSub {
			return nil, fmt.Errorf("compute.arith: only +/- is defined for timestamp and duration, got %s", op)
		}
		tsType := l.DataType().(*arrow.TimestampType)
		dType := r.DataType().(*arrow.DurationType)
		if tsType.Unit != dType.Unit {
			return nil, fmt.Errorf("compute.arith: timestamp/duration unit mismatch (%s vs %s)", tsType.Unit, dType.Unit)
		}
		ta := l.(*array.Timestamp)
		da := r.(*array.Duration)
		sign := int64(1)
		if op == expr.BinaryOpSub {
			sign = -1
		}
		return foldInt64(ta.Len(), tsType, mem,
			func(i int) bool { return ta.IsNull(i) || da.IsNull(i) },
			func(i int) int64 { return int64(ta.Value(i)) + sign*int64(da.Value(i)) }), nil

	case lt == arrow.DURATION && rt == arrow.TIMESTAMP:
		if op != expr.BinaryOpAdd {
			return nil, fmt.Errorf("compute.arith: only addition is defined for duration and timestamp, got %s", op)
		}
		dType := l.DataType().(*arrow.DurationType)
		tsType := r.DataType().(*arrow.TimestampType)
		if tsType.Unit != dType.Unit {
			return nil, fmt.Errorf("compute.arith: duration/timestamp unit mismatch (%s vs %s)", dType.Unit, tsType.Unit)
		}
		da := l.(*array.Duration)
		ta := r.(*array.Timestamp)
		return foldInt64(ta.Len(), tsType, mem,
			func(i int) bool { return da.IsNull(i) || ta.IsNull(i) },
			func(i int) int64 { return int64(da.Value(i)) + int64(ta.Value(i)) }), nil

	default:
		return nil, fmt.Errorf("compute.arith: temporal arithmetic not defined for %s and %s", l.DataType(), r.DataType())
	}
}

// foldInt64 builds an int64-backed Arrow array of the given output type by
// walking n rows, treating isNull as the null predicate and value as the
// per-row result. It backs both Timestamp and Duration outputs, which share an
// int64 storage layout.
func foldInt64(n int, out arrow.DataType, mem memory.Allocator, isNull func(int) bool, value func(int) int64) arrow.Array {
	bld := array.NewBuilder(mem, out)
	defer bld.Release()
	bld.Reserve(n)

	switch b := bld.(type) {
	case *array.TimestampBuilder:
		for i := 0; i < n; i++ {
			if isNull(i) {
				b.AppendNull()
				continue
			}
			b.Append(arrow.Timestamp(value(i)))
		}
	case *array.DurationBuilder:
		for i := 0; i < n; i++ {
			if isNull(i) {
				b.AppendNull()
				continue
			}
			b.Append(arrow.Duration(value(i)))
		}
	}
	return bld.NewArray()
}
