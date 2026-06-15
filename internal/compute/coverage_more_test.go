package compute

import (
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/require"

	"github.com/karthedew/cosma/expr"
	"github.com/karthedew/cosma/schema"
)

func stringChunked(t *testing.T, large bool, vals ...*string) *arrow.Chunked {
	t.Helper()
	mem := memory.DefaultAllocator
	if large {
		b := array.NewLargeStringBuilder(mem)
		for _, v := range vals {
			if v == nil {
				b.AppendNull()
			} else {
				b.Append(*v)
			}
		}
		a := b.NewArray()
		b.Release()
		ch := arrow.NewChunked(arrow.BinaryTypes.LargeString, []arrow.Array{a})
		a.Release()
		return ch
	}
	b := array.NewStringBuilder(mem)
	for _, v := range vals {
		if v == nil {
			b.AppendNull()
		} else {
			b.Append(*v)
		}
	}
	a := b.NewArray()
	b.Release()
	ch := arrow.NewChunked(arrow.BinaryTypes.String, []arrow.Array{a})
	a.Release()
	return ch
}

func strp(v string) *string { return &v }

func TestReduceOrderedAndErrors(t *testing.T) {
	for _, large := range []bool{false, true} {
		ch := stringChunked(t, large, strp("b"), nil, strp("a"), strp("c"))
		ag, err := Reduce(ch)
		ch.Release()
		require.NoError(t, err)
		require.Equal(t, int64(3), ag.Count)
		require.Nil(t, ag.Sum)
		require.Equal(t, "a", ag.Min)
		require.Equal(t, "c", ag.Max)
		require.Zero(t, ag.Mean)
	}

	_, err := Reduce(nil)
	require.ErrorContains(t, err, "nil column")

	b := array.NewBooleanBuilder(memory.DefaultAllocator)
	b.AppendValues([]bool{true}, nil)
	a := b.NewArray()
	b.Release()
	ch := arrow.NewChunked(arrow.FixedWidthTypes.Boolean, []arrow.Array{a})
	a.Release()
	defer ch.Release()
	_, err = Reduce(ch)
	require.ErrorContains(t, err, "not supported")
}

func TestGroupCountBoxedValuesAndBuildArray(t *testing.T) {
	mem := memory.DefaultAllocator

	bb := array.NewBooleanBuilder(mem)
	bb.Append(true)
	bb.AppendNull()
	bb.Append(false)
	boolArr := bb.NewArray()
	bb.Release()
	boolChunked := arrow.NewChunked(arrow.FixedWidthTypes.Boolean, []arrow.Array{boolArr})
	boolArr.Release()
	defer boolChunked.Release()

	ag, err := GroupReduce([]int{0, 0, 1}, 2, boolChunked)
	require.NoError(t, err)
	require.Equal(t, []Aggregates{{Count: 1}, {Count: 1}}, ag)

	boxed, err := BoxedValues(boolChunked)
	require.NoError(t, err)
	require.Equal(t, []any{true, nil, false}, boxed)

	built, err := BuildArray(arrow.FixedWidthTypes.Boolean, boxed, nil)
	require.NoError(t, err)
	gotBool := built.(*array.Boolean)
	require.True(t, gotBool.Value(0))
	require.True(t, gotBool.IsNull(1))
	require.False(t, gotBool.Value(2))
	built.Release()

	stringChunk := stringChunked(t, false, strp("x"), nil, strp("y"))
	boxed, err = BoxedValues(stringChunk)
	require.NoError(t, err)
	require.Equal(t, []any{"x", nil, "y"}, boxed)
	built, err = BuildArray(arrow.BinaryTypes.String, boxed, mem)
	require.NoError(t, err)
	require.Equal(t, "y", built.(*array.String).Value(2))
	built.Release()
	stringChunk.Release()

	_, err = GroupReduce([]int{0}, 1, nil)
	require.ErrorContains(t, err, "nil column")
	_, err = GroupReduce([]int{0}, 1, boolChunked)
	require.ErrorContains(t, err, "group ids")
	_, err = BoxedValues(nil)
	require.ErrorContains(t, err, "nil column")
	_, err = BuildArray(arrow.BinaryTypes.Binary, []any{[]byte("x")}, mem)
	require.ErrorContains(t, err, "not supported")
	_, err = BuildArray(arrow.PrimitiveTypes.Int64, []any{"bad"}, mem)
	require.ErrorContains(t, err, "expected int64")
}

func TestCompareKernelBinaryAndBool(t *testing.T) {
	mem := memory.DefaultAllocator

	lb := array.NewBinaryBuilder(mem, arrow.BinaryTypes.Binary)
	lb.Append([]byte("a"))
	lb.AppendNull()
	lb.Append([]byte("c"))
	left := lb.NewArray()
	lb.Release()
	defer left.Release()

	rb := array.NewBinaryBuilder(mem, arrow.BinaryTypes.Binary)
	rb.Append([]byte("b"))
	rb.Append([]byte("b"))
	rb.Append([]byte("c"))
	right := rb.NewArray()
	rb.Release()
	defer right.Release()

	out, err := compareKernel(expr.BinaryOpLte, left, right, mem)
	require.NoError(t, err)
	bo := out.(*array.Boolean)
	require.True(t, bo.Value(0))
	require.True(t, bo.IsNull(1))
	require.True(t, bo.Value(2))
	out.Release()

	_, err = bytesCompare(expr.BinaryOpAnd, left.(*array.Binary), right.(*array.Binary), mem)
	require.ErrorContains(t, err, "not supported")

	bl := array.NewBooleanBuilder(mem)
	bl.AppendValues([]bool{true, false}, nil)
	ba := bl.NewArray()
	bl.Release()
	defer ba.Release()
	out, err = compareKernel(expr.BinaryOpNeq, ba, ba, mem)
	require.NoError(t, err)
	require.False(t, out.(*array.Boolean).Value(0))
	out.Release()
	_, err = compareKernel(expr.BinaryOpLt, ba, ba, mem)
	require.ErrorContains(t, err, "not supported on bool")
}

func TestEvalLiteralVariantsAndErrors(t *testing.T) {
	mem := memory.DefaultAllocator
	for _, tc := range []struct {
		name string
		lit  expr.LiteralNode
		id   arrow.Type
	}{
		{"int8", expr.LiteralNode{Value: int8(1), Type: arrow.PrimitiveTypes.Int8}, arrow.INT8},
		{"uint64", expr.LiteralNode{Value: uint(2), Type: arrow.PrimitiveTypes.Uint64}, arrow.UINT64},
		{"float32", expr.LiteralNode{Value: float32(3), Type: arrow.PrimitiveTypes.Float32}, arrow.FLOAT32},
		{"string", expr.LiteralNode{Value: "x", Type: arrow.BinaryTypes.String}, arrow.STRING},
		{"bool", expr.LiteralNode{Value: true, Type: arrow.FixedWidthTypes.Boolean}, arrow.BOOL},
		{"timestamp", expr.LiteralNode{Value: arrow.Timestamp(10), Type: &arrow.TimestampType{Unit: arrow.Nanosecond}}, arrow.TIMESTAMP},
	} {
		t.Run(tc.name, func(t *testing.T) {
			arr, err := evalLiteral(tc.lit, 2, mem)
			require.NoError(t, err)
			defer arr.Release()
			require.Equal(t, tc.id, arr.DataType().ID())
			require.Equal(t, 2, arr.Len())
		})
	}

	_, err := evalLiteral(expr.LiteralNode{Value: 1}, 1, mem)
	require.ErrorContains(t, err, "no resolved type")
	_, err = evalLiteral(expr.LiteralNode{Value: "bad", Type: arrow.PrimitiveTypes.Int8}, 1, mem)
	require.ErrorContains(t, err, "expected int8")
	_, err = evalLiteral(expr.LiteralNode{Value: arrow.Date32(1), Type: arrow.FixedWidthTypes.Date32}, 1, mem)
	require.ErrorContains(t, err, "not supported")
	_, err = litTimestamp("bad")
	require.ErrorContains(t, err, "expected timestamp")
	_, err = litUint64(int64(1))
	require.ErrorContains(t, err, "expected uint64")
}

func TestSortIndicesMultiAndErrors(t *testing.T) {
	mem := memory.DefaultAllocator
	gb := array.NewStringBuilder(mem)
	gb.AppendValues([]string{"b", "a", "a", "b"}, nil)
	groupsArr := gb.NewArray()
	gb.Release()
	groups := arrow.NewChunked(arrow.BinaryTypes.String, []arrow.Array{groupsArr})
	groupsArr.Release()
	defer groups.Release()

	sb := array.NewInt64Builder(mem)
	sb.AppendValues([]int64{1, 1, 3, 2}, nil)
	scoresArr := sb.NewArray()
	sb.Release()
	scores := arrow.NewChunked(arrow.PrimitiveTypes.Int64, []arrow.Array{scoresArr})
	scoresArr.Release()
	defer scores.Release()

	idx, err := SortIndicesMulti([]SortKey{{Column: groups}, {Column: scores, Descending: true}})
	require.NoError(t, err)
	require.Equal(t, []int64{2, 1, 3, 0}, idx)

	_, err = SortIndicesMulti(nil)
	require.ErrorContains(t, err, "no keys")
	_, err = SortIndicesMulti([]SortKey{{Column: nil}})
	require.ErrorContains(t, err, "nil column")
	_, err = SortIndicesMulti([]SortKey{{Column: groups}, {Column: stringChunked(t, false, strp("x"))}})
	require.ErrorContains(t, err, "length")
	_, err = SortIndices(boolChunkedForSort(t), false, false)
	require.ErrorContains(t, err, "not supported")
}

func boolChunkedForSort(t *testing.T) *arrow.Chunked {
	t.Helper()
	b := array.NewBooleanBuilder(memory.DefaultAllocator)
	b.AppendValues([]bool{true, false}, nil)
	a := b.NewArray()
	b.Release()
	ch := arrow.NewChunked(arrow.FixedWidthTypes.Boolean, []arrow.Array{a})
	a.Release()
	t.Cleanup(ch.Release)
	return ch
}

func TestTakeAndFilterMoreBranches(t *testing.T) {
	mem := memory.DefaultAllocator
	_, err := Take(nil, []int64{0}, mem)
	require.ErrorContains(t, err, "nil column")

	strCh := stringChunked(t, false, strp("a"), nil, strp("b"))
	out, err := Take(strCh, []int64{2, 1, -1, 0}, nil)
	require.NoError(t, err)
	so := out.(*array.String)
	require.Equal(t, "b", so.Value(0))
	require.True(t, so.IsNull(1))
	require.True(t, so.IsNull(2))
	require.Equal(t, "a", so.Value(3))
	out.Release()
	strCh.Release()

	bb := array.NewBinaryBuilder(mem, arrow.BinaryTypes.Binary)
	bb.Append([]byte("x"))
	bin := bb.NewArray()
	bb.Release()
	binCh := arrow.NewChunked(arrow.BinaryTypes.Binary, []arrow.Array{bin})
	bin.Release()
	_, err = Take(binCh, []int64{0}, mem)
	require.ErrorContains(t, err, "not supported")
	binCh.Release()

	_, err = FilterRecord(nil, nil, mem)
	require.ErrorContains(t, err, "nil record")

	rec := int64Record(t, "a", []int64{1, 2})
	defer rec.Release()
	maskB := array.NewBooleanBuilder(mem)
	maskB.AppendValues([]bool{true}, nil)
	mask := maskB.NewArray()
	maskB.Release()
	_, err = FilterRecord(rec, mask, mem)
	require.ErrorContains(t, err, "mask length")
	mask.Release()

	binB := array.NewBinaryBuilder(mem, arrow.BinaryTypes.Binary)
	binB.Append([]byte("x"))
	binB.Append([]byte("y"))
	binArr := binB.NewArray()
	binB.Release()
	binRec := array.NewRecord(arrow.NewSchema([]arrow.Field{{Name: "b", Type: arrow.BinaryTypes.Binary}}, nil), []arrow.Array{binArr}, 2)
	binArr.Release()
	defer binRec.Release()
	maskB = array.NewBooleanBuilder(mem)
	maskB.AppendValues([]bool{true, false}, nil)
	mask = maskB.NewArray()
	maskB.Release()
	_, err = FilterRecord(binRec, mask, mem)
	require.ErrorContains(t, err, "not supported")
	mask.Release()
}

func TestTemporalArithAdditionalBranches(t *testing.T) {
	mem := memory.DefaultAllocator
	tsType := &arrow.TimestampType{Unit: arrow.Nanosecond, TimeZone: "UTC"}
	durType := &arrow.DurationType{Unit: arrow.Nanosecond}

	tsb := array.NewTimestampBuilder(mem, tsType)
	tsb.Append(arrow.Timestamp(10))
	tsb.AppendNull()
	ts := tsb.NewArray()
	tsb.Release()
	defer ts.Release()

	db := array.NewDurationBuilder(mem, durType)
	db.Append(arrow.Duration(3))
	db.Append(arrow.Duration(4))
	dur := db.NewArray()
	db.Release()
	defer dur.Release()

	out, err := temporalArith(expr.BinaryOpSub, ts, dur, mem)
	require.NoError(t, err)
	gotTS := out.(*array.Timestamp)
	require.Equal(t, arrow.Timestamp(7), gotTS.Value(0))
	require.True(t, gotTS.IsNull(1))
	out.Release()

	out, err = temporalArith(expr.BinaryOpAdd, dur, ts, mem)
	require.NoError(t, err)
	require.Equal(t, arrow.Timestamp(13), out.(*array.Timestamp).Value(0))
	out.Release()

	_, err = temporalArith(expr.BinaryOpAdd, ts, ts, mem)
	require.ErrorContains(t, err, "only subtraction")
	_, err = temporalArith(expr.BinaryOpSub, dur, ts, mem)
	require.ErrorContains(t, err, "only addition")
	require.False(t, isTemporalArith(arrow.PrimitiveTypes.Int64, arrow.PrimitiveTypes.Int64))
}

func TestPrimitiveDispatchMatrices(t *testing.T) {
	mem := memory.DefaultAllocator
	maskB := array.NewBooleanBuilder(mem)
	maskB.AppendValues([]bool{true, false, true}, nil)
	mask := maskB.NewArray().(*array.Boolean)
	maskB.Release()
	defer mask.Release()

	for _, tc := range primitiveCases(t) {
		t.Run(tc.name, func(t *testing.T) {
			ch := tc.chunked()
			defer ch.Release()

			ag, err := Reduce(ch)
			require.NoError(t, err)
			require.Equal(t, int64(3), ag.Count)

			groups, err := GroupReduce([]int{0, 1, 0}, 2, ch)
			require.NoError(t, err)
			require.Equal(t, int64(2), groups[0].Count)

			boxed, err := BoxedValues(ch)
			require.NoError(t, err)
			require.Len(t, boxed, 3)

			built, err := BuildArray(ch.DataType(), boxed, mem)
			require.NoError(t, err)
			require.Equal(t, 3, built.Len())
			built.Release()

			idx, err := SortIndices(ch, false, false)
			require.NoError(t, err)
			require.Len(t, idx, 3)

			taken, err := Take(ch, []int64{2, 0}, mem)
			require.NoError(t, err)
			require.Equal(t, 2, taken.Len())
			taken.Release()

			filtered, err := filterArray(ch.Chunks()[0], mask, mem)
			require.NoError(t, err)
			require.Equal(t, 2, filtered.Len())
			filtered.Release()

			cmpOut, err := compareKernel(expr.BinaryOpEq, ch.Chunks()[0], ch.Chunks()[0], mem)
			require.NoError(t, err)
			require.Equal(t, 3, cmpOut.Len())
			cmpOut.Release()
		})
	}
}

type primitiveCase struct {
	name    string
	chunked func() *arrow.Chunked
}

func primitiveCases(t *testing.T) []primitiveCase {
	t.Helper()
	mem := memory.DefaultAllocator
	return []primitiveCase{
		{"int8", func() *arrow.Chunked {
			b := array.NewInt8Builder(mem)
			b.AppendValues([]int8{2, 1, 3}, nil)
			return chunkFromBuilder(arrow.PrimitiveTypes.Int8, b)
		}},
		{"int16", func() *arrow.Chunked {
			b := array.NewInt16Builder(mem)
			b.AppendValues([]int16{2, 1, 3}, nil)
			return chunkFromBuilder(arrow.PrimitiveTypes.Int16, b)
		}},
		{"int32", func() *arrow.Chunked {
			b := array.NewInt32Builder(mem)
			b.AppendValues([]int32{2, 1, 3}, nil)
			return chunkFromBuilder(arrow.PrimitiveTypes.Int32, b)
		}},
		{"int64", func() *arrow.Chunked {
			b := array.NewInt64Builder(mem)
			b.AppendValues([]int64{2, 1, 3}, nil)
			return chunkFromBuilder(arrow.PrimitiveTypes.Int64, b)
		}},
		{"uint8", func() *arrow.Chunked {
			b := array.NewUint8Builder(mem)
			b.AppendValues([]uint8{2, 1, 3}, nil)
			return chunkFromBuilder(arrow.PrimitiveTypes.Uint8, b)
		}},
		{"uint16", func() *arrow.Chunked {
			b := array.NewUint16Builder(mem)
			b.AppendValues([]uint16{2, 1, 3}, nil)
			return chunkFromBuilder(arrow.PrimitiveTypes.Uint16, b)
		}},
		{"uint32", func() *arrow.Chunked {
			b := array.NewUint32Builder(mem)
			b.AppendValues([]uint32{2, 1, 3}, nil)
			return chunkFromBuilder(arrow.PrimitiveTypes.Uint32, b)
		}},
		{"uint64", func() *arrow.Chunked {
			b := array.NewUint64Builder(mem)
			b.AppendValues([]uint64{2, 1, 3}, nil)
			return chunkFromBuilder(arrow.PrimitiveTypes.Uint64, b)
		}},
		{"float32", func() *arrow.Chunked {
			b := array.NewFloat32Builder(mem)
			b.AppendValues([]float32{2, 1, 3}, nil)
			return chunkFromBuilder(arrow.PrimitiveTypes.Float32, b)
		}},
		{"float64", func() *arrow.Chunked {
			b := array.NewFloat64Builder(mem)
			b.AppendValues([]float64{2, 1, 3}, nil)
			return chunkFromBuilder(arrow.PrimitiveTypes.Float64, b)
		}},
		{"string", func() *arrow.Chunked {
			b := array.NewStringBuilder(mem)
			b.AppendValues([]string{"b", "a", "c"}, nil)
			return chunkFromBuilder(arrow.BinaryTypes.String, b)
		}},
	}
}

func chunkFromBuilder(dtype arrow.DataType, b interface {
	Release()
	NewArray() arrow.Array
}) *arrow.Chunked {
	a := b.NewArray()
	b.Release()
	ch := arrow.NewChunked(dtype, []arrow.Array{a})
	a.Release()
	return ch
}

func TestGroupReduceParallelTypeBranches(t *testing.T) {
	old := Parallelism()
	SetParallelism(3)
	t.Cleanup(func() { SetParallelism(old) })

	for _, tc := range []struct {
		name string
		ch   *arrow.Chunked
	}{
		{"uint64", primitiveCases(t)[7].chunked()},
		{"float64", primitiveCases(t)[9].chunked()},
		{"string", stringChunked(t, false, strp("b"), strp("a"), strp("c"))},
		{"bool", plainBoolChunked()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			defer tc.ch.Release()
			ag, err := GroupReduceParallel([]int{0, 1, 0}, 2, tc.ch)
			require.NoError(t, err)
			require.Equal(t, int64(2), ag[0].Count)
		})
	}

	_, err := GroupReduceParallel([]int{0}, 1, nil)
	require.ErrorContains(t, err, "nil column")
	ch := primitiveCases(t)[0].chunked()
	defer ch.Release()
	_, err = GroupReduceParallel([]int{0}, 1, ch)
	require.ErrorContains(t, err, "group ids")
}

func TestArithmeticUnaryCastAndLiteralCoverage(t *testing.T) {
	mem := memory.DefaultAllocator
	for _, tc := range primitiveCases(t)[:10] {
		t.Run("arith_"+tc.name, func(t *testing.T) {
			ch := tc.chunked()
			defer ch.Release()
			arr := ch.Chunks()[0]
			out, err := arithKernel(expr.BinaryOpAdd, arr, arr, mem)
			require.NoError(t, err)
			require.Equal(t, arr.DataType().ID(), out.DataType().ID())
			out.Release()
		})
	}

	ch := primitiveCases(t)[3].chunked()
	defer ch.Release()
	out, err := arithKernel(expr.BinaryOpDiv, ch.Chunks()[0], ch.Chunks()[0], mem)
	require.NoError(t, err)
	out.Release()
	_, err = arithKernel(expr.BinaryOpAnd, ch.Chunks()[0], ch.Chunks()[0], mem)
	require.ErrorContains(t, err, "not supported")
	_, err = arithKernel(expr.BinaryOpAdd, ch.Chunks()[0], primitiveCases(t)[0].chunked().Chunks()[0], mem)
	require.ErrorContains(t, err, "type mismatch")

	for _, tc := range []struct {
		name string
		ch   *arrow.Chunked
	}{
		{"int8", primitiveCases(t)[0].chunked()},
		{"int16", primitiveCases(t)[1].chunked()},
		{"int32", primitiveCases(t)[2].chunked()},
		{"int64", primitiveCases(t)[3].chunked()},
		{"float32", primitiveCases(t)[8].chunked()},
		{"float64", primitiveCases(t)[9].chunked()},
	} {
		t.Run("neg_"+tc.name, func(t *testing.T) {
			defer tc.ch.Release()
			out, err := negKernel(tc.ch.Chunks()[0], mem)
			require.NoError(t, err)
			out.Release()
		})
	}
	_, err = negKernel(primitiveCases(t)[4].chunked().Chunks()[0], mem)
	require.ErrorIs(t, err, errNoUnaryKernel)

	for _, tc := range []expr.LiteralNode{
		{Value: int16(1), Type: arrow.PrimitiveTypes.Int16},
		{Value: int32(1), Type: arrow.PrimitiveTypes.Int32},
		{Value: int64(1), Type: arrow.PrimitiveTypes.Int64},
		{Value: uint8(1), Type: arrow.PrimitiveTypes.Uint8},
		{Value: uint16(1), Type: arrow.PrimitiveTypes.Uint16},
		{Value: uint32(1), Type: arrow.PrimitiveTypes.Uint32},
		{Value: uint64(1), Type: arrow.PrimitiveTypes.Uint64},
		{Value: float64(1), Type: arrow.PrimitiveTypes.Float64},
	} {
		arr, err := evalLiteral(tc, 1, mem)
		require.NoError(t, err)
		arr.Release()
	}
	require.Equal(t, int64(1), mustLitInt64(t, int(1)))
	_, err = litInt64(uint64(1))
	require.ErrorContains(t, err, "expected int64")

	rec := int64Record(t, "a", []int64{1, 2})
	defer rec.Release()
	same, err := evalCast(expr.CastNode{Inner: expr.Col("a").Node, Type: arrow.PrimitiveTypes.Int64}, rec, mem)
	require.NoError(t, err)
	require.Equal(t, arrow.INT64, same.DataType().ID())
	same.Release()
	casted, err := evalCast(expr.CastNode{Inner: expr.Col("a").Node, Type: arrow.PrimitiveTypes.Float64}, rec, mem)
	require.NoError(t, err)
	require.Equal(t, arrow.FLOAT64, casted.DataType().ID())
	casted.Release()
	_, err = evalCast(expr.CastNode{Inner: expr.Col("a").Node}, rec, mem)
	require.ErrorContains(t, err, "cast target type is nil")
	_, err = evalCast(expr.CastNode{Inner: expr.Col("missing").Node, Type: arrow.PrimitiveTypes.Int64}, rec, mem)
	require.ErrorContains(t, err, "not in batch")
}

func mustLitInt64(t *testing.T, v any) int64 {
	t.Helper()
	out, err := litInt64(v)
	require.NoError(t, err)
	return out
}

func TestRemainingCompareAndTemporalBranches(t *testing.T) {
	mem := memory.DefaultAllocator
	for _, op := range []expr.BinaryOp{expr.BinaryOpEq, expr.BinaryOpNeq, expr.BinaryOpLt, expr.BinaryOpLte, expr.BinaryOpGt, expr.BinaryOpGte} {
		fn := bytesOp(op)
		require.NotNil(t, fn)
		_ = fn(0)
	}
	require.Nil(t, bytesOp(expr.BinaryOpAnd))

	b := array.NewBooleanBuilder(mem)
	b.AppendValues([]bool{true, false}, nil)
	ba := b.NewArray()
	b.Release()
	defer ba.Release()
	out, err := boolCompare(expr.BinaryOpEq, ba.(*array.Boolean), ba.(*array.Boolean), mem)
	require.NoError(t, err)
	out.Release()

	tsNano := &arrow.TimestampType{Unit: arrow.Nanosecond}
	tsMicro := &arrow.TimestampType{Unit: arrow.Microsecond}
	tsb := array.NewTimestampBuilder(mem, tsNano)
	tsb.Append(arrow.Timestamp(1))
	ts1 := tsb.NewArray()
	tsb.Release()
	defer ts1.Release()
	tsb = array.NewTimestampBuilder(mem, tsMicro)
	tsb.Append(arrow.Timestamp(1))
	ts2 := tsb.NewArray()
	tsb.Release()
	defer ts2.Release()
	_, err = temporalArith(expr.BinaryOpSub, ts1, ts2, mem)
	require.ErrorContains(t, err, "unit mismatch")

	db := array.NewDurationBuilder(mem, &arrow.DurationType{Unit: arrow.Microsecond})
	db.Append(arrow.Duration(1))
	dur := db.NewArray()
	db.Release()
	defer dur.Release()
	_, err = temporalArith(expr.BinaryOpAdd, ts1, dur, mem)
	require.ErrorContains(t, err, "unit mismatch")
	_, err = temporalArith(expr.BinaryOpAdd, dur, ts1, mem)
	require.ErrorContains(t, err, "unit mismatch")
	_, err = temporalArith(expr.BinaryOpAdd, primitiveCases(t)[0].chunked().Chunks()[0], primitiveCases(t)[1].chunked().Chunks()[0], mem)
	require.ErrorContains(t, err, "not defined")
}

func TestEvalAndEvalParallelBranches(t *testing.T) {
	mem := memory.DefaultAllocator
	rec1 := int64Record(t, "a", []int64{1, 2, 3})
	defer rec1.Release()
	rec2 := int64Record(t, "a", []int64{4, 5})
	defer rec2.Release()

	_, err := Eval(nil, rec1, mem)
	require.ErrorContains(t, err, "nil expression")
	_, err = Eval(expr.Col("a").Node, nil, mem)
	require.ErrorContains(t, err, "nil record")
	_, err = Eval(unsupportedExprNode{}, rec1, mem)
	require.ErrorContains(t, err, "unsupported node")
	arr, err := Eval(expr.Col("a").As("alias").Node, rec1, nil)
	require.NoError(t, err)
	arr.Release()
	_, err = Eval(expr.UnaryNode{Op: expr.UnaryOpInvalid, Inner: expr.Col("a").Node}, rec1, mem)
	require.ErrorContains(t, err, "unary op")
	_, err = Eval(expr.Col("a").Not().Node, rec1, mem)
	require.ErrorContains(t, err, "not requires bool")
	_, err = Eval(expr.BinaryNode{Op: expr.BinaryOpInvalid, Left: expr.Col("a").Node, Right: expr.Col("a").Node}, rec1, mem)
	require.ErrorContains(t, err, "binary op")

	old := Parallelism()
	SetParallelism(2)
	t.Cleanup(func() { SetParallelism(old) })

	out, err := EvalParallel(context.Background(), expr.Col("a").Gt(expr.Lit(int64(2))).Node, []arrow.Record{rec1, rec2}, nil)
	require.NoError(t, err)
	require.Len(t, out, 2)
	for _, r := range out {
		r.Release()
	}

	out, err = EvalParallel(context.Background(), expr.Col("missing").Gt(expr.Lit(int64(2))).Node, []arrow.Record{rec1}, mem)
	require.ErrorContains(t, err, "eval")
	require.Nil(t, out)
	out, err = EvalParallel(context.Background(), expr.Col("a").Node, []arrow.Record{rec1}, mem)
	require.ErrorContains(t, err, "filter")
	require.Nil(t, out)
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	out, err = EvalParallel(canceled, expr.Col("a").Gt(expr.Lit(int64(2))).Node, []arrow.Record{rec1}, mem)
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, out)
	out, err = EvalParallel(context.Background(), expr.Col("a").Node, nil, mem)
	require.NoError(t, err)
	require.Nil(t, out)
}

type unsupportedExprNode struct{}

func (unsupportedExprNode) String() string            { return "unsupported" }
func (unsupportedExprNode) Children() []expr.ExprNode { return nil }
func (unsupportedExprNode) DataType(*schema.Schema) (arrow.DataType, error) {
	return arrow.PrimitiveTypes.Int64, nil
}

func plainBoolChunked() *arrow.Chunked {
	b := array.NewBooleanBuilder(memory.DefaultAllocator)
	b.AppendValues([]bool{true, false, true}, nil)
	a := b.NewArray()
	b.Release()
	ch := arrow.NewChunked(arrow.FixedWidthTypes.Boolean, []arrow.Array{a})
	a.Release()
	return ch
}
