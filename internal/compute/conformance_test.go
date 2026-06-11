package compute

import (
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"
	"github.com/apache/arrow/go/v18/arrow/memory"
	"github.com/stretchr/testify/require"

	"github.com/karthedew/cosma/expr"
)

// noNotImplemented asserts that an error, if present, is a clean rejection and
// never a runtime "not yet implemented" / "not implemented" placeholder. Phase 2
// exit criteria: every constructible expression either evaluates or fails with a
// real bind/type error.
func noNotImplemented(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		return
	}
	msg := strings.ToLower(err.Error())
	require.NotContains(t, msg, "not yet implemented", "got placeholder error: %v", err)
	require.NotContains(t, msg, "not implemented", "got placeholder error: %v", err)
}

var allComparisons = []struct {
	name string
	op   func(a, b expr.Expr) expr.Expr
}{
	{"eq", func(a, b expr.Expr) expr.Expr { return a.Eq(b) }},
	{"neq", func(a, b expr.Expr) expr.Expr { return a.Neq(b) }},
	{"lt", func(a, b expr.Expr) expr.Expr { return a.Lt(b) }},
	{"lte", func(a, b expr.Expr) expr.Expr { return a.Lte(b) }},
	{"gt", func(a, b expr.Expr) expr.Expr { return a.Gt(b) }},
	{"gte", func(a, b expr.Expr) expr.Expr { return a.Gte(b) }},
}

func TestConformance_Comparisons(t *testing.T) {
	t.Parallel()
	mem := memory.DefaultAllocator

	cases := []struct {
		name  string
		rec   func(*testing.T) arrow.Record
		other expr.Expr
	}{
		{
			name:  "int64",
			rec:   func(t *testing.T) arrow.Record { return int64Record(t, "c", []int64{1, 2, 3}) },
			other: expr.Lit(int64(2)),
		},
		{
			name: "float64",
			rec: func(t *testing.T) arrow.Record {
				b := array.NewFloat64Builder(mem)
				defer b.Release()
				b.AppendValues([]float64{1, 2, 3}, nil)
				a := b.NewArray()
				defer a.Release()
				s := arrow.NewSchema([]arrow.Field{{Name: "c", Type: arrow.PrimitiveTypes.Float64}}, nil)
				return array.NewRecord(s, []arrow.Array{a}, 3)
			},
			other: expr.Lit(float64(2)),
		},
		{
			name: "string",
			rec: func(t *testing.T) arrow.Record {
				b := array.NewStringBuilder(mem)
				defer b.Release()
				b.AppendValues([]string{"a", "b", "c"}, nil)
				a := b.NewArray()
				defer a.Release()
				s := arrow.NewSchema([]arrow.Field{{Name: "c", Type: arrow.BinaryTypes.String}}, nil)
				return array.NewRecord(s, []arrow.Array{a}, 3)
			},
			other: expr.Lit("b"),
		},
		{
			name: "timestamp",
			rec: func(t *testing.T) arrow.Record {
				ts := &arrow.TimestampType{Unit: arrow.Nanosecond, TimeZone: "UTC"}
				b := array.NewTimestampBuilder(mem, ts)
				defer b.Release()
				base := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
				b.AppendValues([]arrow.Timestamp{
					arrow.Timestamp(base.UnixNano()),
					arrow.Timestamp(base.Add(time.Hour).UnixNano()),
				}, nil)
				a := b.NewArray()
				defer a.Release()
				s := arrow.NewSchema([]arrow.Field{{Name: "c", Type: ts}}, nil)
				return array.NewRecord(s, []arrow.Array{a}, 2)
			},
			other: expr.LitTimestamp(time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC), "UTC"),
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			for _, cmp := range allComparisons {
				rec := tc.rec(t)
				e := cmp.op(expr.Col("c"), tc.other)
				out, err := Eval(e.Node, rec, mem)
				noNotImplemented(t, err)
				require.NoError(t, err, "%s %s", tc.name, cmp.name)
				_, ok := out.(*array.Boolean)
				require.True(t, ok, "%s %s result type %T", tc.name, cmp.name, out)
				out.Release()
				rec.Release()
			}
		})
	}
}

func TestConformance_LogicalKleene(t *testing.T) {
	t.Parallel()
	// a,b cover the full null truth space.
	rec := boolRecordTwo(t,
		[]bool{true, false, false, true, false},
		[]bool{true, true, false, true, false}, // last two rows null on a
		[]bool{true, false, true, false, true},
		[]bool{true, true, false, false, true}, // some null on b
	)
	defer rec.Release()

	for _, e := range []expr.Expr{
		expr.Col("a").And(expr.Col("b")),
		expr.Col("a").Or(expr.Col("b")),
		expr.Col("a").Not(),
	} {
		out, err := Eval(e.Node, rec, memory.DefaultAllocator)
		noNotImplemented(t, err)
		require.NoError(t, err)
		_, ok := out.(*array.Boolean)
		require.True(t, ok)
		out.Release()
	}
}

func TestConformance_NullTests(t *testing.T) {
	t.Parallel()
	mem := memory.DefaultAllocator
	b := array.NewInt64Builder(mem)
	defer b.Release()
	b.AppendValues([]int64{1, 0, 3}, []bool{true, false, true})
	a := b.NewArray()
	defer a.Release()
	s := arrow.NewSchema([]arrow.Field{{Name: "x", Type: arrow.PrimitiveTypes.Int64}}, nil)
	rec := array.NewRecord(s, []arrow.Array{a}, 3)
	defer rec.Release()

	for _, e := range []expr.Expr{expr.Col("x").IsNull(), expr.Col("x").IsNotNull()} {
		out, err := Eval(e.Node, rec, mem)
		noNotImplemented(t, err)
		require.NoError(t, err)
		res := out.(*array.Boolean)
		require.Equal(t, 0, res.NullN(), "null tests never produce nulls")
		out.Release()
	}
}

func TestConformance_Cast(t *testing.T) {
	t.Parallel()
	rec := int64Record(t, "x", []int64{1, 2, 3})
	defer rec.Release()
	out, err := Eval(expr.Col("x").Cast(arrow.PrimitiveTypes.Float64).Node, rec, memory.DefaultAllocator)
	noNotImplemented(t, err)
	require.NoError(t, err)
	require.Equal(t, arrow.FLOAT64, out.DataType().ID())
	out.Release()
}

func TestConformance_Aggregations(t *testing.T) {
	t.Parallel()

	// Numeric column: Sum/Count/Mean/Min/Max all defined.
	numChunked := chunkedInt64(t, []int64{10, 20, 30, 40})
	groups := []int{0, 1, 0, 1}
	num, err := GroupReduce(groups, 2, numChunked)
	noNotImplemented(t, err)
	require.NoError(t, err)
	require.EqualValues(t, 40, num[0].Sum)
	require.EqualValues(t, 10, num[0].Min)
	require.EqualValues(t, 30, num[0].Max)
	require.EqualValues(t, 2, num[0].Count)
	require.EqualValues(t, 20, num[0].Mean)

	// String column: Count + lexicographic Min/Max defined; Sum/Mean undefined.
	strChunked := chunkedString(t, []string{"amy", "zoe", "bob", "ann"})
	str, err := GroupReduce(groups, 2, strChunked)
	noNotImplemented(t, err)
	require.NoError(t, err)
	require.EqualValues(t, 2, str[0].Count)
	require.Equal(t, "amy", str[0].Min)
	require.Equal(t, "bob", str[0].Max)
	require.Nil(t, str[0].Sum, "string sum is undefined")
}

func chunkedInt64(t *testing.T, vals []int64) *arrow.Chunked {
	t.Helper()
	b := array.NewInt64Builder(memory.DefaultAllocator)
	defer b.Release()
	b.AppendValues(vals, nil)
	a := b.NewArray()
	defer a.Release()
	return arrow.NewChunked(arrow.PrimitiveTypes.Int64, []arrow.Array{a})
}

func chunkedString(t *testing.T, vals []string) *arrow.Chunked {
	t.Helper()
	b := array.NewStringBuilder(memory.DefaultAllocator)
	defer b.Release()
	b.AppendValues(vals, nil)
	a := b.NewArray()
	defer a.Release()
	return arrow.NewChunked(arrow.BinaryTypes.String, []arrow.Array{a})
}
