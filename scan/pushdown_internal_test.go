package scan

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/karthedew/cosma/carray"
	"github.com/karthedew/cosma/expr"
)

// pushdown_internal_test.go covers the hint-translation helpers directly:
// rangePredicate (each operator, both operand orders), asInt64 (literal types
// and fractional-float rejection), intersectAxisRange (All / Slice step1 /
// Slice stepN / Index / Indices), splitConjuncts, columnLiteralOperands, and
// cloneSelection. The end-to-end pruning behaviour is already covered by
// array_test.go; here we pin the unit contracts.

func ip(v int64) *int64 { return &v }

func TestAsInt64(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		v    any
		want int64
		ok   bool
	}{
		{"int", int(5), 5, true},
		{"int8", int8(-1), -1, true},
		{"int16", int16(300), 300, true},
		{"int32", int32(-7), -7, true},
		{"int64", int64(1 << 40), 1 << 40, true},
		{"uint", uint(9), 9, true},
		{"uint8", uint8(255), 255, true},
		{"uint16", uint16(65535), 65535, true},
		{"uint32", uint32(4000000000), 4000000000, true},
		{"uint64", uint64(1 << 40), 1 << 40, true},
		{"integral-float64", float64(23), 23, true},
		{"integral-float32", float32(12), 12, true},
		{"fractional-float64", float64(23.5), 0, false},
		{"fractional-float32", float32(0.25), 0, false},
		{"string", "12", 0, false},
		{"bool", true, 0, false},
		{"nil", nil, 0, false},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, ok := asInt64(tc.v)
			assert.Equal(t, tc.ok, ok)
			if tc.ok {
				assert.Equal(t, tc.want, got)
			}
		})
	}
}

func TestFlipOp(t *testing.T) {
	t.Parallel()
	cases := []struct{ in, want expr.BinaryOp }{
		{expr.BinaryOpLt, expr.BinaryOpGt},
		{expr.BinaryOpLte, expr.BinaryOpGte},
		{expr.BinaryOpGt, expr.BinaryOpLt},
		{expr.BinaryOpGte, expr.BinaryOpLte},
		{expr.BinaryOpEq, expr.BinaryOpEq}, // symmetric
	}
	for _, tc := range cases {
		assert.Equal(t, tc.want, flipOp(tc.in))
	}
}

func TestColumnLiteralOperands(t *testing.T) {
	t.Parallel()

	col := expr.ColumnNode{Name: "time"}
	lit := expr.LiteralNode{Value: int64(5)}

	t.Run("col-left", func(t *testing.T) {
		t.Parallel()
		name, v, swapped, found := columnLiteralOperands(col, lit)
		assert.True(t, found)
		assert.False(t, swapped)
		assert.Equal(t, "time", name)
		assert.Equal(t, int64(5), v)
	})
	t.Run("lit-left", func(t *testing.T) {
		t.Parallel()
		name, v, swapped, found := columnLiteralOperands(lit, col)
		assert.True(t, found)
		assert.True(t, swapped)
		assert.Equal(t, "time", name)
		assert.Equal(t, int64(5), v)
	})
	t.Run("col-col", func(t *testing.T) {
		t.Parallel()
		_, _, _, found := columnLiteralOperands(col, expr.ColumnNode{Name: "x"})
		assert.False(t, found)
	})
	t.Run("lit-lit", func(t *testing.T) {
		t.Parallel()
		_, _, _, found := columnLiteralOperands(lit, expr.LiteralNode{Value: int64(1)})
		assert.False(t, found)
	})
}

func TestRangePredicate(t *testing.T) {
	t.Parallel()

	const big = maxInt64Range
	const small = minInt64Range

	// helper to build `col <op> lit` and `lit <op> col`.
	colLit := func(op expr.BinaryOp, v any) expr.ExprNode {
		return expr.BinaryNode{Op: op, Left: expr.ColumnNode{Name: "time"}, Right: expr.LiteralNode{Value: v}}
	}
	litCol := func(op expr.BinaryOp, v any) expr.ExprNode {
		return expr.BinaryNode{Op: op, Left: expr.LiteralNode{Value: v}, Right: expr.ColumnNode{Name: "time"}}
	}

	cases := []struct {
		name   string
		node   expr.ExprNode
		wantOK bool
		lo, hi int64
	}{
		{"eq", colLit(expr.BinaryOpEq, int64(5)), true, 5, 6},
		{"lt", colLit(expr.BinaryOpLt, int64(5)), true, small, 5},
		{"lte", colLit(expr.BinaryOpLte, int64(5)), true, small, 6},
		{"gt", colLit(expr.BinaryOpGt, int64(5)), true, 6, big},
		{"gte", colLit(expr.BinaryOpGte, int64(5)), true, 5, big},

		// Swapped operands flip the operator: 5 < time  <=>  time > 5.
		{"swapped-lt", litCol(expr.BinaryOpLt, int64(5)), true, 6, big},
		{"swapped-gt", litCol(expr.BinaryOpGt, int64(5)), true, small, 5},
		{"swapped-gte", litCol(expr.BinaryOpGte, int64(5)), true, small, 6},
		{"swapped-lte", litCol(expr.BinaryOpLte, int64(5)), true, 5, big},
		{"swapped-eq", litCol(expr.BinaryOpEq, int64(5)), true, 5, 6},

		// Integral float literal is accepted.
		{"integral-float", colLit(expr.BinaryOpLte, float64(23.0)), true, small, 24},

		// Rejected shapes.
		{"fractional-float", colLit(expr.BinaryOpLt, float64(2.5)), false, 0, 0},
		{"unsupported-op", colLit(expr.BinaryOpAnd, int64(1)), false, 0, 0},
		{"not-binary", expr.ColumnNode{Name: "time"}, false, 0, 0},
		{"col-col", expr.BinaryNode{Op: expr.BinaryOpLt, Left: expr.ColumnNode{Name: "a"}, Right: expr.ColumnNode{Name: "b"}}, false, 0, 0},
		{"string-literal", colLit(expr.BinaryOpEq, "x"), false, 0, 0},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			col, lo, hi, ok := rangePredicate(tc.node)
			require.Equal(t, tc.wantOK, ok)
			if !tc.wantOK {
				return
			}
			assert.Equal(t, "time", col)
			assert.Equal(t, tc.lo, lo)
			assert.Equal(t, tc.hi, hi)
		})
	}
}

func TestSplitConjuncts(t *testing.T) {
	t.Parallel()

	t.Run("nil", func(t *testing.T) {
		t.Parallel()
		assert.Nil(t, splitConjuncts(expr.Expr{}))
	})

	t.Run("single", func(t *testing.T) {
		t.Parallel()
		e := expr.Col("a").Lt(expr.Lit(int64(1)))
		got := splitConjuncts(e)
		require.Len(t, got, 1)
		assert.Equal(t, e.Node, got[0].Node)
	})

	t.Run("flattens-nested-and", func(t *testing.T) {
		t.Parallel()
		// (a<1 AND b>2) AND c==3  -> three conjuncts, left-to-right order.
		e := expr.Col("a").Lt(expr.Lit(int64(1))).
			And(expr.Col("b").Gt(expr.Lit(int64(2)))).
			And(expr.Col("c").Eq(expr.Lit(int64(3))))
		got := splitConjuncts(e)
		require.Len(t, got, 3)
		assert.Equal(t, "(a < 1)", got[0].String())
		assert.Equal(t, "(b > 2)", got[1].String())
		assert.Equal(t, "(c == 3)", got[2].String())
	})

	t.Run("or-not-split", func(t *testing.T) {
		t.Parallel()
		// An OR is a single opaque conjunct (only AND flattens).
		e := expr.Col("a").Lt(expr.Lit(int64(1))).Or(expr.Col("b").Gt(expr.Lit(int64(2))))
		got := splitConjuncts(e)
		require.Len(t, got, 1)
	})
}

func TestSplitConjunctsAll(t *testing.T) {
	t.Parallel()
	filters := []expr.Expr{
		expr.Col("a").Lt(expr.Lit(int64(1))).And(expr.Col("b").Gt(expr.Lit(int64(2)))),
		expr.Col("c").Eq(expr.Lit(int64(3))),
	}
	got := splitConjunctsAll(filters)
	require.Len(t, got, 3)
}

func TestIntersectAxisRange(t *testing.T) {
	t.Parallel()

	t.Run("all-tightens-to-clamped-range", func(t *testing.T) {
		t.Parallel()
		got := intersectAxisRange(carray.AxisSel{All: true}, 100, 5, 20)
		require.NotNil(t, got.Slice)
		assert.Equal(t, carray.Slice{Start: 5, Stop: 20, Step: 1}, *got.Slice)
	})

	t.Run("zero-value-tightens", func(t *testing.T) {
		t.Parallel()
		got := intersectAxisRange(carray.AxisSel{}, 100, 0, 24)
		require.NotNil(t, got.Slice)
		assert.Equal(t, carray.Slice{Start: 0, Stop: 24, Step: 1}, *got.Slice)
	})

	t.Run("all-clamps-to-axis-extent", func(t *testing.T) {
		t.Parallel()
		// lo below 0 and hi above size are clamped to [0, size).
		got := intersectAxisRange(carray.AxisSel{All: true}, 10, minInt64Range, maxInt64Range)
		require.NotNil(t, got.Slice)
		assert.Equal(t, carray.Slice{Start: 0, Stop: 10, Step: 1}, *got.Slice)
	})

	t.Run("all-empty-range-lo-gt-hi", func(t *testing.T) {
		t.Parallel()
		// A predicate keeping nothing (gt then lt) collapses to an empty slice.
		got := intersectAxisRange(carray.AxisSel{All: true}, 10, 8, 3)
		require.NotNil(t, got.Slice)
		assert.Equal(t, carray.Slice{Start: 3, Stop: 3, Step: 1}, *got.Slice)
	})

	t.Run("slice-step1-intersected", func(t *testing.T) {
		t.Parallel()
		in := carray.AxisSel{Slice: &carray.Slice{Start: 2, Stop: 50, Step: 1}}
		got := intersectAxisRange(in, 100, 10, 30)
		require.NotNil(t, got.Slice)
		assert.Equal(t, carray.Slice{Start: 10, Stop: 30, Step: 1}, *got.Slice)
	})

	t.Run("slice-step1-no-overlap-clamps-stop-to-start", func(t *testing.T) {
		t.Parallel()
		in := carray.AxisSel{Slice: &carray.Slice{Start: 60, Stop: 90, Step: 1}}
		got := intersectAxisRange(in, 100, 10, 30)
		require.NotNil(t, got.Slice)
		// keep-range is entirely below the slice: stop clamps down to start.
		assert.Equal(t, carray.Slice{Start: 60, Stop: 60, Step: 1}, *got.Slice)
	})

	t.Run("slice-stepN-unchanged", func(t *testing.T) {
		t.Parallel()
		in := carray.AxisSel{Slice: &carray.Slice{Start: 0, Stop: 50, Step: 2}}
		got := intersectAxisRange(in, 100, 10, 30)
		require.NotNil(t, got.Slice)
		assert.Equal(t, *in.Slice, *got.Slice) // conservative: strided slices untouched
	})

	t.Run("index-unchanged", func(t *testing.T) {
		t.Parallel()
		in := carray.AxisSel{Index: ip(7)}
		got := intersectAxisRange(in, 100, 0, 3)
		require.NotNil(t, got.Index)
		assert.Equal(t, int64(7), *got.Index)
	})

	t.Run("indices-filtered-to-range", func(t *testing.T) {
		t.Parallel()
		in := carray.AxisSel{Indices: []int64{1, 5, 9, 15, 22}}
		got := intersectAxisRange(in, 100, 5, 16)
		assert.Equal(t, []int64{5, 9, 15}, got.Indices)
	})

	t.Run("indices-none-survive", func(t *testing.T) {
		t.Parallel()
		in := carray.AxisSel{Indices: []int64{1, 2, 3}}
		got := intersectAxisRange(in, 100, 50, 60)
		assert.Empty(t, got.Indices)
	})
}

func TestCloneSelectionDeepCopy(t *testing.T) {
	t.Parallel()

	orig := carray.Selection{Axes: []carray.AxisSel{
		{Index: ip(3)},
		{Slice: &carray.Slice{Start: 1, Stop: 4, Step: 1}},
		{Indices: []int64{0, 2}},
	}}
	shape := []int64{10, 10, 10}

	cl := cloneSelection(orig, shape)
	require.Len(t, cl.Axes, 3)

	// Mutating the clone must not touch the original (deep copy of each field).
	*cl.Axes[0].Index = 99
	cl.Axes[1].Slice.Start = 7
	cl.Axes[2].Indices[0] = 42

	assert.Equal(t, int64(3), *orig.Axes[0].Index)
	assert.Equal(t, int64(1), orig.Axes[1].Slice.Start)
	assert.Equal(t, int64(0), orig.Axes[2].Indices[0])
}

func TestCloneSelectionMaterializesShortAxes(t *testing.T) {
	t.Parallel()
	// A nil/short Axes slice is materialized to one all-selector per axis.
	cl := cloneSelection(carray.Selection{}, []int64{5, 6, 7})
	require.Len(t, cl.Axes, 3)
	for _, a := range cl.Axes {
		assert.Nil(t, a.Index)
		assert.Nil(t, a.Slice)
		assert.Nil(t, a.Indices)
	}
}

func TestApplyPushdownProjectionAndRange(t *testing.T) {
	t.Parallel()

	// 2-D array, both axes kept (cols time=axis0, station=axis1).
	cols := []string{"time", "station"}
	axisOfCol := []int{0, 1}
	shape := []int64{100, 50}
	base := carray.Selection{}

	t.Run("range-on-kept-axis-tightens", func(t *testing.T) {
		t.Parallel()
		hints := dataframeScanHints{
			Filters: []expr.Expr{expr.Col("time").Lt(expr.Lit(int64(24)))},
			Limit:   -1,
		}
		res := applyPushdown(hints, base, cols, axisOfCol, shape, nil)
		require.Len(t, res.sel.Axes, 2)
		require.NotNil(t, res.sel.Axes[0].Slice)
		assert.Equal(t, carray.Slice{Start: 0, Stop: 24, Step: 1}, *res.sel.Axes[0].Slice)
		// axis 1 untouched.
		assert.Nil(t, res.sel.Axes[1].Slice)
		assert.Equal(t, []bool{true, true}, res.keepOutput)
		assert.Equal(t, int64(-1), res.limit)
	})

	t.Run("projection-drops-unreferenced-cols", func(t *testing.T) {
		t.Parallel()
		hints := dataframeScanHints{Columns: []string{"station"}, Limit: 7}
		res := applyPushdown(hints, base, cols, axisOfCol, shape, nil)
		assert.Equal(t, []bool{false, true}, res.keepOutput)
		assert.Equal(t, int64(7), res.limit)
	})

	t.Run("predicate-on-unknown-col-ignored", func(t *testing.T) {
		t.Parallel()
		hints := dataframeScanHints{
			Filters: []expr.Expr{expr.Col("value").Gt(expr.Lit(int64(0)))},
			Limit:   -1,
		}
		res := applyPushdown(hints, base, cols, axisOfCol, shape, nil)
		// No axis tightened.
		assert.Nil(t, res.sel.Axes[0].Slice)
		assert.Nil(t, res.sel.Axes[1].Slice)
	})

	t.Run("does-not-mutate-base", func(t *testing.T) {
		t.Parallel()
		base2 := carray.Selection{Axes: []carray.AxisSel{{All: true}, {All: true}}}
		hints := dataframeScanHints{
			Filters: []expr.Expr{expr.Col("time").Lte(expr.Lit(int64(10)))},
			Limit:   -1,
		}
		_ = applyPushdown(hints, base2, cols, axisOfCol, shape, nil)
		// base2's axis 0 still the original All selector.
		assert.Nil(t, base2.Axes[0].Slice)
		assert.True(t, base2.Axes[0].All)
	})
}
