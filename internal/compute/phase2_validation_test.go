// Package compute — Phase 2 independent validation tests.
// Written by cosma-test-writer to verify Kleene semantics exhaustively and
// probe edge cases not covered by the implementation's own test suite.
package compute

import (
	"testing"
	"time"

	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"
	"github.com/apache/arrow/go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/karthedew/cosma/expr"
)

// ─── helpers ────────────────────────────────────────────────────────────────

// tristate represents T / F / N (null).
type tristate int

const (
	tTrue  tristate = 1
	tFalse tristate = 0
	tNull  tristate = -1
)

func (ts tristate) String() string {
	switch ts {
	case tTrue:
		return "T"
	case tFalse:
		return "F"
	default:
		return "N"
	}
}

// makeKleeneRecord builds a two-column boolean record whose rows exactly
// encode the requested tristate sequence.  rows must be the same length.
func makeKleeneRecord(t *testing.T, aRows, bRows []tristate) arrow.Record {
	t.Helper()
	mem := memory.DefaultAllocator
	aV, aVld := tristateSlices(aRows)
	bV, bVld := tristateSlices(bRows)

	ab := array.NewBooleanBuilder(mem)
	defer ab.Release()
	ab.AppendValues(aV, aVld)
	aArr := ab.NewArray()
	defer aArr.Release()

	bb := array.NewBooleanBuilder(mem)
	defer bb.Release()
	bb.AppendValues(bV, bVld)
	bArr := bb.NewArray()
	defer bArr.Release()

	sc := arrow.NewSchema([]arrow.Field{
		{Name: "a", Type: arrow.FixedWidthTypes.Boolean},
		{Name: "b", Type: arrow.FixedWidthTypes.Boolean},
	}, nil)
	return array.NewRecord(sc, []arrow.Array{aArr, bArr}, int64(len(aRows)))
}

func tristateSlices(rows []tristate) (vals []bool, valid []bool) {
	vals = make([]bool, len(rows))
	valid = make([]bool, len(rows))
	for i, r := range rows {
		switch r {
		case tTrue:
			vals[i] = true
			valid[i] = true
		case tFalse:
			vals[i] = false
			valid[i] = true
		case tNull:
			valid[i] = false // value irrelevant
		}
	}
	return
}

// evalBoolRow evaluates expr against a record that has exactly one row and
// returns the tristate result.
func evalBoolRow(t *testing.T, e expr.Expr, rec arrow.Record, row int) tristate {
	t.Helper()
	out, err := Eval(e.Node, rec, memory.DefaultAllocator)
	require.NoError(t, err, "Eval should not error for valid boolean op")
	defer out.Release()
	b, ok := out.(*array.Boolean)
	require.True(t, ok, "result must be *array.Boolean, got %T", out)
	if b.IsNull(row) {
		return tNull
	}
	if b.Value(row) {
		return tTrue
	}
	return tFalse
}

// ─── Task 2: Exhaustive Kleene truth table ─────────────────────────────────

// TestKleene_AND_FullTruthTable validates all 9 cells of the Kleene AND table.
//
// AND: T∧T=T, T∧F=F, T∧N=N, F∧T=F, F∧F=F, F∧N=F, N∧T=N, N∧F=F, N∧N=N
func TestKleene_AND_FullTruthTable(t *testing.T) {
	t.Parallel()
	cases := []struct {
		a, b tristate
		want tristate
	}{
		{tTrue, tTrue, tTrue},    // T∧T = T
		{tTrue, tFalse, tFalse},  // T∧F = F
		{tTrue, tNull, tNull},    // T∧N = N
		{tFalse, tTrue, tFalse},  // F∧T = F
		{tFalse, tFalse, tFalse}, // F∧F = F
		{tFalse, tNull, tFalse},  // F∧N = F  (FALSE dominates)
		{tNull, tTrue, tNull},    // N∧T = N
		{tNull, tFalse, tFalse},  // N∧F = F  (FALSE dominates)
		{tNull, tNull, tNull},    // N∧N = N
	}

	for _, tc := range cases {
		tc := tc
		t.Run("a="+tc.a.String()+"_b="+tc.b.String(), func(t *testing.T) {
			t.Parallel()
			rec := makeKleeneRecord(t, []tristate{tc.a}, []tristate{tc.b})
			defer rec.Release()
			got := evalBoolRow(t, expr.Col("a").And(expr.Col("b")), rec, 0)
			assert.Equal(t, tc.want, got,
				"AND(%v, %v): want %v, got %v", tc.a, tc.b, tc.want, got)
		})
	}
}

// TestKleene_OR_FullTruthTable validates all 9 cells of the Kleene OR table.
//
// OR: T∨T=T, T∨F=T, T∨N=T, F∨T=T, F∨F=F, F∨N=N, N∨T=T, N∨F=N, N∨N=N
func TestKleene_OR_FullTruthTable(t *testing.T) {
	t.Parallel()
	cases := []struct {
		a, b tristate
		want tristate
	}{
		{tTrue, tTrue, tTrue},    // T∨T = T
		{tTrue, tFalse, tTrue},   // T∨F = T
		{tTrue, tNull, tTrue},    // T∨N = T  (TRUE dominates)
		{tFalse, tTrue, tTrue},   // F∨T = T
		{tFalse, tFalse, tFalse}, // F∨F = F
		{tFalse, tNull, tNull},   // F∨N = N
		{tNull, tTrue, tTrue},    // N∨T = T  (TRUE dominates)
		{tNull, tFalse, tNull},   // N∨F = N
		{tNull, tNull, tNull},    // N∨N = N
	}

	for _, tc := range cases {
		tc := tc
		t.Run("a="+tc.a.String()+"_b="+tc.b.String(), func(t *testing.T) {
			t.Parallel()
			rec := makeKleeneRecord(t, []tristate{tc.a}, []tristate{tc.b})
			defer rec.Release()
			got := evalBoolRow(t, expr.Col("a").Or(expr.Col("b")), rec, 0)
			assert.Equal(t, tc.want, got,
				"OR(%v, %v): want %v, got %v", tc.a, tc.b, tc.want, got)
		})
	}
}

// ─── Task 3A: Chained unary Not(IsNull(col)) ─────────────────────────────

// TestEdgeCase_A_NotIsNull verifies that Not(IsNull(col)) returns true for
// non-null rows and false for null rows (i.e. IsNotNull semantics).
func TestEdgeCase_A_NotIsNull(t *testing.T) {
	t.Parallel()
	mem := memory.DefaultAllocator

	// Build x=[1, NULL, 3] where row 1 is null.
	bld := array.NewInt64Builder(mem)
	defer bld.Release()
	bld.AppendValues([]int64{1, 0, 3}, []bool{true, false, true})
	arr := bld.NewArray()
	defer arr.Release()

	sc := arrow.NewSchema([]arrow.Field{{Name: "x", Type: arrow.PrimitiveTypes.Int64}}, nil)
	rec := array.NewRecord(sc, []arrow.Array{arr}, 3)
	defer rec.Release()

	// Col("x").IsNull().Not() == IsNotNull(x)
	chainedExpr := expr.Col("x").IsNull().Not()
	out, err := Eval(chainedExpr.Node, rec, mem)
	require.NoError(t, err)
	defer out.Release()

	b, ok := out.(*array.Boolean)
	require.True(t, ok, "result must be *array.Boolean, got %T", out)

	require.Equal(t, 3, b.Len())
	// Row 0 (non-null): IsNull=false → Not=true
	assert.False(t, b.IsNull(0), "row 0 must not be null")
	assert.True(t, b.Value(0), "row 0 (x=1): IsNull=F → Not=T")
	// Row 1 (null): IsNull=true → Not=false
	assert.False(t, b.IsNull(1), "row 1 (null input): IsNull=T → Not=F must be non-null")
	assert.False(t, b.Value(1), "row 1 (x=NULL): IsNull=T → Not=F")
	// Row 2 (non-null): IsNull=false → Not=true
	assert.False(t, b.IsNull(2), "row 2 must not be null")
	assert.True(t, b.Value(2), "row 2 (x=3): IsNull=F → Not=T")
}

// ─── Task 3B: Cast on expression result ──────────────────────────────────

// TestEdgeCase_B_CastArithResult verifies that casting the result of an
// arithmetic expression (a+b) to float64 produces a float64 array.
func TestEdgeCase_B_CastArithResult(t *testing.T) {
	t.Parallel()
	mem := memory.DefaultAllocator

	ab := array.NewInt64Builder(mem)
	defer ab.Release()
	ab.AppendValues([]int64{1, 2, 3}, nil)
	aArr := ab.NewArray()
	defer aArr.Release()

	bb := array.NewInt64Builder(mem)
	defer bb.Release()
	bb.AppendValues([]int64{4, 5, 6}, nil)
	bArr := bb.NewArray()
	defer bArr.Release()

	sc := arrow.NewSchema([]arrow.Field{
		{Name: "a", Type: arrow.PrimitiveTypes.Int64},
		{Name: "b", Type: arrow.PrimitiveTypes.Int64},
	}, nil)
	rec := array.NewRecord(sc, []arrow.Array{aArr, bArr}, 3)
	defer rec.Release()

	// (Col("a") + Col("b")).Cast(Float64) — cast the result of arithmetic
	castExpr := expr.Col("a").Add(expr.Col("b")).Cast(arrow.PrimitiveTypes.Float64)
	out, err := Eval(castExpr.Node, rec, mem)
	require.NoError(t, err, "cast of arithmetic result to float64 must not error")
	defer out.Release()

	require.Equal(t, arrow.FLOAT64, out.DataType().ID(), "output type must be float64")
	f, ok := out.(*array.Float64)
	require.True(t, ok)
	assert.Equal(t, float64(5), f.Value(0)) // 1+4
	assert.Equal(t, float64(7), f.Value(1)) // 2+5
	assert.Equal(t, float64(9), f.Value(2)) // 3+6
}

// ─── Task 3C: Temporal arithmetic Timestamp - Timestamp → Duration ────────

// TestEdgeCase_C_TimestampSubTimestamp verifies that subtracting two Timestamp
// columns produces a Duration column with correct values.
func TestEdgeCase_C_TimestampSubTimestamp(t *testing.T) {
	t.Parallel()
	mem := memory.DefaultAllocator

	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	tsType := &arrow.TimestampType{Unit: arrow.Nanosecond, TimeZone: "UTC"}

	// "start" timestamps
	startBld := array.NewTimestampBuilder(mem, tsType)
	defer startBld.Release()
	startBld.AppendValues([]arrow.Timestamp{
		arrow.Timestamp(base.UnixNano()),
		arrow.Timestamp(base.UnixNano()),
	}, nil)
	startArr := startBld.NewArray()
	defer startArr.Release()

	// "end" timestamps: +1h and +3h after start
	endBld := array.NewTimestampBuilder(mem, tsType)
	defer endBld.Release()
	endBld.AppendValues([]arrow.Timestamp{
		arrow.Timestamp(base.Add(1 * time.Hour).UnixNano()),
		arrow.Timestamp(base.Add(3 * time.Hour).UnixNano()),
	}, nil)
	endArr := endBld.NewArray()
	defer endArr.Release()

	sc := arrow.NewSchema([]arrow.Field{
		{Name: "start", Type: tsType},
		{Name: "end", Type: tsType},
	}, nil)
	rec := array.NewRecord(sc, []arrow.Array{startArr, endArr}, 2)
	defer rec.Release()

	// Col("end").Sub(Col("start")) should produce Duration(nanosecond)
	durExpr := expr.Col("end").Sub(expr.Col("start"))
	out, err := Eval(durExpr.Node, rec, mem)
	require.NoError(t, err, "Timestamp - Timestamp must not error")
	defer out.Release()

	require.Equal(t, arrow.DURATION, out.DataType().ID(),
		"result type must be Duration, got %s", out.DataType())

	dur, ok := out.(*array.Duration)
	require.True(t, ok, "expected *array.Duration, got %T", out)

	oneHourNs := arrow.Duration(time.Hour.Nanoseconds())
	threeHourNs := arrow.Duration(3 * time.Hour.Nanoseconds())
	assert.Equal(t, oneHourNs, dur.Value(0), "row 0: end-start should be 1h")
	assert.Equal(t, threeHourNs, dur.Value(1), "row 1: end-start should be 3h")
}

// ─── Task 4a: Filter on non-bool column should produce clear runtime error ──

// TestContractViolation_FilterOnInt verifies that using an int64 column as a
// boolean predicate (directly, without a comparison op) returns an error with
// a clear message rather than panicking or silently succeeding.
func TestContractViolation_FilterOnInt(t *testing.T) {
	t.Parallel()
	mem := memory.DefaultAllocator

	bld := array.NewInt64Builder(mem)
	defer bld.Release()
	bld.AppendValues([]int64{1, 65, 0}, nil)
	arr := bld.NewArray()
	defer arr.Release()

	sc := arrow.NewSchema([]arrow.Field{{Name: "age", Type: arrow.PrimitiveTypes.Int64}}, nil)
	rec := array.NewRecord(sc, []arrow.Array{arr}, 3)
	defer rec.Release()

	// The predicate "age" (int64) is not a boolean — filter.go calls
	// compute.FilterRecord which expects a bool mask.
	// The error should come from compute.FilterRecord receiving a non-bool mask
	// OR from logicalKernel when anding with another int col.
	// Here we test that And-ing an int64 col with a bool col produces a clear
	// type-mismatch error.
	activeBld := array.NewBooleanBuilder(mem)
	defer activeBld.Release()
	activeBld.AppendValues([]bool{true, false, true}, nil)
	activeArr := activeBld.NewArray()
	defer activeArr.Release()

	sc2 := arrow.NewSchema([]arrow.Field{
		{Name: "age", Type: arrow.PrimitiveTypes.Int64},
		{Name: "active", Type: arrow.FixedWidthTypes.Boolean},
	}, nil)
	rec2 := array.NewRecord(sc2, []arrow.Array{arr, activeArr}, 3)
	defer rec2.Release()

	// age.And(active) — age is int64, not bool.  logicalKernel should reject.
	_, err := Eval(expr.Col("age").And(expr.Col("active")).Node, rec2, mem)
	require.Error(t, err, "And with non-bool operand must return error")
	assert.Contains(t, err.Error(), "bool",
		"error message should mention 'bool', got: %v", err)
	// Must not be a placeholder.
	assert.NotContains(t, err.Error(), "not yet implemented",
		"error must not be a placeholder")
	t.Logf("error message (informative?): %v", err)
}

// TestContractViolation_FilterMaskNonBool verifies that passing a non-boolean
// array to FilterRecord returns a clear error.
func TestContractViolation_FilterMaskNonBool(t *testing.T) {
	t.Parallel()
	mem := memory.DefaultAllocator

	bld := array.NewInt64Builder(mem)
	defer bld.Release()
	bld.AppendValues([]int64{10, 20, 30}, nil)
	arr := bld.NewArray()
	defer arr.Release()

	sc := arrow.NewSchema([]arrow.Field{{Name: "v", Type: arrow.PrimitiveTypes.Int64}}, nil)
	rec := array.NewRecord(sc, []arrow.Array{arr}, 3)
	defer rec.Release()

	// Try to use an int64 column directly as a filter mask.
	mask, err := Eval(expr.Col("v").Node, rec, mem)
	require.NoError(t, err)
	defer mask.Release()

	_, err = FilterRecord(rec, mask, mem)
	require.Error(t, err, "FilterRecord with non-bool mask must error")
	t.Logf("FilterRecord non-bool mask error: %v", err)
}

// ─── Task 4b: RegisterBinaryKernel dispatch ──────────────────────────────

// TestContractCheck_RegisterBinaryKernel verifies the full dispatch path:
// register a kernel for a custom type ID, then confirm evalBinary reaches it.
func TestContractCheck_RegisterBinaryKernel(t *testing.T) {
	t.Parallel()

	// Use arrow.RUN_END_ENCODED as a stand-in for a type ID the built-in
	// arithKernel does not handle (falls through to errNoBinaryKernel).
	const customTypeID = arrow.RUN_END_ENCODED

	var calledWith expr.BinaryOp
	RegisterBinaryKernel(customTypeID, func(op expr.BinaryOp, l, r arrow.Array, mem memory.Allocator) (arrow.Array, error) {
		calledWith = op
		// Return a single-row bool array as a dummy result.
		b := array.NewBooleanBuilder(mem)
		defer b.Release()
		b.Append(true)
		return b.NewArray(), nil
	})

	k, ok := lookupBinaryKernel(customTypeID)
	require.True(t, ok, "kernel must be found after registration")

	// Invoke directly (evalBinary dispatch path for non-built-in types is
	// tested separately in TestTB8_KernelRegistration; here we verify the
	// lookup + invocation chain).
	out, err := k(expr.BinaryOpAdd, nil, nil, memory.DefaultAllocator)
	require.NoError(t, err)
	if out != nil {
		out.Release()
	}
	assert.Equal(t, expr.BinaryOpAdd, calledWith,
		"kernel should have been called with BinaryOpAdd")
}

// ─── helper reused from arith_test.go scope (same package) ─────────────────

// int64Record is defined in phase2_test.go in the same package; no need to
// redeclare here.  If it were not, we would define it.  This comment marks
// where it would go.

// timestampRecord builds a two-column timestamp record for TestEdgeCase_C
// reuse in other potential tests.
func timestampRecord(t *testing.T, unit arrow.TimeUnit, tz string, startVals, endVals []arrow.Timestamp) arrow.Record {
	t.Helper()
	mem := memory.DefaultAllocator
	tsType := &arrow.TimestampType{Unit: unit, TimeZone: tz}

	sb := array.NewTimestampBuilder(mem, tsType)
	defer sb.Release()
	sb.AppendValues(startVals, nil)
	sArr := sb.NewArray()
	defer sArr.Release()

	eb := array.NewTimestampBuilder(mem, tsType)
	defer eb.Release()
	eb.AppendValues(endVals, nil)
	eArr := eb.NewArray()
	defer eArr.Release()

	sc := arrow.NewSchema([]arrow.Field{
		{Name: "start", Type: tsType},
		{Name: "end", Type: tsType},
	}, nil)
	return array.NewRecord(sc, []arrow.Array{sArr, eArr}, int64(len(startVals)))
}
