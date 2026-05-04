package exec

import (
	"context"
	"errors"
	"testing"

	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"
	"github.com/apache/arrow/go/v18/arrow/memory"

	"github.com/karthedew/cosma/internal/expr"
)

// trackingAllocator wraps memory.CheckedAllocator so tests fail loudly if
// Eval forgets to release an intermediate.
type trackingAllocator = memory.CheckedAllocator

func newTrackingAlloc(t *testing.T) *trackingAllocator {
	t.Helper()
	return memory.NewCheckedAllocator(memory.NewGoAllocator())
}

func assertNoLeaks(t *testing.T, a *trackingAllocator) {
	t.Helper()
	a.AssertSize(t, 0)
}

// makeInt64Batch builds a single-batch record with one int64 column.
// Nullability is honored via the boolean validity slice (nil = all valid).
func makeInt64Batch(t *testing.T, mem memory.Allocator, name string, values []int64, valid []bool) arrow.Record {
	t.Helper()
	b := array.NewInt64Builder(mem)
	defer b.Release()
	b.AppendValues(values, valid)
	arr := b.NewArray()
	defer arr.Release()
	sch := arrow.NewSchema([]arrow.Field{{Name: name, Type: arrow.PrimitiveTypes.Int64, Nullable: true}}, nil)
	return array.NewRecord(sch, []arrow.Array{arr}, int64(len(values)))
}

func makeTwoInt64Batch(t *testing.T, mem memory.Allocator, a, b []int64) arrow.Record {
	t.Helper()
	ba := array.NewInt64Builder(mem)
	defer ba.Release()
	ba.AppendValues(a, nil)
	aa := ba.NewArray()
	defer aa.Release()

	bb := array.NewInt64Builder(mem)
	defer bb.Release()
	bb.AppendValues(b, nil)
	bbarr := bb.NewArray()
	defer bbarr.Release()

	sch := arrow.NewSchema([]arrow.Field{
		{Name: "a", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "b", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	}, nil)
	return array.NewRecord(sch, []arrow.Array{aa, bbarr}, int64(len(a)))
}

func boolValues(t *testing.T, arr arrow.Array) []*bool {
	t.Helper()
	bools, ok := arr.(*array.Boolean)
	if !ok {
		t.Fatalf("expected *array.Boolean, got %T", arr)
	}
	out := make([]*bool, bools.Len())
	for i := 0; i < bools.Len(); i++ {
		if bools.IsNull(i) {
			out[i] = nil
			continue
		}
		v := bools.Value(i)
		out[i] = &v
	}
	return out
}

func ptr(b bool) *bool { return &b }

func equalBoolMask(got []*bool, want []*bool) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if (got[i] == nil) != (want[i] == nil) {
			return false
		}
		if got[i] != nil && *got[i] != *want[i] {
			return false
		}
	}
	return true
}

func TestEvalColumnEqLiteral(t *testing.T) {
	mem := newTrackingAlloc(t)
	rec := makeInt64Batch(t, mem, "ids", []int64{1, 2, 3, 0}, []bool{true, true, true, false})
	defer rec.Release()

	tree := expr.Col("ids").Eq(2).Build()
	out, err := Eval(context.Background(), tree, rec, nil, mem)
	if err != nil {
		t.Fatalf("Eval: %v", err)
	}
	defer out.Release()

	got := boolValues(t, out)
	want := []*bool{ptr(false), ptr(true), ptr(false), nil}
	if !equalBoolMask(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestEvalColumnEqColumn(t *testing.T) {
	mem := newTrackingAlloc(t)
	rec := makeTwoInt64Batch(t, mem, []int64{1, 2, 3}, []int64{1, 5, 3})
	defer rec.Release()

	tree := expr.Col("a").Eq(expr.Col("b")).Build()
	out, err := Eval(context.Background(), tree, rec, nil, mem)
	if err != nil {
		t.Fatalf("Eval: %v", err)
	}
	defer out.Release()

	got := boolValues(t, out)
	want := []*bool{ptr(true), ptr(false), ptr(true)}
	if !equalBoolMask(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestEvalContextCancelled(t *testing.T) {
	mem := newTrackingAlloc(t)
	rec := makeInt64Batch(t, mem, "ids", []int64{1, 2, 3}, nil)
	defer rec.Release()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	tree := expr.Col("ids").Eq(1).Build()
	_, err := Eval(ctx, tree, rec, nil, mem)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

func TestEvalColumnNotFound(t *testing.T) {
	mem := newTrackingAlloc(t)
	rec := makeInt64Batch(t, mem, "ids", []int64{1}, nil)
	defer rec.Release()

	tree := expr.Col("missing").Eq(1).Build()
	_, err := Eval(context.Background(), tree, rec, nil, mem)
	if err == nil {
		t.Fatalf("expected error for missing column")
	}
}

func TestEvalSelNotImplementedYet(t *testing.T) {
	mem := newTrackingAlloc(t)
	rec := makeInt64Batch(t, mem, "ids", []int64{1, 2}, nil)
	defer rec.Release()

	_, err := Eval(context.Background(), expr.Col("ids").Build(), rec, []int32{0}, mem)
	if err == nil {
		t.Fatalf("expected error when sel != nil")
	}
}

func TestEvalUnsupportedOp(t *testing.T) {
	mem := newTrackingAlloc(t)
	rec := makeInt64Batch(t, mem, "ids", []int64{1, 2}, nil)
	defer rec.Release()

	// Add is not in the kernel set yet — confirm we surface that cleanly.
	tree := expr.Col("ids").Add(1).Build()
	_, err := Eval(context.Background(), tree, rec, nil, mem)
	if err == nil {
		t.Fatalf("expected error for not-yet-implemented op")
	}
}

func TestEvalGtInt64(t *testing.T) {
	mem := newTrackingAlloc(t)
	rec := makeInt64Batch(t, mem, "ids", []int64{1, 2, 3, 4}, nil)
	defer rec.Release()

	tree := expr.Col("ids").Gt(2).Build()
	out, err := Eval(context.Background(), tree, rec, nil, mem)
	if err != nil {
		t.Fatalf("Eval: %v", err)
	}
	defer out.Release()

	got := boolValues(t, out)
	want := []*bool{ptr(false), ptr(false), ptr(true), ptr(true)}
	if !equalBoolMask(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestEvalEqFloat64(t *testing.T) {
	mem := newTrackingAlloc(t)
	b := array.NewFloat64Builder(mem)
	b.AppendValues([]float64{1.5, 2.5, 3.5}, nil)
	arr := b.NewArray()
	b.Release()
	defer arr.Release()
	sch := arrow.NewSchema([]arrow.Field{{Name: "x", Type: arrow.PrimitiveTypes.Float64}}, nil)
	rec := array.NewRecord(sch, []arrow.Array{arr}, 3)
	defer rec.Release()

	tree := expr.Col("x").Eq(2.5).Build()
	out, err := Eval(context.Background(), tree, rec, nil, mem)
	if err != nil {
		t.Fatalf("Eval: %v", err)
	}
	defer out.Release()

	got := boolValues(t, out)
	want := []*bool{ptr(false), ptr(true), ptr(false)}
	if !equalBoolMask(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestEvalEqString(t *testing.T) {
	mem := newTrackingAlloc(t)
	b := array.NewStringBuilder(mem)
	b.AppendValues([]string{"alpha", "beta", "gamma"}, nil)
	arr := b.NewArray()
	b.Release()
	defer arr.Release()
	sch := arrow.NewSchema([]arrow.Field{{Name: "s", Type: arrow.BinaryTypes.String}}, nil)
	rec := array.NewRecord(sch, []arrow.Array{arr}, 3)
	defer rec.Release()

	tree := expr.Col("s").Eq("beta").Build()
	out, err := Eval(context.Background(), tree, rec, nil, mem)
	if err != nil {
		t.Fatalf("Eval: %v", err)
	}
	defer out.Release()

	got := boolValues(t, out)
	want := []*bool{ptr(false), ptr(true), ptr(false)}
	if !equalBoolMask(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestEvalEqBool(t *testing.T) {
	mem := newTrackingAlloc(t)
	b := array.NewBooleanBuilder(mem)
	b.AppendValues([]bool{true, false, true}, nil)
	arr := b.NewArray()
	b.Release()
	defer arr.Release()
	sch := arrow.NewSchema([]arrow.Field{{Name: "f", Type: arrow.FixedWidthTypes.Boolean}}, nil)
	rec := array.NewRecord(sch, []arrow.Array{arr}, 3)
	defer rec.Release()

	tree := expr.Col("f").Eq(true).Build()
	out, err := Eval(context.Background(), tree, rec, nil, mem)
	if err != nil {
		t.Fatalf("Eval: %v", err)
	}
	defer out.Release()

	got := boolValues(t, out)
	want := []*bool{ptr(true), ptr(false), ptr(true)}
	if !equalBoolMask(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestEvalGtBoolUnsupported(t *testing.T) {
	mem := newTrackingAlloc(t)
	b := array.NewBooleanBuilder(mem)
	b.AppendValues([]bool{true, false}, nil)
	arr := b.NewArray()
	b.Release()
	defer arr.Release()
	sch := arrow.NewSchema([]arrow.Field{{Name: "f", Type: arrow.FixedWidthTypes.Boolean}}, nil)
	rec := array.NewRecord(sch, []arrow.Array{arr}, 2)
	defer rec.Release()

	tree := expr.Col("f").Gt(true).Build()
	_, err := Eval(context.Background(), tree, rec, nil, mem)
	if err == nil {
		t.Fatalf("expected error for Gt on bool")
	}
}

func TestEvalLiteralBroadcastTypedWidths(t *testing.T) {
	mem := newTrackingAlloc(t)
	// Float32 column compared against an Int32-typed literal would fail
	// type check; use matching widths to exercise both broadcast paths.
	fb := array.NewFloat32Builder(mem)
	fb.AppendValues([]float32{1.0, 2.0, 3.0}, nil)
	farr := fb.NewArray()
	fb.Release()
	defer farr.Release()
	sch := arrow.NewSchema([]arrow.Field{{Name: "x", Type: arrow.PrimitiveTypes.Float32}}, nil)
	rec := array.NewRecord(sch, []arrow.Array{farr}, 3)
	defer rec.Release()

	tree := expr.Col("x").Eq(expr.Float32(2.0)).Build()
	out, err := Eval(context.Background(), tree, rec, nil, mem)
	if err != nil {
		t.Fatalf("Eval: %v", err)
	}
	defer out.Release()

	got := boolValues(t, out)
	want := []*bool{ptr(false), ptr(true), ptr(false)}
	if !equalBoolMask(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestEvalReleasesIntermediates(t *testing.T) {
	// Run a binary op that allocates intermediates on both sides, release
	// only the top-level result, and assert the allocator is back to zero.
	// Any missed Release inside Eval shows up as a leak here.
	mem := newTrackingAlloc(t)
	rec := makeInt64Batch(t, mem, "ids", []int64{1, 2, 3, 4, 5}, nil)

	for i := 0; i < 10; i++ {
		tree := expr.Col("ids").Eq(int64(i)).Build()
		out, err := Eval(context.Background(), tree, rec, nil, mem)
		if err != nil {
			rec.Release()
			t.Fatalf("Eval: %v", err)
		}
		out.Release()
	}
	rec.Release()
	assertNoLeaks(t, mem)
}
