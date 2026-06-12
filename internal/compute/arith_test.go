package compute

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/karthedew/cosma/expr"
)

func twoColInt64(t *testing.T, a, b []int64) arrow.Record {
	t.Helper()
	mem := memory.DefaultAllocator
	build := func(vals []int64) arrow.Array {
		bld := array.NewInt64Builder(mem)
		defer bld.Release()
		bld.AppendValues(vals, nil)
		return bld.NewArray()
	}
	ca, cb := build(a), build(b)
	defer ca.Release()
	defer cb.Release()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "a", Type: arrow.PrimitiveTypes.Int64},
		{Name: "b", Type: arrow.PrimitiveTypes.Int64},
	}, nil)
	return array.NewRecord(schema, []arrow.Array{ca, cb}, int64(len(a)))
}

func TestEvalArithAddColumns(t *testing.T) {
	rec := twoColInt64(t, []int64{1, 2, 3}, []int64{10, 20, 30})
	defer rec.Release()

	out, err := Eval(expr.Col("a").Add(expr.Col("b")).Node, rec, memory.DefaultAllocator)
	if err != nil {
		t.Fatalf("Eval: %v", err)
	}
	defer out.Release()

	got := out.(*array.Int64)
	want := []int64{11, 22, 33}
	for i, w := range want {
		if got.Value(i) != w {
			t.Fatalf("out[%d] = %d, want %d", i, got.Value(i), w)
		}
	}
}

func TestEvalArithDivByZeroIsNull(t *testing.T) {
	rec := twoColInt64(t, []int64{10, 20}, []int64{2, 0})
	defer rec.Release()

	out, err := Eval(expr.Col("a").Div(expr.Col("b")).Node, rec, memory.DefaultAllocator)
	if err != nil {
		t.Fatalf("Eval: %v", err)
	}
	defer out.Release()

	got := out.(*array.Int64)
	if got.IsNull(0) || got.Value(0) != 5 {
		t.Fatalf("out[0] = %d (null=%v), want 5", got.Value(0), got.IsNull(0))
	}
	if !got.IsNull(1) {
		t.Fatalf("out[1] should be null (division by zero)")
	}
}
