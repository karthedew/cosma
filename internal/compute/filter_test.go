package compute

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/karthedew/cosma/expr"
)

// int64Record builds a one-column int64 record batch from vals.
func int64Record(t *testing.T, name string, vals []int64) arrow.Record {
	t.Helper()
	mem := memory.DefaultAllocator
	bld := array.NewInt64Builder(mem)
	defer bld.Release()
	bld.AppendValues(vals, nil)
	arr := bld.NewArray()
	defer arr.Release()

	schema := arrow.NewSchema([]arrow.Field{{Name: name, Type: arrow.PrimitiveTypes.Int64}}, nil)
	return array.NewRecord(schema, []arrow.Array{arr}, int64(len(vals)))
}

func TestEvalCompareProducesMask(t *testing.T) {
	rec := int64Record(t, "a", []int64{1, 2, 3, 4})
	defer rec.Release()

	mask, err := Eval(expr.Col("a").Gt(expr.Lit(int64(2))).Node, rec, memory.DefaultAllocator)
	if err != nil {
		t.Fatalf("Eval: %v", err)
	}
	defer mask.Release()

	b, ok := mask.(*array.Boolean)
	if !ok {
		t.Fatalf("mask type = %T, want *array.Boolean", mask)
	}
	want := []bool{false, false, true, true}
	if b.Len() != len(want) {
		t.Fatalf("mask len = %d, want %d", b.Len(), len(want))
	}
	for i, w := range want {
		if b.Value(i) != w {
			t.Fatalf("mask[%d] = %v, want %v", i, b.Value(i), w)
		}
	}
}

func TestFilterRecordSelectsRows(t *testing.T) {
	rec := int64Record(t, "a", []int64{10, 20, 30})
	defer rec.Release()

	mask, err := Eval(expr.Col("a").Gte(expr.Lit(int64(20))).Node, rec, memory.DefaultAllocator)
	if err != nil {
		t.Fatalf("Eval: %v", err)
	}
	defer mask.Release()

	out, err := FilterRecord(rec, mask, memory.DefaultAllocator)
	if err != nil {
		t.Fatalf("FilterRecord: %v", err)
	}
	defer out.Release()

	if out.NumRows() != 2 {
		t.Fatalf("NumRows = %d, want 2", out.NumRows())
	}
	col := out.Column(0).(*array.Int64)
	if col.Value(0) != 20 || col.Value(1) != 30 {
		t.Fatalf("values = [%d %d], want [20 30]", col.Value(0), col.Value(1))
	}
}

func TestFilterRecordRejectsNonBoolMask(t *testing.T) {
	rec := int64Record(t, "a", []int64{1, 2, 3})
	defer rec.Release()

	// A bare column evaluates to int64, not a boolean mask.
	mask, err := Eval(expr.Col("a").Node, rec, memory.DefaultAllocator)
	if err != nil {
		t.Fatalf("Eval: %v", err)
	}
	defer mask.Release()

	if _, err := FilterRecord(rec, mask, memory.DefaultAllocator); err == nil {
		t.Fatalf("expected error for non-boolean mask")
	}
}
