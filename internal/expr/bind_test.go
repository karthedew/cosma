package expr

import (
	"testing"

	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"
	"github.com/apache/arrow/go/v18/arrow/memory"

	"github.com/karthedew/cosma/schema"
)

func TestBindPredicateStringGtFails(t *testing.T) {
	s := schema.New(schema.Field{Name: "name", Type: schema.Utf8, ArrowType: arrow.BinaryTypes.String})
	_, err := BindPredicate(Gt{Left: ColumnNode{Name: "name"}, Right: LiteralNode{Value: "a"}}, s)
	if err == nil {
		t.Fatalf("expected bind error")
	}
}

func TestBoundPredicateEval(t *testing.T) {
	s := schema.New(schema.Field{Name: "ids", Type: schema.Int32, ArrowType: arrow.PrimitiveTypes.Int32})
	pred, err := BindPredicate(Gt{Left: ColumnNode{Name: "ids"}, Right: LiteralNode{Value: 1}}, s)
	if err != nil {
		t.Fatalf("BindPredicate: %v", err)
	}

	builder := array.NewInt32Builder(memory.DefaultAllocator)
	defer builder.Release()
	builder.AppendValues([]int32{1, 2, 3}, nil)
	arr := builder.NewArray()
	defer arr.Release()

	rec := array.NewRecord(arrow.NewSchema([]arrow.Field{{Name: "ids", Type: arrow.PrimitiveTypes.Int32}}, nil), []arrow.Array{arr}, int64(arr.Len()))
	defer rec.Release()

	mask, err := pred.Eval(rec)
	if err != nil {
		t.Fatalf("Eval: %v", err)
	}
	defer mask.Release()

	bools := mask.(*array.Boolean)
	if bools.Len() != 3 {
		t.Fatalf("expected 3 mask values")
	}
	if bools.Value(0) || !bools.Value(1) || !bools.Value(2) {
		t.Fatalf("unexpected mask values")
	}
}
