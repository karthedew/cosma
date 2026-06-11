package compute

import (
	"math"
	"testing"

	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"
	"github.com/apache/arrow/go/v18/arrow/memory"
)

// chunkedInt64WithNulls builds a two-chunk int64 column; a nil validity slice
// means all-valid, otherwise valid[i]==false marks a null.
func chunkedInt64WithNulls(t *testing.T) *arrow.Chunked {
	t.Helper()
	mem := memory.DefaultAllocator
	b1 := array.NewInt64Builder(mem)
	b1.AppendValues([]int64{1, 2}, nil)
	a1 := b1.NewArray()
	b1.Release()

	b2 := array.NewInt64Builder(mem)
	b2.AppendValues([]int64{0, 4}, []bool{false, true}) // first is null
	a2 := b2.NewArray()
	b2.Release()

	ch := arrow.NewChunked(arrow.PrimitiveTypes.Int64, []arrow.Array{a1, a2})
	a1.Release()
	a2.Release()
	return ch
}

func TestReduceSkipsNulls(t *testing.T) {
	ch := chunkedInt64WithNulls(t)
	defer ch.Release()

	ag, err := Reduce(ch)
	if err != nil {
		t.Fatalf("Reduce: %v", err)
	}
	if ag.Count != 3 {
		t.Fatalf("Count = %d, want 3", ag.Count)
	}
	if ag.Sum != int64(7) {
		t.Fatalf("Sum = %v, want 7", ag.Sum)
	}
	if ag.Min != int64(1) || ag.Max != int64(4) {
		t.Fatalf("Min/Max = %v/%v, want 1/4", ag.Min, ag.Max)
	}
	if math.Abs(ag.Mean-7.0/3.0) > 1e-9 {
		t.Fatalf("Mean = %v, want %v", ag.Mean, 7.0/3.0)
	}
}

func TestReduceEmptyOrAllNull(t *testing.T) {
	mem := memory.DefaultAllocator
	b := array.NewInt64Builder(mem)
	b.AppendNulls(2)
	a := b.NewArray()
	b.Release()
	ch := arrow.NewChunked(arrow.PrimitiveTypes.Int64, []arrow.Array{a})
	a.Release()
	defer ch.Release()

	ag, err := Reduce(ch)
	if err != nil {
		t.Fatalf("Reduce: %v", err)
	}
	if ag.Count != 0 {
		t.Fatalf("Count = %d, want 0", ag.Count)
	}
	if ag.Sum != nil || ag.Min != nil || ag.Max != nil {
		t.Fatalf("all-null reductions should be nil, got sum=%v min=%v max=%v", ag.Sum, ag.Min, ag.Max)
	}
}
