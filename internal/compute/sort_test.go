package compute

import (
	"reflect"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func TestSortIndicesNullsLast(t *testing.T) {
	mem := memory.DefaultAllocator
	b := array.NewInt64Builder(mem)
	// values: 3, null, 1, 2  -> ascending order of indices: 2,3,0, then null 1
	b.Append(3)
	b.AppendNull()
	b.Append(1)
	b.Append(2)
	a := b.NewArray()
	b.Release()
	ch := arrow.NewChunked(arrow.PrimitiveTypes.Int64, []arrow.Array{a})
	a.Release()
	defer ch.Release()

	idx, err := SortIndices(ch, false, false)
	if err != nil {
		t.Fatalf("SortIndices: %v", err)
	}
	if !reflect.DeepEqual(idx, []int64{2, 3, 0, 1}) {
		t.Fatalf("idx = %v, want [2 3 0 1]", idx)
	}
}
