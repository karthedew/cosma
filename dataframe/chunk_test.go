package dataframe

import (
	"reflect"
	"testing"

	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"
	"github.com/apache/arrow/go/v18/arrow/memory"
)

// multiChunkInt64 builds a Series whose column has one chunk per group.
func multiChunkInt64(t *testing.T, name string, groups [][]int64) *Series {
	t.Helper()
	arrs := make([]arrow.Array, len(groups))
	for i, g := range groups {
		b := array.NewInt64Builder(memory.DefaultAllocator)
		b.AppendValues(g, nil)
		arrs[i] = b.NewArray()
		b.Release()
	}
	ch := arrow.NewChunked(arrow.PrimitiveTypes.Int64, arrs)
	for _, a := range arrs {
		a.Release()
	}
	return NewSeriesFromChunked(name, ch)
}

func TestChunkSizesAligned(t *testing.T) {
	a := multiChunkInt64(t, "a", [][]int64{{1, 2}, {3}})
	b := multiChunkInt64(t, "b", [][]int64{{10, 20}, {30}})
	df, err := New([]*Series{a, b})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	if got := df.NumChunks(); got != 2 {
		t.Fatalf("NumChunks = %d, want 2", got)
	}
	if got := df.ChunkSizes(); !reflect.DeepEqual(got, []int64{2, 1}) {
		t.Fatalf("ChunkSizes = %v, want [2 1]", got)
	}
}

func TestChunkSizesMisaligned(t *testing.T) {
	// a cuts at row 2, b cuts at row 1: the canonical layout cuts at both.
	a := multiChunkInt64(t, "a", [][]int64{{1, 2}, {3}})
	b := multiChunkInt64(t, "b", [][]int64{{10}, {20, 30}})
	df, err := New([]*Series{a, b})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	if got := df.NumChunks(); got != 3 {
		t.Fatalf("NumChunks = %d, want 3", got)
	}
	if got := df.ChunkSizes(); !reflect.DeepEqual(got, []int64{1, 1, 1}) {
		t.Fatalf("ChunkSizes = %v, want [1 1 1]", got)
	}
}

func TestChunkSizesSingleChunk(t *testing.T) {
	df := threeRowDF(t)
	if got := df.NumChunks(); got != 1 {
		t.Fatalf("NumChunks = %d, want 1", got)
	}
	if got := df.ChunkSizes(); !reflect.DeepEqual(got, []int64{3}) {
		t.Fatalf("ChunkSizes = %v, want [3]", got)
	}
}

func TestChunkSizesEmpty(t *testing.T) {
	df := threeRowDF(t).Head(0)
	if got := df.NumChunks(); got != 0 {
		t.Fatalf("NumChunks = %d, want 0", got)
	}
	if got := df.ChunkSizes(); got != nil {
		t.Fatalf("ChunkSizes = %v, want nil", got)
	}
}
