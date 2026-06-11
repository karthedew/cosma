package dataframe

import (
	"reflect"
	"testing"
)

func TestRechunkCoalescesToSingleChunk(t *testing.T) {
	a := multiChunkInt64(t, "a", [][]int64{{1, 2}, {3}})
	df, err := New([]*Series{a})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	got := df.Rechunk()
	if got.NumChunks() != 1 {
		t.Fatalf("NumChunks = %d, want 1", got.NumChunks())
	}
	if vals := int64Values(t, got, "a"); !reflect.DeepEqual(vals, []int64{1, 2, 3}) {
		t.Fatalf("values = %v, want [1 2 3]", vals)
	}
	if got.NumRows() != 3 {
		t.Fatalf("NumRows = %d, want 3", got.NumRows())
	}

	// Receiver is unchanged.
	if df.NumChunks() != 2 {
		t.Fatalf("receiver NumChunks = %d, want 2", df.NumChunks())
	}
}

func TestRechunkToRows(t *testing.T) {
	// Start misaligned, then repartition to a uniform 2 rows per chunk.
	a := multiChunkInt64(t, "a", [][]int64{{1, 2, 3}, {4, 5}})
	df, err := New([]*Series{a})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	got := df.RechunkToRows(2)
	if sizes := got.ChunkSizes(); !reflect.DeepEqual(sizes, []int64{2, 2, 1}) {
		t.Fatalf("ChunkSizes = %v, want [2 2 1]", sizes)
	}
	if vals := int64Values(t, got, "a"); !reflect.DeepEqual(vals, []int64{1, 2, 3, 4, 5}) {
		t.Fatalf("values = %v, want [1 2 3 4 5]", vals)
	}

	// n <= 0 coalesces to a single chunk.
	if got := df.RechunkToRows(0); got.NumChunks() != 1 {
		t.Fatalf("RechunkToRows(0).NumChunks = %d, want 1", got.NumChunks())
	}
}

func TestShouldRechunkAligned(t *testing.T) {
	a := multiChunkInt64(t, "a", [][]int64{{1, 2}, {3}})
	b := multiChunkInt64(t, "b", [][]int64{{10, 20}, {30}})
	df, err := New([]*Series{a, b})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	if df.ShouldRechunk() {
		t.Fatalf("ShouldRechunk = true for aligned frame, want false")
	}
	if got := df.RechunkIfNeeded(); got.NumChunks() != 2 {
		t.Fatalf("RechunkIfNeeded NumChunks = %d, want 2 (unchanged)", got.NumChunks())
	}
}

func TestShouldRechunkMisaligned(t *testing.T) {
	a := multiChunkInt64(t, "a", [][]int64{{1, 2}, {3}})
	b := multiChunkInt64(t, "b", [][]int64{{10}, {20, 30}})
	df, err := New([]*Series{a, b})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	if !df.ShouldRechunk() {
		t.Fatalf("ShouldRechunk = false for misaligned frame, want true")
	}
	if got := df.RechunkIfNeeded(); got.NumChunks() != 1 {
		t.Fatalf("RechunkIfNeeded NumChunks = %d, want 1 (coalesced)", got.NumChunks())
	}
}
