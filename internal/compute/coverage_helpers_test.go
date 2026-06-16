package compute

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// chunkedFrom builds a single-chunk *arrow.Chunked from a freshly built array.
// The caller owns the returned chunked and must Release it.
func chunkedFrom(t *testing.T, dt arrow.DataType, arr arrow.Array) *arrow.Chunked {
	t.Helper()
	ch := arrow.NewChunked(dt, []arrow.Array{arr})
	arr.Release()
	return ch
}

// oneColRecord builds a one-column record around arr; arr is released into the
// record. The caller owns the record.
func oneColRecord(t *testing.T, name string, arr arrow.Array) arrow.Record {
	t.Helper()
	s := arrow.NewSchema([]arrow.Field{{Name: name, Type: arr.DataType()}}, nil)
	rec := array.NewRecord(s, []arrow.Array{arr}, int64(arr.Len()))
	arr.Release()
	return rec
}

// twoColRecord builds a two-column record around a and b; both are released into
// the record. The caller owns the record.
func twoColRecord(t *testing.T, nameA string, a arrow.Array, nameB string, b arrow.Array) arrow.Record {
	t.Helper()
	s := arrow.NewSchema([]arrow.Field{
		{Name: nameA, Type: a.DataType()},
		{Name: nameB, Type: b.DataType()},
	}, nil)
	rec := array.NewRecord(s, []arrow.Array{a, b}, int64(a.Len()))
	a.Release()
	b.Release()
	return rec
}

// newGoMem returns a fresh per-test allocator with no shared global state.
func newGoMem() memory.Allocator { return memory.NewGoAllocator() }
