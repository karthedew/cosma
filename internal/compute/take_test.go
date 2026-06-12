package compute

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/require"
)

// takeChunkedInt64 builds a chunked Int64 array from the given chunks. A nil entry
// in a chunk's value slice marks a null.
func takeChunkedInt64(t *testing.T, mem memory.Allocator, chunks ...[]*int64) *arrow.Chunked {
	t.Helper()
	arrs := make([]arrow.Array, len(chunks))
	for i, c := range chunks {
		b := array.NewInt64Builder(mem)
		for _, v := range c {
			if v == nil {
				b.AppendNull()
			} else {
				b.Append(*v)
			}
		}
		arrs[i] = b.NewArray()
		b.Release()
	}
	ch := arrow.NewChunked(arrow.PrimitiveTypes.Int64, arrs)
	for _, a := range arrs {
		a.Release()
	}
	return ch
}

func i64p(v int64) *int64 { return &v }

func TestTake_AcrossChunks(t *testing.T) {
	t.Parallel()
	mem := memory.NewGoAllocator()

	// Two chunks: [1,2] and [3,4,5]. Logical indices 0..4.
	ch := takeChunkedInt64(t, mem,
		[]*int64{i64p(1), i64p(2)},
		[]*int64{i64p(3), i64p(4), i64p(5)},
	)
	defer ch.Release()

	out, err := Take(ch, []int64{4, 0, 2}, mem)
	require.NoError(t, err)
	defer out.Release()

	got := out.(*array.Int64)
	require.Equal(t, 3, got.Len())
	require.Equal(t, int64(5), got.Value(0))
	require.Equal(t, int64(1), got.Value(1))
	require.Equal(t, int64(3), got.Value(2))
}

func TestTake_PreservesNull(t *testing.T) {
	t.Parallel()
	mem := memory.NewGoAllocator()

	// Index 1 in chunk order is null.
	ch := takeChunkedInt64(t, mem,
		[]*int64{i64p(1), nil},
		[]*int64{i64p(3), i64p(4), i64p(5)},
	)
	defer ch.Release()

	out, err := Take(ch, []int64{0, 1, 3}, mem)
	require.NoError(t, err)
	defer out.Release()

	got := out.(*array.Int64)
	require.Equal(t, 3, got.Len())
	require.False(t, got.IsNull(0))
	require.Equal(t, int64(1), got.Value(0))
	require.True(t, got.IsNull(1))
	require.False(t, got.IsNull(2))
	require.Equal(t, int64(4), got.Value(2))
}

func TestTake_NegativeIndexEmitsNull(t *testing.T) {
	t.Parallel()
	mem := memory.NewGoAllocator()
	ch := takeChunkedInt64(t, mem, []*int64{i64p(7), i64p(8)})
	defer ch.Release()

	out, err := Take(ch, []int64{0, -1, 1}, mem)
	require.NoError(t, err)
	defer out.Release()

	got := out.(*array.Int64)
	require.Equal(t, int64(7), got.Value(0))
	require.True(t, got.IsNull(1))
	require.Equal(t, int64(8), got.Value(2))
}

func TestTake_OutOfRange(t *testing.T) {
	t.Parallel()
	mem := memory.NewGoAllocator()
	ch := takeChunkedInt64(t, mem, []*int64{i64p(1)})
	defer ch.Release()

	_, err := Take(ch, []int64{5}, mem)
	require.Error(t, err)
}
