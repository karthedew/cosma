package dataframe

import (
	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"
)

// Head returns a new DataFrame containing the first n rows across all chunks.
//
// n is clamped to [0, NumRows]: a non-positive n yields an empty DataFrame that
// keeps the schema, and an n larger than the row count yields all rows. The
// result shares Arrow buffers with the receiver (zero-copy slices); the
// receiver is left unchanged.
func (df *DataFrame) Head(n int) *DataFrame {
	if df == nil {
		return nil
	}
	rows := int64(n)
	if rows < 0 {
		rows = 0
	}
	if rows > df.height {
		rows = df.height
	}

	return df.mapColumns(func(_ string, chunked *arrow.Chunked) *arrow.Chunked {
		if chunked == nil {
			return nil
		}
		return headChunked(chunked, rows)
	})
}

// headChunked builds a new chunked column holding the first n rows of ch.
// Each retained slice is zero-copy; ownership transfers to the returned Chunked.
func headChunked(ch *arrow.Chunked, n int64) *arrow.Chunked {
	var slices []arrow.Array
	remaining := n
	for _, chunk := range ch.Chunks() {
		if remaining <= 0 {
			break
		}
		take := int64(chunk.Len())
		if take > remaining {
			take = remaining
		}
		slices = append(slices, array.NewSlice(chunk, 0, take))
		remaining -= take
	}

	out := arrow.NewChunked(ch.DataType(), slices)
	for _, s := range slices {
		s.Release()
	}
	return out
}
