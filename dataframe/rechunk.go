package dataframe

import (
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// Rechunk returns a new DataFrame in which every column is coalesced into a
// single contiguous chunk. The receiver is left unchanged; the result owns its
// own (copied) Arrow buffers.
func (df *DataFrame) Rechunk() *DataFrame {
	if df == nil {
		return nil
	}

	return df.mapColumns(func(_ string, chunked *arrow.Chunked) *arrow.Chunked {
		if chunked == nil {
			return nil
		}
		return coalesce(chunked)
	})
}

// ShouldRechunk reports whether the DataFrame's columns are misaligned, meaning
// at least one column's chunk boundaries differ from the canonical layout.
// Misaligned columns force per-segment slicing during batch materialization, so
// coalescing them first is usually worthwhile.
func (df *DataFrame) ShouldRechunk() bool {
	if df == nil || df.height == 0 {
		return false
	}
	canonical := df.chunkOffsets()
	for i := range df.cols {
		chunked := df.cols[i].Chunked()
		if chunked == nil {
			continue
		}
		if !equalOffsets(columnOffsets(chunked), canonical) {
			return true
		}
	}
	return false
}

// RechunkIfNeeded coalesces the DataFrame only when ShouldRechunk reports that
// its columns are misaligned; otherwise it returns the receiver unchanged.
func (df *DataFrame) RechunkIfNeeded() *DataFrame {
	if df.ShouldRechunk() {
		return df.Rechunk()
	}
	return df
}

// columnOffsets returns the cumulative row offsets bounding a column's chunks,
// always starting at 0 and ending at the column length.
func columnOffsets(ch *arrow.Chunked) []int64 {
	offsets := []int64{0}
	var off int64
	for _, c := range ch.Chunks() {
		off += int64(c.Len())
		offsets = append(offsets, off)
	}
	return offsets
}

func equalOffsets(a, b []int64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// RechunkToRows returns a new DataFrame whose columns are repartitioned into
// uniform chunks of n rows each (the final chunk may be smaller). A non-positive
// n coalesces to a single chunk. The receiver is left unchanged.
func (df *DataFrame) RechunkToRows(n int) *DataFrame {
	if df == nil {
		return nil
	}
	if n <= 0 {
		return df.Rechunk()
	}

	return df.mapColumns(func(_ string, chunked *arrow.Chunked) *arrow.Chunked {
		if chunked == nil {
			return nil
		}
		return repartition(chunked, int64(n))
	})
}

// repartition coalesces ch and re-slices it into windows of n rows.
func repartition(ch *arrow.Chunked, n int64) *arrow.Chunked {
	single := coalesce(ch)
	chunks := single.Chunks()
	if len(chunks) == 0 {
		return single
	}

	full := chunks[0]
	total := int64(full.Len())
	var slices []arrow.Array
	for off := int64(0); off < total; off += n {
		end := off + n
		if end > total {
			end = total
		}
		slices = append(slices, array.NewSlice(full, off, end))
	}

	out := arrow.NewChunked(ch.DataType(), slices)
	for _, s := range slices {
		s.Release()
	}
	single.Release()
	return out
}

// coalesce concatenates all chunks of ch into a single-chunk Chunked. When ch is
// already a single chunk (or empty) it is rebuilt without a concat copy.
func coalesce(ch *arrow.Chunked) *arrow.Chunked {
	chunks := ch.Chunks()
	if len(chunks) <= 1 {
		return arrow.NewChunked(ch.DataType(), chunks)
	}

	merged, err := array.Concatenate(chunks, memory.DefaultAllocator)
	if err != nil {
		// Concatenate only fails on incompatible types, which cannot happen
		// within a single typed column; fall back to the original chunks.
		return arrow.NewChunked(ch.DataType(), chunks)
	}
	out := arrow.NewChunked(ch.DataType(), []arrow.Array{merged})
	merged.Release()
	return out
}
