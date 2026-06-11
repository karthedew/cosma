package dataframe

import "sort"

// NumChunks reports the number of chunks in the DataFrame's canonical chunk
// layout. When every column shares the same chunk boundaries this is just that
// shared chunk count; when columns are chunked differently it is the number of
// aligned row segments (see ChunkSizes).
func (df *DataFrame) NumChunks() int {
	return len(df.ChunkSizes())
}

// ChunkSizes returns the row count of each chunk in the canonical chunk layout,
// in row order. The layout is the coarsest partition of rows whose boundaries
// respect every column's chunk boundaries, so each segment lies within a single
// chunk of every column and can be materialized as one record batch without
// crossing a chunk seam. An empty DataFrame returns nil.
func (df *DataFrame) ChunkSizes() []int64 {
	if df == nil || df.height == 0 {
		return nil
	}

	offsets := df.chunkOffsets()
	sizes := make([]int64, 0, len(offsets)-1)
	for i := 1; i < len(offsets); i++ {
		sizes = append(sizes, offsets[i]-offsets[i-1])
	}
	return sizes
}

// chunkOffsets returns the sorted, de-duplicated row offsets that bound the
// canonical chunk layout, always including 0 and the row count. It is the union
// of every column's chunk boundaries.
func (df *DataFrame) chunkOffsets() []int64 {
	set := map[int64]struct{}{0: {}, df.height: {}}
	for i := range df.cols {
		chunked := df.cols[i].Chunked()
		if chunked == nil {
			continue
		}
		var off int64
		for _, c := range chunked.Chunks() {
			off += int64(c.Len())
			set[off] = struct{}{}
		}
	}

	offsets := make([]int64, 0, len(set))
	for o := range set {
		offsets = append(offsets, o)
	}
	sort.Slice(offsets, func(i, j int) bool { return offsets[i] < offsets[j] })
	return offsets
}
