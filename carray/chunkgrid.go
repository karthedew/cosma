package carray

import (
	"fmt"
)

// ChunkGrid describes a regular chunking of an array: Shape is the full array
// shape in cells, ChunkShape is the per-axis chunk extent. Edge chunks are
// clipped to Shape when Shape is not an exact multiple of ChunkShape. The two
// slices must have equal length; for a 0-d (scalar) array both are empty and
// the grid holds exactly one chunk.
type ChunkGrid struct {
	Shape      []int64
	ChunkShape []int64
}

// NumChunks returns the total number of chunks in the grid (the product of the
// per-axis chunk counts). A 0-d grid has exactly one chunk.
func (g ChunkGrid) NumChunks() int64 {
	n := int64(1)
	for axis := range g.Shape {
		n *= g.ChunkCount(axis)
	}
	return n
}

// ChunkCount returns the number of chunks along the given axis: ceil(Shape /
// ChunkShape). A zero-size axis has zero chunks.
func (g ChunkGrid) ChunkCount(axis int) int64 {
	size := g.Shape[axis]
	cs := g.ChunkShape[axis]
	if size <= 0 {
		return 0
	}
	return (size + cs - 1) / cs
}

// ChunkBounds returns the half-open cell box [lo, hi) covered by the chunk at
// the given chunk coordinate, with edge chunks clipped to Shape. coord must
// have one entry per axis. For a 0-d grid coord is empty and both bounds are
// empty.
func (g ChunkGrid) ChunkBounds(coord []int64) (lo, hi []int64) {
	lo = make([]int64, len(g.Shape))
	hi = make([]int64, len(g.Shape))
	for axis := range g.Shape {
		cs := g.ChunkShape[axis]
		lo[axis] = coord[axis] * cs
		hi[axis] = lo[axis] + cs
		if hi[axis] > g.Shape[axis] {
			hi[axis] = g.Shape[axis]
		}
	}
	return lo, hi
}

// axisChunks returns the sorted, de-duplicated chunk indices along one axis
// that the selector touches, given the axis size and chunk extent. It assumes
// the selector has been validated.
func (g ChunkGrid) axisChunks(a AxisSel, size, cs int64) []int64 {
	switch {
	case a.Index != nil:
		return []int64{*a.Index / cs}
	case a.Slice != nil:
		s := *a.Slice
		if s.length() == 0 {
			return nil
		}
		// Largest selected position is the last step at or below Stop-1.
		last := s.Start + (s.length()-1)*s.Step
		var out []int64
		// Walk chunk by chunk; a chunk is touched if any stepped position
		// falls inside [chunkLo, chunkHi). Stepping over chunks directly is
		// cheap and avoids per-cell work.
		first := s.Start / cs
		lastChunk := last / cs
		for c := first; c <= lastChunk; c++ {
			lo := c * cs
			hi := lo + cs
			// First stepped position >= lo.
			if s.Start >= hi {
				continue
			}
			var p int64
			if s.Start >= lo {
				p = s.Start
			} else {
				// Smallest p = Start + k*Step with p >= lo.
				k := (lo - s.Start + s.Step - 1) / s.Step
				p = s.Start + k*s.Step
			}
			if p < hi && p <= last {
				out = append(out, c)
			}
		}
		return out
	case a.Indices != nil:
		var out []int64
		prevChunk := int64(-1)
		for _, idx := range a.Indices {
			c := idx / cs
			if c != prevChunk {
				out = append(out, c)
				prevChunk = c
			}
		}
		return out
	default: // All / zero value.
		n := g.chunkCountForSize(size, cs)
		out := make([]int64, n)
		for c := int64(0); c < n; c++ {
			out[c] = c
		}
		return out
	}
}

func (g ChunkGrid) chunkCountForSize(size, cs int64) int64 {
	if size <= 0 {
		return 0
	}
	return (size + cs - 1) / cs
}

// ChunkIter iterates chunk coordinates in row-major order. The slice returned
// by Next is reused across calls: it is only valid until the next Next call.
// Copy it if you need to retain it.
type ChunkIter struct {
	axes  [][]int64 // per-axis touched chunk indices, ascending
	idx   []int64   // current position into each axes[i]
	coord []int64   // reused output buffer
	done  bool
}

// Next advances the iterator. It returns the next chunk coordinate and true, or
// nil and false when the iteration is exhausted. The returned slice is only
// valid until the following Next call.
func (it *ChunkIter) Next() ([]int64, bool) {
	if it.done {
		return nil, false
	}
	// Materialize current coord from idx.
	for axis := range it.axes {
		it.coord[axis] = it.axes[axis][it.idx[axis]]
	}
	// Advance idx in row-major (last axis fastest).
	for axis := len(it.axes) - 1; axis >= 0; axis-- {
		it.idx[axis]++
		if it.idx[axis] < int64(len(it.axes[axis])) {
			break
		}
		it.idx[axis] = 0
		if axis == 0 {
			it.done = true
		}
	}
	if len(it.axes) == 0 {
		// 0-d grid: a single empty coordinate.
		it.done = true
	}
	return it.coord, true
}

// Intersecting returns an iterator over the chunk coordinates touched by the
// selection, in row-major order. A chunk is yielded if and only if it contains
// at least one selected cell. It assumes the selection has passed Validate
// against this grid's Shape.
func (g ChunkGrid) Intersecting(sel Selection) ChunkIter {
	axes := make([][]int64, len(g.Shape))
	empty := false
	for axis := range g.Shape {
		a := sel.axis(axis)
		ch := g.axisChunks(a, g.Shape[axis], g.ChunkShape[axis])
		if len(ch) == 0 {
			empty = true
		}
		axes[axis] = ch
	}
	it := ChunkIter{
		axes:  axes,
		idx:   make([]int64, len(g.Shape)),
		coord: make([]int64, len(g.Shape)),
	}
	if empty {
		// Any axis selecting no chunk means the whole selection is empty.
		it.done = true
	}
	return it
}

// Clip returns the selection intersected with the single chunk at coord,
// rebased to chunk-local coordinates (cell 0 of the result is cell lo of the
// chunk). The result has one AxisSel per array axis (Index axes stay Index so
// the axis-drop survives). It assumes the selection has passed Validate and
// that coord is a chunk the selection actually touches; for a non-touching
// chunk the returned per-axis selectors may be empty.
func (g ChunkGrid) Clip(coord []int64, sel Selection) Selection {
	if len(g.Shape) == 0 {
		// 0-d grid: nothing to clip, selection stays empty (= all of scalar).
		return Selection{Axes: []AxisSel{}}
	}
	lo, hi := g.ChunkBounds(coord)
	out := make([]AxisSel, len(g.Shape))
	for axis := range g.Shape {
		out[axis] = clipAxis(sel.axis(axis), lo[axis], hi[axis])
	}
	return Selection{Axes: out}
}

// clipAxis intersects one axis selector with the chunk's cell range [lo, hi)
// and rebases the result so position lo becomes local position 0.
func clipAxis(a AxisSel, lo, hi int64) AxisSel {
	switch {
	case a.Index != nil:
		idx := *a.Index
		if idx >= lo && idx < hi {
			local := idx - lo
			return AxisSel{Index: &local}
		}
		// Index not in this chunk: empty gather (no cells).
		return AxisSel{Indices: []int64{}}

	case a.Slice != nil:
		s := *a.Slice
		// Clip the stride to [lo, hi). First selected position >= lo.
		var start int64
		if s.Start >= lo {
			start = s.Start
		} else {
			k := (lo - s.Start + s.Step - 1) / s.Step
			start = s.Start + k*s.Step
		}
		stop := s.Stop
		if stop > hi {
			stop = hi
		}
		if start >= stop {
			return AxisSel{Slice: &Slice{Start: 0, Stop: 0, Step: s.Step}}
		}
		return AxisSel{Slice: &Slice{Start: start - lo, Stop: stop - lo, Step: s.Step}}

	case a.Indices != nil:
		var local []int64
		for _, idx := range a.Indices {
			if idx >= lo && idx < hi {
				local = append(local, idx-lo)
			}
		}
		if local == nil {
			local = []int64{}
		}
		return AxisSel{Indices: local}

	default: // All / zero value: whole axis clips to the chunk's local span.
		return AxisSel{Slice: &Slice{Start: 0, Stop: hi - lo, Step: 1}}
	}
}

// validateGrid is an internal sanity check used by tests and callers that want
// to assert the grid is well formed before use.
func (g ChunkGrid) validateGrid() error {
	if len(g.Shape) != len(g.ChunkShape) {
		return fmt.Errorf("carray: grid shape rank %d != chunk shape rank %d", len(g.Shape), len(g.ChunkShape))
	}
	for axis := range g.Shape {
		if g.Shape[axis] < 0 {
			return fmt.Errorf("carray: axis %d size %d < 0", axis, g.Shape[axis])
		}
		if g.ChunkShape[axis] < 1 {
			return fmt.Errorf("carray: axis %d chunk extent %d < 1", axis, g.ChunkShape[axis])
		}
	}
	return nil
}
