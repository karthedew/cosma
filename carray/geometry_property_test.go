package carray

import (
	"fmt"
	"sort"
	"strings"
	"testing"
)

// ---- Brute-force reference machinery -------------------------------------

// cellKey turns a global cell coordinate into a comparable map key.
func cellKey(coord []int64) string {
	var b strings.Builder
	for i, c := range coord {
		if i > 0 {
			b.WriteByte(',')
		}
		fmt.Fprintf(&b, "%d", c)
	}
	return b.String()
}

// oraclePositions enumerates an axis selector's positions from first
// principles: walk every position on the axis and test membership against the
// documented semantics of each variant. It deliberately does not call
// AxisSel.positions — that method is production code (the cell scan consumes
// it), so the oracle must not share its arithmetic.
func oraclePositions(a AxisSel, size int64) []int64 {
	member := func(p int64) bool {
		switch {
		case a.Index != nil:
			return p == *a.Index
		case a.Slice != nil:
			s := *a.Slice
			return p >= s.Start && p < s.Stop && (p-s.Start)%s.Step == 0
		case a.Indices != nil:
			for _, idx := range a.Indices {
				if idx == p {
					return true
				}
			}
			return false
		default: // All / zero value.
			return true
		}
	}
	var out []int64
	for p := int64(0); p < size; p++ {
		if member(p) {
			out = append(out, p)
		}
	}
	return out
}

// bruteCells enumerates, by the orthogonal cartesian product of each axis's
// selected positions, the full set of global cells a selection picks. This is
// the independent oracle the geometry code is checked against.
func bruteCells(sel Selection, shape []int64) map[string]bool {
	perAxis := make([][]int64, len(shape))
	for axis := range shape {
		perAxis[axis] = oraclePositions(sel.axis(axis), shape[axis])
		if len(perAxis[axis]) == 0 {
			return map[string]bool{} // Empty along one axis ⇒ empty overall.
		}
	}
	cells := map[string]bool{}
	coord := make([]int64, len(shape))
	var rec func(axis int)
	rec = func(axis int) {
		if axis == len(shape) {
			cells[cellKey(coord)] = true
			return
		}
		for _, p := range perAxis[axis] {
			coord[axis] = p
			rec(axis + 1)
		}
	}
	if len(shape) == 0 {
		cells[""] = true // 0-d scalar: the single cell.
	} else {
		rec(0)
	}
	return cells
}

// bruteResultShape derives the result shape directly from the enumeration by
// counting distinct positions per non-dropped axis, independent of
// ResultShape's implementation.
func bruteResultShape(sel Selection, shape []int64) []int64 {
	var out []int64
	for axis := range shape {
		a := sel.axis(axis)
		if a.Index != nil {
			continue
		}
		out = append(out, int64(len(oraclePositions(a, shape[axis]))))
	}
	if out == nil {
		out = []int64{}
	}
	return out
}

// chunksFromCells computes, by brute force, the set of chunk coordinates that
// contain at least one selected cell.
func chunksFromCells(g ChunkGrid, cells map[string]bool, shape []int64) map[string]bool {
	out := map[string]bool{}
	for key := range cells {
		if key == "" { // 0-d.
			out[""] = true
			continue
		}
		parts := strings.Split(key, ",")
		coord := make([]int64, len(parts))
		for i, p := range parts {
			var v int64
			fmt.Sscanf(p, "%d", &v)
			coord[i] = v / g.ChunkShape[i]
		}
		out[cellKey(coord)] = true
	}
	return out
}

// ---- Selection generation ------------------------------------------------

// axisSelectors enumerates a wide, systematic set of AxisSel variants for an
// axis of the given size: All, every Index, a spread of Slices (varied
// Start/Stop/Step including Step>1 and empty), and several monotonic Indices
// gathers.
func axisSelectors(size int64) []AxisSel {
	var out []AxisSel
	out = append(out, AxisSel{All: true})
	out = append(out, AxisSel{}) // zero value == all

	for i := int64(0); i < size; i++ {
		idx := i
		out = append(out, AxisSel{Index: &idx})
	}

	for start := int64(0); start <= size; start++ {
		for stop := start; stop <= size; stop++ {
			for _, step := range []int64{1, 2, 3} {
				s := Slice{Start: start, Stop: stop, Step: step}
				out = append(out, AxisSel{Slice: &s})
			}
		}
	}

	// Gather selectors: a few strictly-increasing subsets.
	if size >= 1 {
		out = append(out, AxisSel{Indices: []int64{0}})
	}
	if size >= 2 {
		out = append(out, AxisSel{Indices: []int64{0, size - 1}})
	}
	if size >= 3 {
		out = append(out, AxisSel{Indices: []int64{0, size / 2, size - 1}})
		out = append(out, AxisSel{Indices: []int64{1, size - 1}})
	}
	if size >= 4 {
		var all []int64
		for i := int64(0); i < size; i += 2 {
			all = append(all, i)
		}
		out = append(out, AxisSel{Indices: all})
	}
	return out
}

// ---- Core property test --------------------------------------------------

// checkSelection runs the full agreement battery for one (grid, selection):
// ResultShape, Intersecting-chunk set, and the Clip-reconstructed cell set.
func checkSelection(t *testing.T, g ChunkGrid, sel Selection) {
	t.Helper()
	shape := g.Shape

	if err := sel.Validate(shape); err != nil {
		t.Fatalf("Validate rejected a generated-valid selection %v: %v", sel, err)
	}

	brute := bruteCells(sel, shape)

	// (a) ResultShape matches the enumeration's shape.
	gotShape := sel.ResultShape(shape)
	wantShape := bruteResultShape(sel, shape)
	if !equalInts(gotShape, wantShape) {
		t.Fatalf("ResultShape=%v want %v (sel=%s shape=%v)", gotShape, wantShape, fmtSel(sel), shape)
	}

	// The number of selected cells implied by the result shape must match the
	// enumeration count (product of result-shape extents, times dropped axes
	// which each contribute 1).
	expectCount := int64(1)
	for axis := range shape {
		a := sel.axis(axis)
		expectCount *= int64(len(a.positions(shape[axis])))
	}
	if int64(len(brute)) != expectCount {
		t.Fatalf("brute cell count %d != product-of-positions %d (sel=%s)", len(brute), expectCount, fmtSel(sel))
	}

	// (c) Intersecting yields exactly the chunks containing a selected cell.
	wantChunks := chunksFromCells(g, brute, shape)
	gotChunks := map[string]bool{}
	var chunkCoords [][]int64
	it := g.Intersecting(sel)
	for {
		coord, ok := it.Next()
		if !ok {
			break
		}
		// Honor the reuse contract: copy before retaining.
		cp := append([]int64(nil), coord...)
		key := cellKey(cp)
		if gotChunks[key] {
			t.Fatalf("Intersecting yielded duplicate chunk %v (sel=%s)", cp, fmtSel(sel))
		}
		gotChunks[key] = true
		chunkCoords = append(chunkCoords, cp)
	}
	if !equalStringSets(gotChunks, wantChunks) {
		t.Fatalf("Intersecting chunks %v want %v (sel=%s shape=%v chunk=%v)",
			sortedKeys(gotChunks), sortedKeys(wantChunks), fmtSel(sel), shape, g.ChunkShape)
	}

	// Row-major ordering of the yielded chunk coords.
	assertRowMajor(t, chunkCoords, sel)

	// (b) Union over Intersecting chunks of Clip-selected cells (mapped back to
	// global coords) equals exactly the brute-force set.
	recon := map[string]bool{}
	for _, coord := range chunkCoords {
		lo, _ := g.ChunkBounds(coord)
		local := g.Clip(coord, sel)
		// Clip output is chunk-local; enumerate it over the chunk-local space.
		localShape := chunkLocalShape(g, coord)
		if err := local.Validate(localShape); err != nil {
			t.Fatalf("Clip produced invalid local selection %s for chunk %v: %v", fmtSel(local), coord, err)
		}
		localCells := bruteCells(local, localShape)
		for k := range localCells {
			if k == "" { // 0-d.
				recon[""] = true
				continue
			}
			parts := strings.Split(k, ",")
			gcoord := make([]int64, len(parts))
			for i, p := range parts {
				var v int64
				fmt.Sscanf(p, "%d", &v)
				gcoord[i] = v + lo[i]
			}
			recon[cellKey(gcoord)] = true
		}
	}
	if !equalStringSets(recon, brute) {
		t.Fatalf("Clip-reconstructed cells differ from brute force (sel=%s shape=%v chunk=%v)\n got=%v\nwant=%v",
			fmtSel(sel), shape, g.ChunkShape, sortedKeys(recon), sortedKeys(brute))
	}
}

// chunkLocalShape returns the cell extents of a chunk (edge-clipped).
func chunkLocalShape(g ChunkGrid, coord []int64) []int64 {
	lo, hi := g.ChunkBounds(coord)
	out := make([]int64, len(lo))
	for i := range lo {
		out[i] = hi[i] - lo[i]
	}
	return out
}

// assertRowMajor verifies the chunk coords are strictly ascending in row-major
// order (last axis fastest).
func assertRowMajor(t *testing.T, coords [][]int64, sel Selection) {
	t.Helper()
	for i := 1; i < len(coords); i++ {
		if compareRowMajor(coords[i-1], coords[i]) >= 0 {
			t.Fatalf("Intersecting not row-major ascending: %v then %v (sel=%s)", coords[i-1], coords[i], fmtSel(sel))
		}
	}
}

func compareRowMajor(a, b []int64) int {
	for i := range a {
		if a[i] < b[i] {
			return -1
		}
		if a[i] > b[i] {
			return 1
		}
	}
	return 0
}

// ---- The property test driver --------------------------------------------

func TestGeometryProperty(t *testing.T) {
	// A spread of grids: 1-4 axes, sizes up to 7, dividing and non-dividing
	// chunk shapes, single-chunk and many-chunk.
	grids := buildGrids()

	total := 0
	for _, g := range grids {
		if err := g.validateGrid(); err != nil {
			t.Fatalf("bad grid %v/%v: %v", g.Shape, g.ChunkShape, err)
		}
		// Build the cartesian product of per-axis selectors. To keep the
		// product tractable on 3-4 axes, cap selectors per axis there.
		perAxis := make([][]AxisSel, len(g.Shape))
		for axis := range g.Shape {
			sels := axisSelectors(g.Shape[axis])
			perAxis[axis] = capSelectors(sels, len(g.Shape))
		}
		choose := make([]int, len(g.Shape))
		for {
			axes := make([]AxisSel, len(g.Shape))
			for axis := range g.Shape {
				axes[axis] = perAxis[axis][choose[axis]]
			}
			checkSelection(t, g, Selection{Axes: axes})
			total++

			// Advance the odometer.
			done := true
			for axis := len(choose) - 1; axis >= 0; axis-- {
				choose[axis]++
				if choose[axis] < len(perAxis[axis]) {
					done = false
					break
				}
				choose[axis] = 0
			}
			if done {
				break
			}
		}
		// Also exercise the nil-Axes "select all" form.
		checkSelection(t, g, Selection{})
	}
	t.Logf("checked %d (grid, selection) pairs", total)
}

// capSelectors trims the selector list for higher-rank grids so the cartesian
// product stays in the millions rather than billions, while still keeping a
// representative spread (All, an Index, dividing & non-dividing slices with
// step>1, a gather).
func capSelectors(sels []AxisSel, rank int) []AxisSel {
	if rank <= 2 {
		return sels // full sweep for 1-2 axes
	}
	limit := 14
	if rank == 4 {
		limit = 8
	}
	if len(sels) <= limit {
		return sels
	}
	// Deterministic stride sample that always keeps the first (All) entry.
	out := make([]AxisSel, 0, limit)
	stride := len(sels) / limit
	if stride < 1 {
		stride = 1
	}
	for i := 0; i < len(sels) && len(out) < limit; i += stride {
		out = append(out, sels[i])
	}
	return out
}

func buildGrids() []ChunkGrid {
	var grids []ChunkGrid
	// 1-D.
	for _, size := range []int64{1, 5, 7} {
		for _, cs := range []int64{1, 2, 3, size} {
			if cs < 1 {
				continue
			}
			grids = append(grids, ChunkGrid{Shape: []int64{size}, ChunkShape: []int64{cs}})
		}
	}
	// 2-D, dividing and non-dividing.
	twoD := [][2][2]int64{
		{{6, 5}, {2, 2}}, // non-dividing both
		{{6, 4}, {3, 2}}, // dividing
		{{7, 7}, {3, 3}}, // non-dividing both
		{{4, 6}, {4, 6}}, // single chunk
		{{5, 5}, {1, 5}}, // 1-wide rows
		{{7, 4}, {2, 3}}, // mixed
	}
	for _, d := range twoD {
		grids = append(grids, ChunkGrid{Shape: []int64{d[0][0], d[0][1]}, ChunkShape: []int64{d[1][0], d[1][1]}})
	}
	// 3-D.
	grids = append(grids,
		ChunkGrid{Shape: []int64{4, 5, 3}, ChunkShape: []int64{2, 2, 2}},
		ChunkGrid{Shape: []int64{3, 3, 3}, ChunkShape: []int64{3, 3, 3}}, // single chunk
		ChunkGrid{Shape: []int64{5, 4, 4}, ChunkShape: []int64{2, 3, 1}},
	)
	// 4-D.
	grids = append(grids,
		ChunkGrid{Shape: []int64{3, 4, 2, 3}, ChunkShape: []int64{2, 2, 1, 2}},
		ChunkGrid{Shape: []int64{2, 2, 2, 2}, ChunkShape: []int64{1, 1, 1, 1}},
	)
	return grids
}

// ---- small helpers -------------------------------------------------------

func equalInts(a, b []int64) bool {
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

func equalStringSets(a, b map[string]bool) bool {
	if len(a) != len(b) {
		return false
	}
	for k := range a {
		if !b[k] {
			return false
		}
	}
	return true
}

func sortedKeys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func fmtSel(sel Selection) string {
	var b strings.Builder
	b.WriteByte('[')
	for i, a := range sel.Axes {
		if i > 0 {
			b.WriteByte(' ')
		}
		switch {
		case a.Index != nil:
			fmt.Fprintf(&b, "I%d", *a.Index)
		case a.Slice != nil:
			fmt.Fprintf(&b, "S%d:%d:%d", a.Slice.Start, a.Slice.Stop, a.Slice.Step)
		case a.Indices != nil:
			fmt.Fprintf(&b, "G%v", a.Indices)
		default:
			b.WriteString("*")
		}
	}
	b.WriteByte(']')
	return b.String()
}
