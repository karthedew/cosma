package compute

import (
	"fmt"
	"sync"
	"time"

	"github.com/apache/arrow/go/v18/arrow"
)

// GroupReduceParallel is the parallel version of GroupReduce. It partitions
// the flat row range [0, len(groupIDs)) into up to workers() equal-sized
// strips, reduces each strip independently, then merges the partial results.
//
// The two-phase approach is safe because Aggregates fields (Sum, Count,
// Min, Max) are commutative and associative: partial sums add, counts add,
// min/max take the extremum.
//
// Output: a []Aggregates of length numGroups in the same format as
// GroupReduce.
func GroupReduceParallel(groupIDs []int, numGroups int, chunked *arrow.Chunked) ([]Aggregates, error) {
	if chunked == nil {
		return nil, fmt.Errorf("compute.GroupReduceParallel: nil column")
	}
	if int64(len(groupIDs)) != int64(chunked.Len()) {
		return nil, fmt.Errorf("compute.GroupReduceParallel: %d group ids for %d rows", len(groupIDs), chunked.Len())
	}

	n := len(groupIDs)
	w := workers()
	if w > n {
		w = n
	}
	if w <= 1 {
		return GroupReduce(groupIDs, numGroups, chunked)
	}

	start := time.Now()

	// Build a flat row-indexed view of the chunked column so each worker can
	// access an arbitrary row range without chunk-boundary logic.
	flat, err := flattenChunked(chunked)
	if err != nil {
		return nil, fmt.Errorf("compute.GroupReduceParallel: %w", err)
	}

	// Divide rows into strips.
	strips := makeStrips(n, w)

	partials := make([][]Aggregates, len(strips))
	var wg sync.WaitGroup
	wg.Add(len(strips))

	for s, strip := range strips {
		s, strip := s, strip
		go func() {
			defer wg.Done()
			partials[s] = reduceStrip(flat, groupIDs, numGroups, strip.lo, strip.hi)
		}()
	}
	wg.Wait()

	// Merge partial results.
	merged := mergePartials(partials, numGroups, chunked.DataType())

	storeMetrics(OpMetrics{
		Op:      "groupby",
		Workers: len(strips),
		Rows:    int64(n),
		Elapsed: time.Since(start),
	})

	return merged, nil
}

// strip is a [lo, hi) row range.
type strip struct{ lo, hi int }

// makeStrips divides n rows into at most w equal-sized strips.
func makeStrips(n, w int) []strip {
	if w > n {
		w = n
	}
	strips := make([]strip, w)
	size := n / w
	rem := n % w
	lo := 0
	for i := range strips {
		hi := lo + size
		if i < rem {
			hi++
		}
		strips[i] = strip{lo: lo, hi: hi}
		lo = hi
	}
	return strips
}

// flatRow is a single row materialized from any numeric Arrow type.
// For non-numeric types only isNull and groupID are meaningful.
type flatRow struct {
	isNull bool
	i8     int8
	i16    int16
	i32    int32
	i64    int64
	u8     uint8
	u16    uint16
	u32    uint32
	u64    uint64
	f32    float32
	f64    float64
	str    string
}

// flatColumn holds per-row values for one typed column.
// We use a union-less approach: one typed slice per numeric kind so workers
// share the immutable data without allocation per row.
type flatColumn struct {
	dtype  arrow.Type
	nulls  []bool   // true == is null
	i64    []int64  // INT8/16/32/64 widened to int64
	u64    []uint64 // UINT8/16/32/64 widened to uint64
	f64    []float64
	str    []string
}

// flattenChunked materializes a chunked column into a flatColumn for random
// row access by worker goroutines. Each goroutine accesses a disjoint strip so
// no synchronization is needed after construction.
func flattenChunked(chunked *arrow.Chunked) (*flatColumn, error) {
	n := chunked.Len()
	fc := &flatColumn{dtype: chunked.DataType().ID(), nulls: make([]bool, n)}

	switch chunked.DataType().ID() {
	case arrow.INT8, arrow.INT16, arrow.INT32, arrow.INT64:
		fc.i64 = make([]int64, n)
		row := 0
		for _, ch := range chunked.Chunks() {
			for i := 0; i < ch.Len(); i++ {
				fc.nulls[row] = ch.IsNull(i)
				if !fc.nulls[row] {
					fc.i64[row] = intValue(ch, i)
				}
				row++
			}
		}
	case arrow.UINT8, arrow.UINT16, arrow.UINT32, arrow.UINT64:
		fc.u64 = make([]uint64, n)
		row := 0
		for _, ch := range chunked.Chunks() {
			for i := 0; i < ch.Len(); i++ {
				fc.nulls[row] = ch.IsNull(i)
				if !fc.nulls[row] {
					fc.u64[row] = uintValue(ch, i)
				}
				row++
			}
		}
	case arrow.FLOAT32, arrow.FLOAT64:
		fc.f64 = make([]float64, n)
		row := 0
		for _, ch := range chunked.Chunks() {
			for i := 0; i < ch.Len(); i++ {
				fc.nulls[row] = ch.IsNull(i)
				if !fc.nulls[row] {
					fc.f64[row] = floatValue(ch, i)
				}
				row++
			}
		}
	case arrow.STRING, arrow.LARGE_STRING:
		fc.str = make([]string, n)
		row := 0
		for _, ch := range chunked.Chunks() {
			for i := 0; i < ch.Len(); i++ {
				fc.nulls[row] = ch.IsNull(i)
				if !fc.nulls[row] {
					fc.str[row] = stringValue(ch, i)
				}
				row++
			}
		}
	default:
		// For types we cannot reduce numerically (e.g. BOOL, TIMESTAMP), we
		// still need nulls for count-only aggregations.
		row := 0
		for _, ch := range chunked.Chunks() {
			for i := 0; i < ch.Len(); i++ {
				fc.nulls[row] = ch.IsNull(i)
				row++
			}
		}
	}
	return fc, nil
}

// reduceStrip folds rows [lo, hi) from a flatColumn into per-group partial
// Aggregates. The result slice has length numGroups.
func reduceStrip(fc *flatColumn, groupIDs []int, numGroups, lo, hi int) []Aggregates {
	out := make([]Aggregates, numGroups)
	seen := make([]bool, numGroups)

	switch fc.dtype {
	case arrow.INT8, arrow.INT16, arrow.INT32, arrow.INT64:
		sums := make([]int64, numGroups)
		mins := make([]int64, numGroups)
		maxs := make([]int64, numGroups)
		for r := lo; r < hi; r++ {
			g := groupIDs[r]
			if fc.nulls[r] {
				continue
			}
			v := fc.i64[r]
			sums[g] += v
			if !seen[g] {
				mins[g], maxs[g], seen[g] = v, v, true
			} else {
				if v < mins[g] {
					mins[g] = v
				}
				if v > maxs[g] {
					maxs[g] = v
				}
			}
			out[g].Count++
		}
		for g := range out {
			if out[g].Count > 0 {
				out[g].Sum = sums[g]
				out[g].Min = mins[g]
				out[g].Max = maxs[g]
				out[g].Mean = float64(sums[g]) / float64(out[g].Count)
			}
		}

	case arrow.UINT8, arrow.UINT16, arrow.UINT32, arrow.UINT64:
		sums := make([]uint64, numGroups)
		mins := make([]uint64, numGroups)
		maxs := make([]uint64, numGroups)
		for r := lo; r < hi; r++ {
			g := groupIDs[r]
			if fc.nulls[r] {
				continue
			}
			v := fc.u64[r]
			sums[g] += v
			if !seen[g] {
				mins[g], maxs[g], seen[g] = v, v, true
			} else {
				if v < mins[g] {
					mins[g] = v
				}
				if v > maxs[g] {
					maxs[g] = v
				}
			}
			out[g].Count++
		}
		for g := range out {
			if out[g].Count > 0 {
				out[g].Sum = sums[g]
				out[g].Min = mins[g]
				out[g].Max = maxs[g]
				out[g].Mean = float64(sums[g]) / float64(out[g].Count)
			}
		}

	case arrow.FLOAT32, arrow.FLOAT64:
		sums := make([]float64, numGroups)
		mins := make([]float64, numGroups)
		maxs := make([]float64, numGroups)
		for r := lo; r < hi; r++ {
			g := groupIDs[r]
			if fc.nulls[r] {
				continue
			}
			v := fc.f64[r]
			sums[g] += v
			if !seen[g] {
				mins[g], maxs[g], seen[g] = v, v, true
			} else {
				if v < mins[g] {
					mins[g] = v
				}
				if v > maxs[g] {
					maxs[g] = v
				}
			}
			out[g].Count++
		}
		for g := range out {
			if out[g].Count > 0 {
				out[g].Sum = sums[g]
				out[g].Min = mins[g]
				out[g].Max = maxs[g]
				out[g].Mean = sums[g] / float64(out[g].Count)
			}
		}

	case arrow.STRING, arrow.LARGE_STRING:
		mins := make([]string, numGroups)
		maxs := make([]string, numGroups)
		for r := lo; r < hi; r++ {
			g := groupIDs[r]
			if fc.nulls[r] {
				continue
			}
			v := fc.str[r]
			if !seen[g] {
				mins[g], maxs[g], seen[g] = v, v, true
			} else {
				if v < mins[g] {
					mins[g] = v
				}
				if v > maxs[g] {
					maxs[g] = v
				}
			}
			out[g].Count++
		}
		for g := range out {
			if out[g].Count > 0 {
				out[g].Min = mins[g]
				out[g].Max = maxs[g]
			}
		}

	default:
		// Count-only for unsupported types.
		for r := lo; r < hi; r++ {
			if !fc.nulls[r] {
				out[groupIDs[r]].Count++
			}
		}
	}
	return out
}

// mergePartials combines per-strip partial Aggregates into the final result.
// Each group's partials are merged sequentially; the merge is O(strips *
// numGroups) and is not parallelized (strips is small, numGroups is bounded
// by data cardinality).
func mergePartials(partials [][]Aggregates, numGroups int, dtype arrow.DataType) []Aggregates {
	out := make([]Aggregates, numGroups)
	seen := make([]bool, numGroups)

	for _, part := range partials {
		for g, p := range part {
			if p.Count == 0 {
				continue
			}
			o := &out[g]
			o.Count += p.Count

			switch dtype.ID() {
			case arrow.INT8, arrow.INT16, arrow.INT32, arrow.INT64:
				ps, pm, px := p.Sum.(int64), p.Min.(int64), p.Max.(int64)
				if !seen[g] {
					o.Sum = ps
					o.Min = pm
					o.Max = px
					seen[g] = true
				} else {
					o.Sum = o.Sum.(int64) + ps
					if pm < o.Min.(int64) {
						o.Min = pm
					}
					if px > o.Max.(int64) {
						o.Max = px
					}
				}
			case arrow.UINT8, arrow.UINT16, arrow.UINT32, arrow.UINT64:
				ps, pm, px := p.Sum.(uint64), p.Min.(uint64), p.Max.(uint64)
				if !seen[g] {
					o.Sum = ps
					o.Min = pm
					o.Max = px
					seen[g] = true
				} else {
					o.Sum = o.Sum.(uint64) + ps
					if pm < o.Min.(uint64) {
						o.Min = pm
					}
					if px > o.Max.(uint64) {
						o.Max = px
					}
				}
			case arrow.FLOAT32, arrow.FLOAT64:
				ps, pm, px := p.Sum.(float64), p.Min.(float64), p.Max.(float64)
				if !seen[g] {
					o.Sum = ps
					o.Min = pm
					o.Max = px
					seen[g] = true
				} else {
					o.Sum = o.Sum.(float64) + ps
					if pm < o.Min.(float64) {
						o.Min = pm
					}
					if px > o.Max.(float64) {
						o.Max = px
					}
				}
			case arrow.STRING, arrow.LARGE_STRING:
				pm, px := p.Min.(string), p.Max.(string)
				if !seen[g] {
					o.Min = pm
					o.Max = px
					seen[g] = true
				} else {
					if pm < o.Min.(string) {
						o.Min = pm
					}
					if px > o.Max.(string) {
						o.Max = px
					}
				}
			}
		}
	}

	// Recompute means from merged sums/counts.
	for g := range out {
		if out[g].Count == 0 {
			continue
		}
		switch dtype.ID() {
		case arrow.INT8, arrow.INT16, arrow.INT32, arrow.INT64:
			out[g].Mean = float64(out[g].Sum.(int64)) / float64(out[g].Count)
		case arrow.UINT8, arrow.UINT16, arrow.UINT32, arrow.UINT64:
			out[g].Mean = float64(out[g].Sum.(uint64)) / float64(out[g].Count)
		case arrow.FLOAT32, arrow.FLOAT64:
			out[g].Mean = out[g].Sum.(float64) / float64(out[g].Count)
		}
	}

	return out
}

// ---------------------------------------------------------------------------
// Type-erased array accessors (avoid re-importing array sub-package)
// ---------------------------------------------------------------------------

// intValue reads the int64-widened value at position i from any signed integer
// Arrow array. It panics if arr is not a signed integer array.
func intValue(arr arrow.Array, i int) int64 {
	switch a := arr.(type) {
	case interface{ Value(int) int8 }:
		return int64(a.Value(i))
	case interface{ Value(int) int16 }:
		return int64(a.Value(i))
	case interface{ Value(int) int32 }:
		return int64(a.Value(i))
	case interface{ Value(int) int64 }:
		return a.Value(i)
	}
	panic(fmt.Sprintf("intValue: unsupported type %T", arr))
}

func uintValue(arr arrow.Array, i int) uint64 {
	switch a := arr.(type) {
	case interface{ Value(int) uint8 }:
		return uint64(a.Value(i))
	case interface{ Value(int) uint16 }:
		return uint64(a.Value(i))
	case interface{ Value(int) uint32 }:
		return uint64(a.Value(i))
	case interface{ Value(int) uint64 }:
		return a.Value(i)
	}
	panic(fmt.Sprintf("uintValue: unsupported type %T", arr))
}

func floatValue(arr arrow.Array, i int) float64 {
	switch a := arr.(type) {
	case interface{ Value(int) float32 }:
		return float64(a.Value(i))
	case interface{ Value(int) float64 }:
		return a.Value(i)
	}
	panic(fmt.Sprintf("floatValue: unsupported type %T", arr))
}

func stringValue(arr arrow.Array, i int) string {
	switch a := arr.(type) {
	case interface{ Value(int) string }:
		return a.Value(i)
	}
	panic(fmt.Sprintf("stringValue: unsupported type %T", arr))
}
