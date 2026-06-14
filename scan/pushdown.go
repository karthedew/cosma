package scan

import (
	"github.com/karthedew/cosma/carray"
	"github.com/karthedew/cosma/expr"
)

// pushdown.go translates Z1's advisory ScanHints into a tightened
// carray.Selection. The contract is strictly advisory: the surviving Filter /
// Project / Limit nodes above the scan keep the result exact, so tightening here
// may only ever PRUNE work (fewer chunks, fewer output columns) — it must never
// drop a cell the predicate keeps. When in doubt we tighten less.
//
// What is honored:
//   - Filters: each conjunct of the form `col <op> lit` / `lit <op> col` with
//     op in {Eq,Lt,Lte,Gt,Gte} and col a KEPT index column → that axis's
//     AxisSel is intersected with the implied half-open int64 range. Predicates
//     on the value column, on dropped (Index) axes, or in any other shape are
//     ignored.
//   - Columns: unreferenced KEPT index columns are dropped from OUTPUT only; the
//     value column is always produced and the set of cells read never changes.
//   - Limit: surfaced to the executor, which stops the chunk iterator once
//     emitted rows reach it (best-effort; an exact PhysLimit sits above).

// pushdownResult bundles the three independent outcomes of translating hints.
type pushdownResult struct {
	// sel is the original ArrayScan.Selection tightened by recognized range
	// predicates. It is always valid against the array shape (no tightening can
	// produce an invalid selector — only narrower ranges).
	sel carray.Selection
	// keepOutput[i] reports whether kept index column i (positional over the kept
	// dims) survives projection into the output. The value column is always kept.
	keepOutput []bool
	// limit mirrors ScanHints.Limit (-1 == none). Best-effort row budget.
	limit int64
}

// applyPushdown tightens base by the recognized hints and resolves the output
// column projection. cols are the names of the KEPT index columns in output
// order (one per non-Index axis), axisOfCol[i] is the array-axis index that
// kept-column i corresponds to, and shape is the full array shape. base must
// already be valid against shape.
func applyPushdown(
	hints dataframeScanHints,
	base carray.Selection,
	cols []string,
	axisOfCol []int,
	shape []int64,
) pushdownResult {
	res := pushdownResult{
		sel:        cloneSelection(base, shape),
		keepOutput: make([]bool, len(cols)),
		limit:      hints.Limit,
	}
	for i := range res.keepOutput {
		res.keepOutput[i] = true
	}

	// Projection: drop unreferenced kept index columns from output only.
	if len(hints.Columns) > 0 {
		want := make(map[string]bool, len(hints.Columns))
		for _, c := range hints.Columns {
			want[c] = true
		}
		for i, name := range cols {
			res.keepOutput[i] = want[name]
		}
	}

	// Filter pushdown: tighten axis ranges for honorable conjuncts.
	colAxis := make(map[string]int, len(cols))
	for i, name := range cols {
		colAxis[name] = axisOfCol[i]
	}
	for _, conj := range splitConjunctsAll(hints.Filters) {
		name, lo, hi, ok := rangePredicate(conj.Node)
		if !ok {
			continue
		}
		axis, isKept := colAxis[name]
		if !isKept {
			continue // value column, dropped Index axis, or unknown name.
		}
		res.sel.Axes[axis] = intersectAxisRange(res.sel.Axes[axis], shape[axis], lo, hi)
	}

	return res
}

// dataframeScanHints mirrors dataframe.ScanHints. It is declared here so this
// file's helpers are testable without constructing a full LazyFrame; array.go
// converts the real hints into this shape at Open time.
type dataframeScanHints struct {
	Filters []expr.Expr
	Columns []string
	Limit   int64
}

// splitConjunctsAll flattens every top-level filter (themselves ANDed together)
// into the union of their conjuncts.
func splitConjunctsAll(filters []expr.Expr) []expr.Expr {
	var out []expr.Expr
	for _, f := range filters {
		out = append(out, splitConjuncts(f)...)
	}
	return out
}

// splitConjuncts flattens an And tree into its conjuncts. A non-And expression
// is returned as a single conjunct. The traversal is iterative over a stack so
// deeply nested And chains do not recurse without bound.
func splitConjuncts(pred expr.Expr) []expr.Expr {
	if pred.Node == nil {
		return nil
	}
	var out []expr.Expr
	stack := []expr.ExprNode{pred.Node}
	for len(stack) > 0 {
		n := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if b, ok := n.(expr.BinaryNode); ok && b.Op == expr.BinaryOpAnd {
			// Push right then left so left is processed first (output order
			// matches reading order, which only matters for determinism).
			stack = append(stack, b.Right, b.Left)
			continue
		}
		out = append(out, expr.Expr{Node: n})
	}
	return out
}

// rangePredicate recognizes a single comparison of a column against an integer
// literal and returns the implied half-open keep-range [lo, hi) for that
// column's axis, plus the column name. It accepts both `col <op> lit` and
// `lit <op> col` (flipping the op for the latter). op must be one of
// Eq/Lt/Lte/Gt/Gte. ok is false for any shape it does not understand.
//
// lo/hi are clamped to the open int64 range sentinels: lo defaults to
// minInt64Range and hi to maxInt64Range, so a one-sided predicate leaves the
// other bound wide open. intersectAxisRange clamps to the real axis size.
func rangePredicate(n expr.ExprNode) (col string, lo, hi int64, ok bool) {
	b, isBin := n.(expr.BinaryNode)
	if !isBin {
		return "", 0, 0, false
	}

	op := b.Op
	colName, litVal, swapped, found := columnLiteralOperands(b.Left, b.Right)
	if !found {
		return "", 0, 0, false
	}
	if swapped {
		op = flipOp(op)
	}

	v, isInt := asInt64(litVal)
	if !isInt {
		return "", 0, 0, false
	}

	lo, hi = minInt64Range, maxInt64Range
	switch op {
	case expr.BinaryOpEq:
		lo, hi = v, v+1
	case expr.BinaryOpLt:
		hi = v
	case expr.BinaryOpLte:
		hi = v + 1
	case expr.BinaryOpGt:
		lo = v + 1
	case expr.BinaryOpGte:
		lo = v
	default:
		return "", 0, 0, false
	}
	return colName, lo, hi, true
}

// columnLiteralOperands identifies which operand is a column and which is a
// literal. swapped is true when the literal was on the left (so the caller flips
// the operator). found is false unless exactly one operand is a ColumnNode and
// the other a LiteralNode.
func columnLiteralOperands(left, right expr.ExprNode) (col string, lit any, swapped, found bool) {
	if c, ok := left.(expr.ColumnNode); ok {
		if l, ok := right.(expr.LiteralNode); ok {
			return c.Name, l.Value, false, true
		}
	}
	if c, ok := right.(expr.ColumnNode); ok {
		if l, ok := left.(expr.LiteralNode); ok {
			return c.Name, l.Value, true, true
		}
	}
	return "", nil, false, false
}

// flipOp mirrors a comparison operator across its operands (a < b <=> b > a).
func flipOp(op expr.BinaryOp) expr.BinaryOp {
	switch op {
	case expr.BinaryOpLt:
		return expr.BinaryOpGt
	case expr.BinaryOpLte:
		return expr.BinaryOpGte
	case expr.BinaryOpGt:
		return expr.BinaryOpLt
	case expr.BinaryOpGte:
		return expr.BinaryOpLte
	default:
		return op // Eq is symmetric.
	}
}

// asInt64 coerces a literal value to int64 when it has no fractional part. Float
// literals are accepted only when integral so `time <= 23.0` still prunes;
// fractional literals on an integer index axis are ignored (return false) rather
// than rounded, keeping the contract conservative.
func asInt64(v any) (int64, bool) {
	switch x := v.(type) {
	case int64:
		return x, true
	case int32:
		return int64(x), true
	case int16:
		return int64(x), true
	case int8:
		return int64(x), true
	case int:
		return int64(x), true
	case uint64:
		return int64(x), true
	case uint32:
		return int64(x), true
	case uint16:
		return int64(x), true
	case uint8:
		return int64(x), true
	case uint:
		return int64(x), true
	case float64:
		if x == float64(int64(x)) {
			return int64(x), true
		}
		return 0, false
	case float32:
		if x == float32(int64(x)) {
			return int64(x), true
		}
		return 0, false
	default:
		return 0, false
	}
}

// minInt64Range / maxInt64Range are the open sentinels for an unbounded side of
// a half-open keep-range. They are clamped to [0, size) by intersectAxisRange.
const (
	minInt64Range int64 = -1 << 62
	maxInt64Range int64 = 1 << 62
)

// intersectAxisRange narrows an axis selector to keep only positions in the
// half-open [lo, hi) range, clamped to [0, size). It is conservative: it only
// tightens shapes it can tighten exactly and otherwise returns the input
// unchanged (never dropping a kept cell).
//
//   - All / zero value → a Slice over the clamped [lo, hi) (step 1).
//   - Slice (step 1)   → its [Start, Stop) intersected with [lo, hi).
//   - Slice (step >1)  → left unchanged (range-clamping a strided slice can be
//     done but the saved chunks rarely justify the off-by-one risk; conservative
//     is correct).
//   - Index            → left unchanged (single cell; the Filter keeps it exact
//     and the axis is dropped from output anyway).
//   - Indices          → keep only gathered positions inside [lo, hi).
func intersectAxisRange(a carray.AxisSel, size, lo, hi int64) carray.AxisSel {
	// Clamp the keep-range to the real axis extent.
	if lo < 0 {
		lo = 0
	}
	if hi > size {
		hi = size
	}
	if lo > size {
		lo = size
	}
	if hi < 0 {
		hi = 0
	}

	switch {
	case a.Index != nil:
		return a // Conservative: leave single-index axes alone.

	case a.Indices != nil:
		kept := make([]int64, 0, len(a.Indices))
		for _, idx := range a.Indices {
			if idx >= lo && idx < hi {
				kept = append(kept, idx)
			}
		}
		return carray.AxisSel{Indices: kept}

	case a.Slice != nil:
		s := *a.Slice
		if s.Step != 1 {
			return a // Conservative for strided slices.
		}
		start, stop := s.Start, s.Stop
		if lo > start {
			start = lo
		}
		if hi < stop {
			stop = hi
		}
		if stop < start {
			stop = start
		}
		return carray.AxisSel{Slice: &carray.Slice{Start: start, Stop: stop, Step: 1}}

	default: // All / zero value: the whole axis tightens to the clamped range.
		if lo > hi {
			lo = hi
		}
		return carray.AxisSel{Slice: &carray.Slice{Start: lo, Stop: hi, Step: 1}}
	}
}

// cloneSelection makes a deep-enough copy of sel (sized to shape) so tightening
// does not mutate the caller's ArrayScan.Selection. A nil/short Axes slice is
// materialized to a full per-axis "all" selection so every axis is addressable.
func cloneSelection(sel carray.Selection, shape []int64) carray.Selection {
	axes := make([]carray.AxisSel, len(shape))
	for i := range shape {
		var src carray.AxisSel
		if i < len(sel.Axes) {
			src = sel.Axes[i]
		}
		axes[i] = cloneAxis(src)
	}
	return carray.Selection{Axes: axes}
}

func cloneAxis(a carray.AxisSel) carray.AxisSel {
	out := carray.AxisSel{All: a.All}
	if a.Index != nil {
		v := *a.Index
		out.Index = &v
	}
	if a.Slice != nil {
		s := *a.Slice
		out.Slice = &s
	}
	if a.Indices != nil {
		idx := make([]int64, len(a.Indices))
		copy(idx, a.Indices)
		out.Indices = idx
	}
	return out
}
