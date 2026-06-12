package carray

import (
	"fmt"
)

// Slice is a half-open range [Start, Stop) over an axis with a positive Step.
// Step must be >= 1. The selected positions are Start, Start+Step, Start+2*Step,
// ... up to but not including Stop. A slice with Start == Stop selects nothing.
type Slice struct {
	Start int64
	Stop  int64
	Step  int64
}

// length returns the number of positions the slice selects. It assumes the
// slice has already been validated (Step >= 1, Start <= Stop, in-bounds).
func (s Slice) length() int64 {
	if s.Stop <= s.Start {
		return 0
	}
	span := s.Stop - s.Start
	// Ceiling division of span by Step.
	return (span + s.Step - 1) / s.Step
}

// AxisSel is a one-of describing how a single axis is selected. Exactly one of
// the variants is active; a zero-value AxisSel (no variant set) selects the
// entire axis, identical to All == true.
//
//   - All == true            select the whole axis.
//   - Index != nil           select a single position; the axis is dropped
//     from the result shape.
//   - Slice != nil           select a strided half-open range.
//   - Indices != nil         gather an explicit, strictly increasing list of
//     positions.
type AxisSel struct {
	All     bool
	Index   *int64
	Slice   *Slice
	Indices []int64
}

// isAll reports whether the axis selector denotes the whole axis. The zero
// value (no variant set) counts as all.
func (a AxisSel) isAll() bool {
	if a.Index != nil || a.Slice != nil || a.Indices != nil {
		return false
	}
	return true // All == true, or nothing set.
}

// positions returns the selected positions along an axis of the given size,
// in ascending order. It assumes the selector has been validated.
func (a AxisSel) positions(size int64) []int64 {
	switch {
	case a.Index != nil:
		return []int64{*a.Index}
	case a.Slice != nil:
		s := *a.Slice
		out := make([]int64, 0, s.length())
		for p := s.Start; p < s.Stop; p += s.Step {
			out = append(out, p)
		}
		return out
	case a.Indices != nil:
		out := make([]int64, len(a.Indices))
		copy(out, a.Indices)
		return out
	default: // All / zero value.
		out := make([]int64, size)
		for i := int64(0); i < size; i++ {
			out[i] = i
		}
		return out
	}
}

// resultLen returns the number of positions selected along an axis of the
// given size. Index axes contribute no entry to the result shape and are
// handled by ResultShape, not here.
func (a AxisSel) resultLen(size int64) int64 {
	switch {
	case a.Index != nil:
		return 1 // Caller drops the axis; length is 1 cell.
	case a.Slice != nil:
		return a.Slice.length()
	case a.Indices != nil:
		return int64(len(a.Indices))
	default:
		return size
	}
}

// Selection describes an orthogonal, per-axis selection over an array. Axes is
// indexed positionally; the zero value (nil Axes) selects every cell. A nil or
// zero-value AxisSel entry selects the whole corresponding axis.
type Selection struct {
	Axes []AxisSel
}

// axis returns the selector for axis i, treating a short or nil Axes slice as
// "all" for the missing axes. This is only safe after Validate has confirmed
// that a non-nil Axes slice matches the shape length.
func (sel Selection) axis(i int) AxisSel {
	if i < len(sel.Axes) {
		return sel.Axes[i]
	}
	return AxisSel{}
}

// Validate checks the selection against an array shape. A nil Axes slice is
// valid and means "select all". Otherwise len(Axes) must equal len(shape).
// It rejects: length mismatch, out-of-bounds Index, Slice with Step < 1 or
// out-of-range bounds, and Indices that are not strictly increasing or fall
// out of bounds.
func (sel Selection) Validate(shape []int64) error {
	if sel.Axes == nil {
		return nil
	}
	if len(sel.Axes) != len(shape) {
		return fmt.Errorf("carray: selection has %d axes, shape has %d", len(sel.Axes), len(shape))
	}
	for i, a := range sel.Axes {
		size := shape[i]
		if err := a.validate(i, size); err != nil {
			return err
		}
	}
	return nil
}

func (a AxisSel) validate(axis int, size int64) error {
	// Count active variants to enforce the one-of contract.
	active := 0
	if a.Index != nil {
		active++
	}
	if a.Slice != nil {
		active++
	}
	if a.Indices != nil {
		active++
	}
	if active > 1 {
		return fmt.Errorf("carray: axis %d selector sets multiple variants", axis)
	}

	switch {
	case a.Index != nil:
		if *a.Index < 0 || *a.Index >= size {
			return fmt.Errorf("carray: axis %d index %d out of bounds [0,%d)", axis, *a.Index, size)
		}
	case a.Slice != nil:
		s := *a.Slice
		if s.Step < 1 {
			return fmt.Errorf("carray: axis %d slice step %d < 1", axis, s.Step)
		}
		if s.Start < 0 || s.Start > size {
			return fmt.Errorf("carray: axis %d slice start %d out of bounds [0,%d]", axis, s.Start, size)
		}
		if s.Stop < s.Start || s.Stop > size {
			return fmt.Errorf("carray: axis %d slice stop %d out of bounds [%d,%d]", axis, s.Stop, s.Start, size)
		}
	case a.Indices != nil:
		prev := int64(-1)
		for _, idx := range a.Indices {
			if idx < 0 || idx >= size {
				return fmt.Errorf("carray: axis %d gather index %d out of bounds [0,%d)", axis, idx, size)
			}
			if idx <= prev {
				return fmt.Errorf("carray: axis %d gather indices not strictly increasing at %d", axis, idx)
			}
			prev = idx
		}
	}
	return nil
}

// ResultShape returns the shape of the array produced by applying the selection
// to an array of the given shape. Axes selected by a single Index are dropped.
// It assumes the selection has already passed Validate.
func (sel Selection) ResultShape(shape []int64) []int64 {
	out := make([]int64, 0, len(shape))
	for i, size := range shape {
		a := sel.axis(i)
		if a.Index != nil {
			continue // Single-index axis is dropped.
		}
		out = append(out, a.resultLen(size))
	}
	return out
}
