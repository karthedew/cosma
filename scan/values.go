package scan

import (
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"

	"github.com/karthedew/cosma/carray"
	"github.com/karthedew/cosma/schema"
)

// values.go holds scan-local helpers that bridge carray selections and store
// dtypes into the Arrow surface the cell scan needs. carray keeps its
// per-axis position math unexported; the helpers here reproduce exactly its
// public contract (AxisSel one-of semantics) over the AxisSel struct fields,
// which ARE exported. They consume carray, never modify it.

// selAxisIsIndex reports whether the selection drops axis via a single Index
// selector. A short/nil Axes slice never drops an axis (it means "all").
func selAxisIsIndex(sel carray.Selection, axis int) bool {
	if axis >= len(sel.Axes) {
		return false
	}
	return sel.Axes[axis].Index != nil
}

// selAxisPositions returns the selected positions along an axis of the given
// size, ascending. It mirrors carray.AxisSel.positions over the exported
// fields. An empty result means the axis selects no cell (e.g. a Clip miss).
func selAxisPositions(sel carray.Selection, axis int, size int64) []int64 {
	var a carray.AxisSel
	if axis < len(sel.Axes) {
		a = sel.Axes[axis]
	}
	switch {
	case a.Index != nil:
		return []int64{*a.Index}
	case a.Slice != nil:
		s := *a.Slice
		if s.Stop <= s.Start || s.Step < 1 {
			return nil
		}
		out := make([]int64, 0, (s.Stop-s.Start+s.Step-1)/s.Step)
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

// arrowTypeForDType maps a store array's logical dtype to the Arrow type of the
// scan's value column. Only the fixed-width primitive dtypes the chunk-decode
// pipeline supports are mappable; everything else errors loudly (the value
// column would have no decodable bytes).
func arrowTypeForDType(dt schema.DType) (arrow.DataType, error) {
	switch dt {
	case schema.Bool:
		return arrow.FixedWidthTypes.Boolean, nil
	case schema.Int8:
		return arrow.PrimitiveTypes.Int8, nil
	case schema.Int16:
		return arrow.PrimitiveTypes.Int16, nil
	case schema.Int32:
		return arrow.PrimitiveTypes.Int32, nil
	case schema.Int64:
		return arrow.PrimitiveTypes.Int64, nil
	case schema.UInt8:
		return arrow.PrimitiveTypes.Uint8, nil
	case schema.UInt16:
		return arrow.PrimitiveTypes.Uint16, nil
	case schema.UInt32:
		return arrow.PrimitiveTypes.Uint32, nil
	case schema.UInt64:
		return arrow.PrimitiveTypes.Uint64, nil
	case schema.Float32:
		return arrow.PrimitiveTypes.Float32, nil
	case schema.Float64:
		return arrow.PrimitiveTypes.Float64, nil
	default:
		return nil, fmt.Errorf("dtype %q is not a supported cell-scan value type", dt)
	}
}

// dtypeWidth returns the byte width of one cell for the dtypes arrowTypeForDType
// accepts. It mirrors ndingest's internal width table (kept scan-local since
// ndingest does not export it).
func dtypeWidth(dt schema.DType) (int, error) {
	switch dt {
	case schema.Bool, schema.Int8, schema.UInt8:
		return 1, nil
	case schema.Int16, schema.UInt16:
		return 2, nil
	case schema.Int32, schema.UInt32, schema.Float32:
		return 4, nil
	case schema.Int64, schema.UInt64, schema.Float64:
		return 8, nil
	default:
		return 0, fmt.Errorf("dtype %q has no fixed cell width", dt)
	}
}
