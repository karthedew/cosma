package dataframe

import (
	"context"
	"fmt"

	"github.com/apache/arrow/go/v18/arrow/memory"

	"github.com/karthedew/cosma/internal/compute"
)

// Join combines df (left) with other (right) on the shared key column on,
// returning a new DataFrame. how selects the join kind:
//
//   - "inner": emit only rows whose key appears in both frames.
//   - "left":  emit every left row; right columns are null where no key matches.
//
// Output columns are: all left columns in order, then all non-key right columns
// in order. The right key column is dropped (it is redundant with the left key).
// A non-key right column whose name collides with a left column is renamed with
// a "_right" suffix (Polars default).
//
// Rows are gathered with compute.Take, so the key and all value columns must be
// of a type Take supports. ctx is checked during the probe phase so a cancelled
// context stops the join early. The receiver and other are unchanged.
func (df *DataFrame) Join(ctx context.Context, other *DataFrame, on string, how string) (*DataFrame, error) {
	if other == nil {
		return nil, fmt.Errorf("join: other DataFrame is nil")
	}
	switch how {
	case "inner", "left":
	default:
		return nil, fmt.Errorf("join: unsupported how %q (want \"inner\" or \"left\")", how)
	}

	leftKeyIdx := df.columnIndex(on)
	if leftKeyIdx < 0 {
		return nil, fmt.Errorf("join: key column %q not found in left", on)
	}
	rightKeyIdx := other.columnIndex(on)
	if rightKeyIdx < 0 {
		return nil, fmt.Errorf("join: key column %q not found in right", on)
	}

	leftKeys, err := compute.BoxedValues(df.cols[leftKeyIdx].Chunked())
	if err != nil {
		return nil, fmt.Errorf("join: left key %q: %w", on, err)
	}
	rightKeys, err := compute.BoxedValues(other.cols[rightKeyIdx].Chunked())
	if err != nil {
		return nil, fmt.Errorf("join: right key %q: %w", on, err)
	}

	// Build phase: hash right key values -> right row indices. Null keys never
	// match (SQL NULL != NULL), so they are excluded from the table.
	buildTable := make(map[any][]int64, len(rightKeys))
	for r, v := range rightKeys {
		if v == nil {
			continue
		}
		buildTable[v] = append(buildTable[v], int64(r))
	}

	// Probe phase: for each left row, gather matching right rows. leftTake and
	// rightTake are parallel index slices feeding compute.Take. A right index of
	// -1 marks a null fill (left join with no match).
	var leftTake, rightTake []int64
	for l := 0; l < len(leftKeys); l++ {
		if l%4096 == 0 {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
		}
		key := leftKeys[l]
		var matches []int64
		if key != nil {
			matches = buildTable[key]
		}
		if len(matches) == 0 {
			if how == "left" {
				leftTake = append(leftTake, int64(l))
				rightTake = append(rightTake, -1)
			}
			continue
		}
		for _, r := range matches {
			leftTake = append(leftTake, int64(l))
			rightTake = append(rightTake, r)
		}
	}

	if err := ctx.Err(); err != nil {
		return nil, err
	}

	mem := memory.DefaultAllocator

	// Names already used on the left, so right columns can detect collisions.
	leftNames := make(map[string]struct{}, len(df.cols))
	for i := range df.cols {
		leftNames[df.cols[i].Name()] = struct{}{}
	}

	series := make([]*Series, 0, len(df.cols)+len(other.cols)-1)

	// Left columns, gathered by leftTake.
	for i := range df.cols {
		arr, err := compute.Take(df.cols[i].Chunked(), leftTake, mem)
		if err != nil {
			return nil, fmt.Errorf("join: left column %q: %w", df.cols[i].Name(), err)
		}
		series = append(series, seriesFromArray(df.cols[i].Name(), arr))
	}

	// Right columns (excluding the key), gathered by rightTake with -1 -> null.
	for i := range other.cols {
		if i == rightKeyIdx {
			continue
		}
		name := other.cols[i].Name()
		if _, clash := leftNames[name]; clash {
			name += "_right"
		}
		arr, err := compute.Take(other.cols[i].Chunked(), rightTake, mem)
		if err != nil {
			return nil, fmt.Errorf("join: right column %q: %w", other.cols[i].Name(), err)
		}
		series = append(series, seriesFromArray(name, arr))
	}

	return New(series)
}
