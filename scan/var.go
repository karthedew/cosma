package scan

import (
	"github.com/karthedew/cosma/dataset"
)

// Var builds an ArrayScan for the named data variable or coordinate of an
// interpreted dataset View. The output index columns are named by the variable's
// RESOLVED dim names (from the View's ArrayRef), so column names reflect the
// semantic dims the convention inferred rather than raw positional names.
//
// Var is a free function rather than a method on dataset.View by design: scan
// imports dataset, so dataset must not import scan or the edge would cycle. The
// store hierarchy needed for the scan is recovered from the View via
// View.Tree() — an accessor added precisely so this bridge can live in scan with
// imports staying one-directional (scan → dataset → carray/store). The design
// doc's sketched v.ScanVar("temp") is therefore spelled scan.Var(v, "temp").
//
// name is looked up first among the View's data variables, then its coordinates.
// An unknown name, or a View not carrying its tree (View.Tree() == nil), yields
// an ArrayScan whose Tree is nil; LazyScanArray defers that to a clear Open-time
// error, matching the rest of the LazyScan* family.
//
// WithCoords (joining coordinate VALUES onto the cell rows) is post-MVP and not
// provided here; Var emits integer index columns only.
func Var(v *dataset.View, name string) ArrayScan {
	if v == nil {
		return ArrayScan{}
	}
	ref, ok := v.DataVars[name]
	if !ok {
		ref, ok = v.Coords[name]
	}
	if !ok {
		return ArrayScan{}
	}
	return ArrayScan{
		Tree:      v.Tree(),
		ArrayPath: ref.Path,
		Dims:      ref.Dims,
	}
}
