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
// By default Var emits integer index columns. Pass scan.WithCoords() to replace
// each dimension that has a coordinate in the View with that coordinate's VALUES
// (see WithCoords).
func Var(v *dataset.View, name string, opts ...VarOption) ArrayScan {
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

	cfg := varConfig{}
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}

	s := ArrayScan{
		Tree:      v.Tree(),
		ArrayPath: ref.Path,
		Dims:      ref.Dims,
	}
	if cfg.withCoords {
		var coords map[string]string
		for _, dim := range ref.Dims {
			cref, ok := v.Coords[dim]
			if !ok {
				continue // dim has no coordinate; keep its int64 index column.
			}
			if coords == nil {
				coords = make(map[string]string)
			}
			coords[dim] = cref.Path
		}
		s.CoordPaths = coords
	}
	return s
}

// VarOption configures scan.Var.
type VarOption func(*varConfig)

type varConfig struct {
	withCoords bool
}

// WithCoords makes scan.Var join coordinate VALUES onto the scan: each kept
// dimension that has a coordinate variable in the View emits that coordinate's
// values (in the coordinate's dtype, named by the dim) in place of the integer
// index column. Dimensions without a coordinate keep their int64 index column.
// It matches xarray's index-by-coordinate behaviour and populates
// ArrayScan.CoordPaths from the View.
func WithCoords() VarOption {
	return func(c *varConfig) { c.withCoords = true }
}
