// Package carray holds the pure-geometry types for Cosma's n-dimensional
// array engine (ADR 0005): dimensions, selections, and chunk grids. It carries
// all the indexing math for chunked arrays — selection-to-chunk pruning, the
// n-dimensional analogue of Parquet row-group pruning — with no I/O and no
// dependencies beyond the standard library.
package carray

// Dim describes a single array axis. Name may be "" for an anonymous axis;
// Size is the extent of the axis in cells and must be >= 0.
type Dim struct {
	Name string
	Size int64
}
