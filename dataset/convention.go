package dataset

import (
	"fmt"
	"strings"

	"github.com/karthedew/cosma/carray"
	"github.com/karthedew/cosma/store"
)

// Convention is a pluggable strategy for inferring per-array dimension names and
// roles from a group's arrays. Conventions are tried in order; the first whose
// Detect returns true wins (first-match-wins, observable, overridable via
// WithConventions).
//
// Detect reports whether this convention claims the group. Infer then resolves
// each array into an ArrayRef (path, resolved dim names, role) and computes the
// group's aligned []carray.Dim. Infer returns an error only on inconsistency it
// cannot honestly represent (e.g. a dim name carrying two different sizes); the
// caller turns that into a failed interpretation, never a silent coercion.
type Convention interface {
	// Name identifies the convention (for diagnostics).
	Name() string
	// Detect reports whether this convention applies to the group.
	Detect(g *store.Group, arrays []*store.Array) bool
	// Infer resolves the group's arrays into refs plus the aligned dim set.
	Infer(g *store.Group, arrays []*store.Array) ([]ArrayRef, []carray.Dim, error)
}

// XarrayConvention interprets arrays that carry explicit dim names — the
// xarray/zarr "_ARRAY_DIMENSIONS" (v2) or "dimension_names" (v3) metadata, which
// the zarr driver surfaces uniformly as store.Array.DimNames.
//
// A 1-D array whose leaf name equals its single dim name is a coordinate
// (RoleCoord); every other array is data (RoleData). Each array's dims are read
// straight from its DimNames; the group's []carray.Dim is the union of
// (name,size) pairs across all arrays, and a name appearing with two different
// sizes is an inconsistency Infer reports as an error.
type XarrayConvention struct{}

// Name implements Convention.
func (XarrayConvention) Name() string { return "xarray" }

// Detect returns true when at least one array carries DimNames covering all its
// axes. Arrays without dim metadata in an otherwise-named group still get their
// names resolved by Infer (which errors if any array lacks a full DimNames set),
// so detection only needs one named array to claim the group.
func (XarrayConvention) Detect(_ *store.Group, arrays []*store.Array) bool {
	for _, a := range arrays {
		if len(a.DimNames) == a.Ndim() && a.Ndim() > 0 {
			return true
		}
		// A 0-d array with an explicit (empty but non-nil) DimNames also counts as
		// carrying dim metadata, but the common signal is a named axis.
		if a.DimNames != nil {
			return true
		}
	}
	return false
}

// Infer resolves dims and roles for every array, then folds the per-axis
// (name,size) pairs into the group's aligned dim set, erroring on a name with
// conflicting sizes.
func (XarrayConvention) Infer(_ *store.Group, arrays []*store.Array) ([]ArrayRef, []carray.Dim, error) {
	refs := make([]ArrayRef, 0, len(arrays))
	sizes := make(map[string]int64)
	var order []string // first-seen order of dim names, for a deterministic result

	for _, a := range arrays {
		if len(a.DimNames) != a.Ndim() {
			return nil, nil, fmt.Errorf(
				"dataset: array %q has %d axes but %d dim names; xarray convention requires a name per axis",
				a.ArrayPath, a.Ndim(), len(a.DimNames))
		}
		dims := make([]string, a.Ndim())
		copy(dims, a.DimNames)

		role := RoleData
		if a.Ndim() == 1 && dims[0] == leafName(a.ArrayPath) {
			role = RoleCoord
		}

		for axis, name := range dims {
			size := a.Shape[axis]
			if prev, ok := sizes[name]; ok {
				if prev != size {
					return nil, nil, fmt.Errorf(
						"dataset: dim %q has conflicting sizes %d and %d across arrays in the group",
						name, prev, size)
				}
			} else {
				sizes[name] = size
				order = append(order, name)
			}
		}

		refs = append(refs, ArrayRef{Path: a.ArrayPath, Dims: dims, Role: role})
	}

	groupDims := make([]carray.Dim, len(order))
	for i, name := range order {
		groupDims[i] = carray.Dim{Name: name, Size: sizes[name]}
	}
	return refs, groupDims, nil
}

// BareConvention is the always-detecting fallback for arrays with no dim
// metadata. Each array gets positional dim names "dim_0".."dim_{n-1}" and
// RoleData; the group's dims are the per-array positional dims, deduped by the
// (name,size) pair.
//
// BareConvention deliberately makes NO cross-array alignment claim. Positional
// names are inherently per-array (dim_0 of a length-3 array and dim_0 of a
// length-5 array are unrelated axes), so treating a name+size collision as a
// conflict would be a false positive. Instead it dedupes on the full (name,size)
// pair and never errors: two arrays that happen to share dim_0 with the same
// size collapse to one entry; arrays whose dim_0 differs in size simply emit two
// distinct (name,size) entries. The group dim set is therefore a faithful
// catalogue of the positional axes present, not an alignment assertion.
type BareConvention struct{}

// Name implements Convention.
func (BareConvention) Name() string { return "bare" }

// Detect always returns true; BareConvention is the last link in the chain.
func (BareConvention) Detect(_ *store.Group, _ []*store.Array) bool { return true }

// Infer assigns positional dims and RoleData to every array and never errors.
func (BareConvention) Infer(_ *store.Group, arrays []*store.Array) ([]ArrayRef, []carray.Dim, error) {
	refs := make([]ArrayRef, 0, len(arrays))
	var groupDims []carray.Dim
	seen := make(map[string]bool) // key: name + "\x00" + size

	for _, a := range arrays {
		dims := make([]string, a.Ndim())
		for axis := range dims {
			dims[axis] = fmt.Sprintf("dim_%d", axis)
		}
		for axis, name := range dims {
			size := a.Shape[axis]
			key := fmt.Sprintf("%s\x00%d", name, size)
			if !seen[key] {
				seen[key] = true
				groupDims = append(groupDims, carray.Dim{Name: name, Size: size})
			}
		}
		refs = append(refs, ArrayRef{Path: a.ArrayPath, Dims: dims, Role: RoleData})
	}
	return refs, groupDims, nil
}

// defaultConventions is the default chain: explicit dim metadata first, the
// positional fallback last. Order is observable (first-match-wins) and
// overridable via WithConventions.
func defaultConventions() []Convention {
	return []Convention{XarrayConvention{}, BareConvention{}}
}

// leafName returns the final path segment of an absolute store path. The root is
// the empty leaf name.
func leafName(path string) string {
	path = strings.TrimRight(path, "/")
	if path == "" {
		return ""
	}
	if i := strings.LastIndex(path, "/"); i >= 0 {
		return path[i+1:]
	}
	return path
}
