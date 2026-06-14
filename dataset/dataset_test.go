package dataset_test

import (
	"context"
	"testing"

	"github.com/karthedew/cosma/carray"
	"github.com/karthedew/cosma/dataset"
	"github.com/karthedew/cosma/schema"
	"github.com/karthedew/cosma/store"
	"github.com/karthedew/cosma/store/memstore"
	"github.com/karthedew/cosma/store/zarr"
)

// openZarr opens a committed zarr fixture as a store.Tree.
func openZarr(t *testing.T, path string) *store.Tree {
	t.Helper()
	s, err := zarr.OpenFS(path)
	if err != nil {
		t.Fatalf("zarr.OpenFS(%q): %v", path, err)
	}
	tree, err := store.Open(context.Background(), s)
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	return tree
}

// openMem opens a memstore.Store as a store.Tree.
func openMem(t *testing.T, s *memstore.Store) *store.Tree {
	t.Helper()
	tree, err := store.Open(context.Background(), s)
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	return tree
}

// dimSize finds the size of a named dim in a View's dim set.
func dimSize(v *dataset.View, name string) (int64, bool) {
	for _, d := range v.Dims {
		if d.Name == name {
			return d.Size, true
		}
	}
	return 0, false
}

func TestInterpretXarrayFixture(t *testing.T) {
	tree := openZarr(t, "../store/zarr/testdata/v2_xarray")

	v, err := dataset.Interpret(tree, "/")
	if err != nil {
		t.Fatalf("Interpret: %v", err)
	}

	// x and y are 1-D arrays named after their own dim → coords.
	for _, name := range []string{"x", "y"} {
		ref, ok := v.Coords[name]
		if !ok {
			t.Fatalf("expected %q in Coords, coords=%v datavars=%v", name, v.Coords, v.DataVars)
		}
		if ref.Role != dataset.RoleCoord {
			t.Fatalf("coord %q role = %v want RoleCoord", name, ref.Role)
		}
		if len(ref.Dims) != 1 || ref.Dims[0] != name {
			t.Fatalf("coord %q dims = %v want [%s]", name, ref.Dims, name)
		}
	}

	// data is 2-D (y,x) → data var.
	ref, ok := v.DataVars["data"]
	if !ok {
		t.Fatalf("expected data in DataVars, got %v", v.DataVars)
	}
	if ref.Role != dataset.RoleData {
		t.Fatalf("data role = %v want RoleData", ref.Role)
	}
	wantDims := []string{"y", "x"}
	if len(ref.Dims) != 2 || ref.Dims[0] != wantDims[0] || ref.Dims[1] != wantDims[1] {
		t.Fatalf("data dims = %v want %v", ref.Dims, wantDims)
	}

	// Group dims resolved with correct sizes (y=2, x=3 from the fixture shapes).
	if sz, ok := dimSize(v, "y"); !ok || sz != 2 {
		t.Fatalf("dim y size = %d ok=%v want 2", sz, ok)
	}
	if sz, ok := dimSize(v, "x"); !ok || sz != 3 {
		t.Fatalf("dim x size = %d ok=%v want 3", sz, ok)
	}

	if v.Tree() != tree {
		t.Fatalf("View.Tree() did not return the source tree")
	}
}

func TestInterpretBareConvention(t *testing.T) {
	// A group of arrays with NO dim metadata → BareConvention positional dims.
	s := memstore.New()
	s.AddArray("/a", store.Array{
		Shape:      []int64{4, 5},
		ChunkShape: []int64{4, 5},
		DType:      schema.Float64,
	}, nil)
	s.AddArray("/b", store.Array{
		Shape:      []int64{3},
		ChunkShape: []int64{3},
		DType:      schema.Int64,
	}, nil)
	tree := openMem(t, s)

	v, err := dataset.Interpret(tree, "/")
	if err != nil {
		t.Fatalf("Interpret: %v", err)
	}

	// All RoleData, positional dims.
	if len(v.Coords) != 0 {
		t.Fatalf("bare convention produced coords: %v", v.Coords)
	}
	a, ok := v.DataVars["a"]
	if !ok {
		t.Fatalf("missing data var a: %v", v.DataVars)
	}
	if len(a.Dims) != 2 || a.Dims[0] != "dim_0" || a.Dims[1] != "dim_1" {
		t.Fatalf("a dims = %v want [dim_0 dim_1]", a.Dims)
	}
	b, ok := v.DataVars["b"]
	if !ok {
		t.Fatalf("missing data var b: %v", v.DataVars)
	}
	if len(b.Dims) != 1 || b.Dims[0] != "dim_0" {
		t.Fatalf("b dims = %v want [dim_0]", b.Dims)
	}

	// dim_0 appears twice (size 4 and size 3) without erroring: BareConvention
	// makes no cross-array alignment claim and dedupes on (name,size).
	var got4, got3 bool
	for _, d := range v.Dims {
		if d.Name == "dim_0" && d.Size == 4 {
			got4 = true
		}
		if d.Name == "dim_0" && d.Size == 3 {
			got3 = true
		}
	}
	if !got4 || !got3 {
		t.Fatalf("bare dims = %v want both (dim_0,4) and (dim_0,3)", v.Dims)
	}
}

func TestInterpretInconsistentGroupErrors(t *testing.T) {
	// Two arrays sharing dim name "x" with different sizes → inconsistency.
	s := memstore.New()
	s.AddArray("/p", store.Array{
		Shape:      []int64{3},
		ChunkShape: []int64{3},
		DType:      schema.Float64,
		DimNames:   []string{"x"},
	}, nil)
	s.AddArray("/q", store.Array{
		Shape:      []int64{5},
		ChunkShape: []int64{5},
		DType:      schema.Float64,
		DimNames:   []string{"x"},
	}, nil)
	tree := openMem(t, s)

	if _, err := dataset.Interpret(tree, "/"); err == nil {
		t.Fatalf("Interpret: expected error for conflicting dim sizes, got nil")
	}
}

func TestInterpretTreeNilViewForInconsistentSibling(t *testing.T) {
	// Root has two child groups: /good interprets cleanly, /bad is inconsistent.
	s := memstore.New()
	s.AddGroup("/good", nil)
	s.AddGroup("/bad", nil)

	// /good: a consistent xarray-style group.
	s.AddArray("/good/x", store.Array{
		Shape: []int64{3}, ChunkShape: []int64{3},
		DType: schema.Int64, DimNames: []string{"x"},
	}, nil)
	s.AddArray("/good/temp", store.Array{
		Shape: []int64{3}, ChunkShape: []int64{3},
		DType: schema.Float64, DimNames: []string{"x"},
	}, nil)

	// /bad: dim "x" with conflicting sizes.
	s.AddArray("/bad/p", store.Array{
		Shape: []int64{3}, ChunkShape: []int64{3},
		DType: schema.Float64, DimNames: []string{"x"},
	}, nil)
	s.AddArray("/bad/q", store.Array{
		Shape: []int64{5}, ChunkShape: []int64{5},
		DType: schema.Float64, DimNames: []string{"x"},
	}, nil)

	tree := openMem(t, s)

	tv, err := dataset.InterpretTree(tree)
	if err != nil {
		t.Fatalf("InterpretTree: %v", err)
	}

	good := tv.Children["good"]
	if good == nil || good.View == nil {
		t.Fatalf("expected /good to have a valid View, got %+v", good)
	}
	if _, ok := good.View.Coords["x"]; !ok {
		t.Fatalf("/good expected coord x, got coords=%v", good.View.Coords)
	}
	if _, ok := good.View.DataVars["temp"]; !ok {
		t.Fatalf("/good expected data var temp, got datavars=%v", good.View.DataVars)
	}

	bad := tv.Children["bad"]
	if bad == nil {
		t.Fatalf("expected /bad node in tree")
	}
	if bad.View != nil {
		t.Fatalf("expected /bad to have nil View (inconsistent), got %+v", bad.View)
	}
}

func TestWithConventionsOverride(t *testing.T) {
	// Force BareConvention only over a fixture that XarrayConvention would claim;
	// arrays then get positional dims regardless of DimNames.
	tree := openZarr(t, "../store/zarr/testdata/v2_xarray")

	v, err := dataset.Interpret(tree, "/", dataset.WithConventions(dataset.BareConvention{}))
	if err != nil {
		t.Fatalf("Interpret with bare override: %v", err)
	}
	if len(v.Coords) != 0 {
		t.Fatalf("bare override produced coords: %v", v.Coords)
	}
	ref, ok := v.DataVars["data"]
	if !ok {
		t.Fatalf("missing data var: %v", v.DataVars)
	}
	if ref.Dims[0] != "dim_0" || ref.Dims[1] != "dim_1" {
		t.Fatalf("bare override dims = %v want positional", ref.Dims)
	}
}

func TestRoleString(t *testing.T) {
	cases := map[dataset.Role]string{
		dataset.RoleData:    "data",
		dataset.RoleCoord:   "coord",
		dataset.RoleMask:    "mask",
		dataset.RoleBounds:  "bounds",
		dataset.RoleSupport: "support",
	}
	for r, want := range cases {
		if got := r.String(); got != want {
			t.Fatalf("Role(%d).String() = %q want %q", r, got, want)
		}
	}
}

// Sanity that carray.Dim is the type the View exposes (compile-time assertion via
// use).
var _ = func() []carray.Dim { return (&dataset.View{}).Dims }
