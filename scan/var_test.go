package scan_test

import (
	"context"
	"testing"

	"github.com/karthedew/cosma/dataset"
	"github.com/karthedew/cosma/scan"
	"github.com/karthedew/cosma/store"
	"github.com/karthedew/cosma/store/zarr"
)

// TestVarEndToEnd is the design doc §7 final example minus WithCoords: interpret
// a real zarr-python fixture, build an ArrayScan for a data var via scan.Var,
// Collect, and assert the output columns are named by the resolved dims and the
// row count matches the array cell count.
func TestVarEndToEnd(t *testing.T) {
	s, err := zarr.OpenFS("../store/zarr/testdata/v2_xarray")
	if err != nil {
		t.Fatalf("zarr.OpenFS: %v", err)
	}
	tree, err := store.Open(context.Background(), s)
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}

	v, err := dataset.Interpret(tree, "/")
	if err != nil {
		t.Fatalf("dataset.Interpret: %v", err)
	}

	df, err := scan.LazyScanArray(scan.Var(v, "data")).Collect(context.Background())
	if err != nil {
		t.Fatalf("Collect: %v", err)
	}

	// data is (y=2, x=3): output columns named by resolved dims, value last.
	want := []string{"y", "x", "value"}
	got := fieldNames(df)
	if len(got) != len(want) {
		t.Fatalf("columns = %v want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("columns = %v want %v", got, want)
		}
	}

	// Row count == array cell count (2*3 = 6).
	if df.NumRows() != 6 {
		t.Fatalf("NumRows = %d want 6", df.NumRows())
	}
}

// TestVarCoord scans a coordinate variable the same way; its single column is the
// resolved coord dim name.
func TestVarCoord(t *testing.T) {
	s, err := zarr.OpenFS("../store/zarr/testdata/v2_xarray")
	if err != nil {
		t.Fatalf("zarr.OpenFS: %v", err)
	}
	tree, err := store.Open(context.Background(), s)
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	v, err := dataset.Interpret(tree, "/")
	if err != nil {
		t.Fatalf("dataset.Interpret: %v", err)
	}

	df, err := scan.LazyScanArray(scan.Var(v, "x")).Collect(context.Background())
	if err != nil {
		t.Fatalf("Collect: %v", err)
	}
	want := []string{"x", "value"}
	got := fieldNames(df)
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("coord columns = %v want %v", got, want)
	}
	if df.NumRows() != 3 {
		t.Fatalf("coord NumRows = %d want 3", df.NumRows())
	}
}

// TestVarUnknownName yields an empty ArrayScan whose nil Tree defers to a clear
// Collect-time error.
func TestVarUnknownName(t *testing.T) {
	s, err := zarr.OpenFS("../store/zarr/testdata/v2_xarray")
	if err != nil {
		t.Fatalf("zarr.OpenFS: %v", err)
	}
	tree, err := store.Open(context.Background(), s)
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	v, err := dataset.Interpret(tree, "/")
	if err != nil {
		t.Fatalf("dataset.Interpret: %v", err)
	}
	if _, err := scan.LazyScanArray(scan.Var(v, "nope")).Collect(context.Background()); err == nil {
		t.Fatalf("expected error for unknown var name, got nil")
	}
}
