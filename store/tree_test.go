package store_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync"
	"testing"

	"github.com/karthedew/cosma/schema"
	"github.com/karthedew/cosma/store"
	"github.com/karthedew/cosma/store/memstore"
)

// newFixtureStore builds the hierarchy used across the Tree tests:
//
//	/
//	├── weather/
//	│   ├── temp   (4×6 float64)
//	│   └── time   (4 int64)
//	└── flags      (8 bool)
func newFixtureStore() *memstore.Store {
	return memstore.New().
		AddGroup("/weather", store.Attrs{"title": "station data"}).
		AddArray("/weather/temp", store.Array{
			Shape:      []int64{4, 6},
			ChunkShape: []int64{4, 3},
			DType:      schema.Float64,
			DimNames:   []string{"time", "station"},
		}, map[string][]byte{
			"0.0": {1, 2, 3, 4},
		}).
		AddArray("/weather/time", store.Array{
			Shape:      []int64{4},
			ChunkShape: []int64{4},
			DType:      schema.Int64,
		}, map[string][]byte{
			"0": {0, 1, 2, 3},
		}).
		AddArray("/flags", store.Array{
			Shape:      []int64{8},
			ChunkShape: []int64{8},
			DType:      schema.Bool,
		}, nil)
}

func openFixtureTree(t *testing.T) *store.Tree {
	t.Helper()
	s := newFixtureStore()
	t.Cleanup(func() { s.Close() })
	tree, err := store.Open(context.Background(), s)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	return tree
}

func TestTreePrintGolden(t *testing.T) {
	tree := openFixtureTree(t)

	var buf bytes.Buffer
	if err := tree.Print(&buf); err != nil {
		t.Fatalf("Print: %v", err)
	}

	want := `/ [group]
  weather [group]
    temp [array shape=(4, 6) dtype=float64]
    time [array shape=(4) dtype=int64]
  flags [array shape=(8) dtype=bool]
`
	if got := buf.String(); got != want {
		t.Errorf("Print output mismatch:\n--- got ---\n%s--- want ---\n%s", got, want)
	}
}

func TestTreeLookups(t *testing.T) {
	tree := openFixtureTree(t)

	root := tree.Root()
	if root == nil || root.Path() != "/" {
		t.Fatalf("Root() = %+v, want path /", root)
	}
	if len(root.Children) != 2 {
		t.Errorf("root has %d children, want 2", len(root.Children))
	}

	a, ok := tree.Array("/weather/temp")
	if !ok {
		t.Fatal("Array(/weather/temp) not found")
	}
	if a.Ndim() != 2 || a.DType != schema.Float64 {
		t.Errorf("temp array = ndim %d dtype %s, want 2 float64", a.Ndim(), a.DType)
	}
	if a.Name() != "temp" {
		t.Errorf("Name() = %q, want temp", a.Name())
	}

	g, ok := tree.Group("/weather")
	if !ok {
		t.Fatal("Group(/weather) not found")
	}
	if title, ok := g.Attrs().String("title"); !ok || title != "station data" {
		t.Errorf("weather attrs title = %q, %v", title, ok)
	}

	if _, ok := tree.Group("/weather/temp"); ok {
		t.Error("Group(/weather/temp) should be false for an array")
	}
	if _, ok := tree.Object("/nope"); ok {
		t.Error("Object(/nope) should be false")
	}

	arrays := tree.Arrays()
	wantOrder := []string{"/weather/temp", "/weather/time", "/flags"}
	if len(arrays) != len(wantOrder) {
		t.Fatalf("Arrays() returned %d arrays, want %d", len(arrays), len(wantOrder))
	}
	for i, p := range wantOrder {
		if arrays[i].Path() != p {
			t.Errorf("Arrays()[%d] = %q, want %q", i, arrays[i].Path(), p)
		}
	}
}

func TestTreeWalkStopsOnError(t *testing.T) {
	tree := openFixtureTree(t)

	sentinel := errors.New("stop")
	var visited int
	err := tree.Walk(func(o store.Object) error {
		visited++
		if o.Path() == "/weather/temp" {
			return sentinel
		}
		return nil
	})
	if !errors.Is(err, sentinel) {
		t.Errorf("Walk err = %v, want sentinel", err)
	}
	if visited != 3 { // /, /weather, /weather/temp
		t.Errorf("visited %d nodes before stopping, want 3", visited)
	}
}

// TestTreeConcurrentReads hammers every read method from parallel goroutines;
// meaningful only under -race, which the package CI run uses.
func TestTreeConcurrentReads(t *testing.T) {
	tree := openFixtureTree(t)

	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				if _, ok := tree.Group("/weather"); !ok {
					t.Error("Group(/weather) lookup failed")
					return
				}
				if _, ok := tree.Array("/flags"); !ok {
					t.Error("Array(/flags) lookup failed")
					return
				}
				if _, ok := tree.Object("/weather/time"); !ok {
					t.Error("Object(/weather/time) lookup failed")
					return
				}
				_ = tree.Root()
				_ = tree.Arrays()
				_ = tree.Walk(func(store.Object) error { return nil })
				_ = tree.Print(io.Discard)
			}
		}()
	}
	wg.Wait()
}

// TestErrChunkMissingThroughTree exercises the Tree-based access path: locate
// an array via the tree, read a chunk that does not exist, and require the
// sentinel to surface via errors.Is.
func TestErrChunkMissingThroughTree(t *testing.T) {
	tree := openFixtureTree(t)

	a, ok := tree.Array("/flags")
	if !ok {
		t.Fatal("Array(/flags) not found")
	}
	_, err := tree.Store().ReadChunk(context.Background(), store.ChunkKey{
		ArrayPath: a.Path(), Coord: []int64{0},
	})
	if !errors.Is(err, store.ErrChunkMissing) {
		t.Errorf("ReadChunk err = %v, want ErrChunkMissing", err)
	}
}
