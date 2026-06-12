// Package conformance is a reusable driver test suite for store.Store
// implementations. Any driver package can run it against a fixture store to
// prove it satisfies the Layer 1 contract. The zarr driver (Z4) runs this
// verbatim against its fixtures.
//
// A driver provides a Fixture: a built store plus the expected shape of its
// hierarchy and chunks. RunConformance then drives the four-method interface,
// the optional ChunkResolver, and Tree-based access.
package conformance

import (
	"context"
	"errors"
	"reflect"
	"sort"
	"testing"

	"github.com/karthedew/cosma/store"
)

// Fixture describes a driver store and the facts a conformant store must
// report. NewStore must return a fresh, independent store on each call.
type Fixture struct {
	// NewStore builds the store under test. Called multiple times; each call
	// must yield an equivalent, independent store.
	NewStore func() store.Store

	// Groups are the absolute paths of every expected group, including "/".
	Groups []string
	// Arrays are the absolute paths of every expected array.
	Arrays []string

	// PresentChunk is a chunk known to exist; ReadChunk must return non-empty
	// bytes and ChunkResolver (if implemented) must resolve it.
	PresentChunk store.ChunkKey
	// MissingChunk is a chunk known not to exist; ReadChunk must return
	// store.ErrChunkMissing.
	MissingChunk store.ChunkKey
}

// RunConformance exercises a driver against the Layer 1 Store contract and
// Tree-based access. It is the single entry point a driver test calls.
func RunConformance(t *testing.T, fx Fixture) {
	t.Helper()

	t.Run("Meta", func(t *testing.T) { testMeta(t, fx) })
	t.Run("List", func(t *testing.T) { testList(t, fx) })
	t.Run("ReadChunk", func(t *testing.T) { testReadChunk(t, fx) })
	t.Run("ChunkResolver", func(t *testing.T) { testChunkResolver(t, fx) })
	t.Run("Tree", func(t *testing.T) { testTree(t, fx) })
	t.Run("TreeListDiscovery", func(t *testing.T) { testTreeListDiscovery(t, fx) })
	t.Run("TreeDeterminism", func(t *testing.T) { testTreeDeterminism(t, fx) })
	t.Run("Close", func(t *testing.T) { testClose(t, fx) })
}

func testMeta(t *testing.T, fx Fixture) {
	ctx := context.Background()
	s := fx.NewStore()
	defer s.Close()

	for _, p := range fx.Groups {
		obj, err := s.Meta(ctx, p)
		if err != nil {
			t.Fatalf("Meta(%q): unexpected error: %v", p, err)
		}
		if obj.Kind() != store.KindGroup {
			t.Errorf("Meta(%q): kind = %v, want group", p, obj.Kind())
		}
		if obj.Path() != p {
			t.Errorf("Meta(%q): Path() = %q", p, obj.Path())
		}
	}
	for _, p := range fx.Arrays {
		obj, err := s.Meta(ctx, p)
		if err != nil {
			t.Fatalf("Meta(%q): unexpected error: %v", p, err)
		}
		if obj.Kind() != store.KindArray {
			t.Errorf("Meta(%q): kind = %v, want array", p, obj.Kind())
		}
		if _, ok := obj.(*store.Array); !ok {
			t.Errorf("Meta(%q): not a *store.Array", p)
		}
	}
}

func testList(t *testing.T, fx Fixture) {
	ctx := context.Background()
	s := fx.NewStore()
	defer s.Close()

	// Every group must be listable; children must resolve to real objects.
	for _, p := range fx.Groups {
		entries, err := s.List(ctx, p)
		if err != nil {
			t.Fatalf("List(%q): %v", p, err)
		}
		for _, e := range entries {
			if e.Name == "" {
				t.Errorf("List(%q): entry with empty name", p)
			}
		}
	}
	// Arrays are not groups: List must error.
	for _, p := range fx.Arrays {
		if _, err := s.List(ctx, p); err == nil {
			t.Errorf("List(%q): expected error listing an array", p)
		}
	}
}

func testReadChunk(t *testing.T, fx Fixture) {
	ctx := context.Background()
	s := fx.NewStore()
	defer s.Close()

	data, err := s.ReadChunk(ctx, fx.PresentChunk)
	if err != nil {
		t.Fatalf("ReadChunk(present): %v", err)
	}
	if len(data) == 0 {
		t.Errorf("ReadChunk(present): empty bytes")
	}

	_, err = s.ReadChunk(ctx, fx.MissingChunk)
	if !errors.Is(err, store.ErrChunkMissing) {
		t.Errorf("ReadChunk(missing): err = %v, want ErrChunkMissing", err)
	}
}

func testChunkResolver(t *testing.T, fx Fixture) {
	s := fx.NewStore()
	defer s.Close()

	cr, ok := s.(store.ChunkResolver)
	if !ok {
		t.Skip("driver does not implement ChunkResolver")
	}

	ref, err := cr.ResolveChunk(fx.PresentChunk)
	if err != nil {
		t.Fatalf("ResolveChunk(present): %v", err)
	}
	if ref.URL == "" {
		t.Errorf("ResolveChunk(present): empty URL")
	}
	if !reflect.DeepEqual(ref.Key, fx.PresentChunk) {
		t.Errorf("ResolveChunk(present): Key = %+v, want %+v", ref.Key, fx.PresentChunk)
	}

	_, err = cr.ResolveChunk(fx.MissingChunk)
	if !errors.Is(err, store.ErrChunkMissing) {
		t.Errorf("ResolveChunk(missing): err = %v, want ErrChunkMissing", err)
	}
}

func testTree(t *testing.T, fx Fixture) {
	ctx := context.Background()
	s := fx.NewStore()
	defer s.Close()

	tree, err := store.Open(ctx, s)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	if tree.Root() == nil {
		t.Fatal("Root() = nil")
	}
	if tree.Store() != s {
		t.Errorf("Store() did not return the source store")
	}

	for _, p := range fx.Groups {
		if _, ok := tree.Group(p); !ok {
			t.Errorf("Group(%q): not found in tree", p)
		}
		if _, ok := tree.Array(p); ok {
			t.Errorf("Array(%q): group misreported as array", p)
		}
	}
	for _, p := range fx.Arrays {
		if _, ok := tree.Array(p); !ok {
			t.Errorf("Array(%q): not found in tree", p)
		}
		if _, ok := tree.Group(p); ok {
			t.Errorf("Group(%q): array misreported as group", p)
		}
	}

	// Arrays() returns exactly the expected leaves.
	var gotArrays []string
	for _, a := range tree.Arrays() {
		gotArrays = append(gotArrays, a.Path())
	}
	wantArrays := append([]string(nil), fx.Arrays...)
	sort.Strings(gotArrays)
	sort.Strings(wantArrays)
	if !reflect.DeepEqual(gotArrays, wantArrays) {
		t.Errorf("Arrays() = %v, want %v", gotArrays, wantArrays)
	}

	// Every tree group has Children filled (Tree derives them from List) and
	// every child path resolves to a node in the tree.
	for _, p := range fx.Groups {
		g, ok := tree.Group(p)
		if !ok {
			continue // reported above
		}
		entries, err := s.List(ctx, p)
		if err != nil {
			t.Fatalf("List(%q): %v", p, err)
		}
		if len(g.Children) != len(entries) {
			t.Errorf("Group(%q).Children has %d entries, List reports %d",
				p, len(g.Children), len(entries))
		}
		for _, child := range g.Children {
			if _, ok := tree.Object(child); !ok {
				t.Errorf("Group(%q) child %q not present in tree", p, child)
			}
		}
	}

	// ErrChunkMissing propagates through the tree's store.
	_, err = tree.Store().ReadChunk(ctx, fx.MissingChunk)
	if !errors.Is(err, store.ErrChunkMissing) {
		t.Errorf("tree.Store().ReadChunk(missing): err = %v, want ErrChunkMissing", err)
	}
}

// metaStripsChildren wraps a store so Meta returns groups with empty Children.
// This is the shape of drivers like zarr v2, whose group metadata carries no
// child listing — only List can discover children. A conformant Tree must come
// out complete regardless.
type metaStripsChildren struct {
	store.Store
}

func (m metaStripsChildren) Meta(ctx context.Context, path string) (store.Object, error) {
	obj, err := m.Store.Meta(ctx, path)
	if err != nil {
		return nil, err
	}
	if g, ok := obj.(*store.Group); ok {
		stripped := *g
		stripped.Children = nil
		return &stripped, nil
	}
	return obj, nil
}

func testTreeListDiscovery(t *testing.T, fx Fixture) {
	ctx := context.Background()
	s := fx.NewStore()
	defer s.Close()

	tree, err := store.Open(ctx, metaStripsChildren{s})
	if err != nil {
		t.Fatalf("Open(meta-strips-children): %v", err)
	}

	for _, p := range fx.Groups {
		if _, ok := tree.Group(p); !ok {
			t.Errorf("Group(%q): not discovered via List", p)
		}
	}
	for _, p := range fx.Arrays {
		if _, ok := tree.Array(p); !ok {
			t.Errorf("Array(%q): not discovered via List", p)
		}
	}
}

func testTreeDeterminism(t *testing.T, fx Fixture) {
	ctx := context.Background()

	var first []string
	for i := 0; i < 5; i++ {
		s := fx.NewStore()
		tree, err := store.Open(ctx, s)
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		var order []string
		_ = tree.Walk(func(o store.Object) error {
			order = append(order, o.Path())
			return nil
		})
		s.Close()

		if i == 0 {
			first = order
			continue
		}
		if !reflect.DeepEqual(order, first) {
			t.Fatalf("walk order not deterministic:\n run0: %v\n run%d: %v", first, i, order)
		}
	}
}

func testClose(t *testing.T, fx Fixture) {
	s := fx.NewStore()
	if err := s.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
}
