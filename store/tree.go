package store

import (
	"context"
	"fmt"
	"io"
	"strings"
)

// OpenOption configures a Tree Open. The MVP performs an eager full walk and
// defines no options yet; the type exists so post-MVP additions
// (WithPrefix/WithDepth for lazy subtree opens) are non-breaking.
type OpenOption func(*openConfig)

// openConfig holds resolved Open settings. It is unexported; options mutate it.
type openConfig struct{}

// Tree is an immutable snapshot of a store's hierarchy, built by a single
// metadata walk at Open time. All read methods are safe for concurrent use; the
// snapshot is never mutated after construction.
type Tree struct {
	store   Store
	objects map[string]Object // absolute path -> Object, includes root "/"
	order   []string          // deterministic depth-first walk order
}

// Open walks the store's metadata once and returns an immutable Tree snapshot.
// The walk is depth-first from the root; children are visited in the order the
// driver's List reports them, giving a deterministic walk order for a given
// store.
func Open(ctx context.Context, s Store, opts ...OpenOption) (*Tree, error) {
	cfg := openConfig{}
	for _, opt := range opts {
		opt(&cfg)
	}

	t := &Tree{
		store:   s,
		objects: make(map[string]Object),
	}
	if err := t.build(ctx, "/"); err != nil {
		return nil, err
	}
	return t, nil
}

// build recursively populates objects and order starting at path. Children are
// discovered via Store.List — never via the Group.Children the driver's Meta
// returned, which a driver is free to leave empty (zarr v2 group metadata
// carries no child listing; only a directory listing can discover children).
// The Tree stores its own copy of each group with Children filled from the
// List results; the driver's returned object is never mutated.
func (t *Tree) build(ctx context.Context, path string) error {
	obj, err := t.store.Meta(ctx, path)
	if err != nil {
		return fmt.Errorf("store: meta %q: %w", path, err)
	}

	g, ok := obj.(*Group)
	if !ok {
		t.objects[path] = obj
		t.order = append(t.order, path)
		return nil
	}

	entries, err := t.store.List(ctx, path)
	if err != nil {
		return fmt.Errorf("store: list %q: %w", path, err)
	}
	childPaths := make([]string, len(entries))
	for i, e := range entries {
		childPaths[i] = joinPath(path, e.Name)
	}

	// Tree-owned copy: Children is derived from List, not from the driver.
	own := *g
	own.Children = childPaths
	t.objects[path] = &own
	t.order = append(t.order, path)

	for _, child := range childPaths {
		if err := t.build(ctx, child); err != nil {
			return err
		}
	}
	return nil
}

// Root returns the root group of the tree.
func (t *Tree) Root() *Group {
	g, _ := t.Group("/")
	return g
}

// Object returns the node at path, or false if no such node exists.
func (t *Tree) Object(path string) (Object, bool) {
	obj, ok := t.objects[path]
	return obj, ok
}

// Group returns the group at path, or false if the path is absent or names an
// array.
func (t *Tree) Group(path string) (*Group, bool) {
	obj, ok := t.objects[path]
	if !ok {
		return nil, false
	}
	g, ok := obj.(*Group)
	return g, ok
}

// Array returns the array at path, or false if the path is absent or names a
// group.
func (t *Tree) Array(path string) (*Array, bool) {
	obj, ok := t.objects[path]
	if !ok {
		return nil, false
	}
	a, ok := obj.(*Array)
	return a, ok
}

// Walk visits every node in deterministic depth-first order, calling fn for
// each. A non-nil error from fn stops the walk and is returned.
func (t *Tree) Walk(fn func(Object) error) error {
	for _, path := range t.order {
		if err := fn(t.objects[path]); err != nil {
			return err
		}
	}
	return nil
}

// Arrays returns every array leaf in walk order.
func (t *Tree) Arrays() []*Array {
	var out []*Array
	for _, path := range t.order {
		if a, ok := t.objects[path].(*Array); ok {
			out = append(out, a)
		}
	}
	return out
}

// Store returns the underlying store the tree was built from.
func (t *Tree) Store() Store { return t.store }

// Print writes an indented listing of the tree to w. Each line shows a node's
// name; array lines also show kind, shape, and dtype. The root is shown as "/".
func (t *Tree) Print(w io.Writer) error {
	for _, path := range t.order {
		obj := t.objects[path]
		depth := pathDepth(path)
		indent := strings.Repeat("  ", depth)

		name := obj.Name()
		if path == "/" {
			name = "/"
		}

		switch o := obj.(type) {
		case *Array:
			if _, err := fmt.Fprintf(w, "%s%s [array shape=%s dtype=%s]\n",
				indent, name, formatShape(o.Shape), o.DType); err != nil {
				return err
			}
		default:
			if _, err := fmt.Fprintf(w, "%s%s [group]\n", indent, name); err != nil {
				return err
			}
		}
	}
	return nil
}

// pathDepth returns the indentation depth of an absolute path; root is 0.
func pathDepth(path string) int {
	if path == "/" || path == "" {
		return 0
	}
	return strings.Count(strings.Trim(path, "/"), "/") + 1
}

// formatShape renders a shape slice as "(d0, d1, ...)"; "()" for a scalar.
func formatShape(shape []int64) string {
	if len(shape) == 0 {
		return "()"
	}
	parts := make([]string, len(shape))
	for i, d := range shape {
		parts[i] = fmt.Sprintf("%d", d)
	}
	return "(" + strings.Join(parts, ", ") + ")"
}
