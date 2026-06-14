// Package dataset is Cosma's Layer 2: it turns a raw store.Tree (Layer 1, pure
// structure) into semantic, aligned Views — the xarray-Dataset analogue. A View
// resolves each array's role (data vs coordinate) and dimension names and proves
// the group's dims are mutually consistent.
//
// Interpretation is DERIVED, never written back into the Tree, and is allowed to
// fail without poisoning it: a group whose arrays disagree (a dim name used with
// two sizes, a coord whose shape mismatches its dim) simply has no View. Role and
// dim inference is a pluggable Convention chain (XarrayConvention then
// BareConvention by default), overridable via WithConventions — the seam a future
// TensorH5Convention drops into.
//
// Imports stay one-directional: scan → dataset → carray/store. The bridge to the
// cell scan therefore lives in the scan package as the free function
// scan.Var(view, name); see View.Tree, which lets scan.Var recover the store
// hierarchy a View was interpreted from without dataset importing scan.
package dataset

import (
	"fmt"

	"github.com/karthedew/cosma/carray"
	"github.com/karthedew/cosma/store"
)

// Role classifies an array within an interpreted group.
type Role uint8

const (
	// RoleData is a data variable — the payload arrays of the group.
	RoleData Role = iota
	// RoleCoord is a coordinate variable — a 1-D array labelling its own axis.
	RoleCoord
	// RoleMask is a boolean mask variable (reserved; not inferred by the MVP
	// conventions).
	RoleMask
	// RoleBounds is a cell-bounds variable (reserved; not inferred by the MVP
	// conventions).
	RoleBounds
	// RoleSupport is any other auxiliary array (reserved; not inferred by the MVP
	// conventions).
	RoleSupport
)

// String returns the role's lowercase name.
func (r Role) String() string {
	switch r {
	case RoleData:
		return "data"
	case RoleCoord:
		return "coord"
	case RoleMask:
		return "mask"
	case RoleBounds:
		return "bounds"
	case RoleSupport:
		return "support"
	default:
		return fmt.Sprintf("role(%d)", uint8(r))
	}
}

// ArrayRef is one array's interpreted handle within a View: its absolute store
// path, the resolved dim name per axis, and its role.
type ArrayRef struct {
	// Path is the absolute store path of the array.
	Path string
	// Dims names each axis of the array, in axis order (length == array rank).
	Dims []string
	// Role classifies the array within the group.
	Role Role
}

// View is the interpreted, aligned form of one store group — the xarray-Dataset
// analogue. Dims is the group's mutually-consistent dimension set; DataVars and
// Coords map each array's LEAF name to its ArrayRef; Attrs is the group's
// pass-through attributes.
type View struct {
	// Path is the absolute store path of the interpreted group.
	Path string
	// Dims is the group's aligned dimension set (name + size), deduped across
	// arrays. A dim name never appears twice with different sizes.
	Dims []carray.Dim
	// DataVars maps each data variable's leaf name to its ArrayRef.
	DataVars map[string]ArrayRef
	// Coords maps each coordinate variable's leaf name to its ArrayRef.
	Coords map[string]ArrayRef
	// Attrs is the group's pass-through attribute map (may be nil).
	Attrs store.Attrs

	// tree is the store hierarchy this View was interpreted from. It is unexported
	// and surfaced via Tree() so the scan package can build an ArrayScan from a
	// View without dataset importing scan (which would cycle).
	tree *store.Tree
}

// Tree returns the store hierarchy this View was interpreted from. It lets the
// scan package's scan.Var(view, name) recover the *store.Tree needed to build an
// ArrayScan, keeping the dataset → scan import edge from existing. It may be nil
// for a View constructed by hand (the scan bridge errors clearly in that case).
func (v *View) Tree() *store.Tree { return v.tree }

// TreeView mirrors the store tree's group hierarchy. View is the interpreted
// dataset for the group at Path, or nil where the group does not interpret
// cleanly (a plain container — siblings need not align, so one group's failure
// never poisons the rest). Children maps each child group's leaf name to its
// TreeView.
type TreeView struct {
	// Path is the absolute store path of this group.
	Path string
	// View is the interpreted dataset for this group, or nil if it did not
	// interpret cleanly.
	View *View
	// Children maps each child group's leaf name to its TreeView.
	Children map[string]*TreeView
}

// Option configures interpretation.
type Option func(*config)

// config holds resolved interpretation settings.
type config struct {
	conventions []Convention
}

// WithConventions overrides the default convention chain. Conventions are tried
// in order; the first whose Detect returns true interprets the group. Passing no
// conventions is ignored (the default chain is kept) so the call can never
// silently disable interpretation.
func WithConventions(convs ...Convention) Option {
	return func(c *config) {
		if len(convs) > 0 {
			c.conventions = convs
		}
	}
}

// resolveConfig applies options over the default chain.
func resolveConfig(opts []Option) config {
	c := config{conventions: defaultConventions()}
	for _, opt := range opts {
		opt(&c)
	}
	return c
}

// Interpret interprets the single group at path into a View. It fails (returns a
// non-nil error) on inconsistency the convention cannot honestly represent — a
// dim name used with two different sizes, or a coordinate whose shape mismatches
// the group dim it labels — rather than coercing. The path must name a group
// that exists in the tree.
func Interpret(t *store.Tree, path string, opts ...Option) (*View, error) {
	if t == nil {
		return nil, fmt.Errorf("dataset: nil tree")
	}
	g, ok := t.Group(path)
	if !ok {
		return nil, fmt.Errorf("dataset: no group at %q", path)
	}
	cfg := resolveConfig(opts)
	return interpretGroup(t, g, cfg)
}

// InterpretTree walks the whole tree and mirrors its group hierarchy into a
// TreeView. Each group gets a View if it interprets cleanly; otherwise its View
// is nil (a plain container). A per-group interpretation failure NEVER fails the
// whole tree — only a structural error (a missing root) does.
func InterpretTree(t *store.Tree, opts ...Option) (*TreeView, error) {
	if t == nil {
		return nil, fmt.Errorf("dataset: nil tree")
	}
	root, ok := t.Group("/")
	if !ok {
		return nil, fmt.Errorf("dataset: tree has no root group")
	}
	cfg := resolveConfig(opts)
	return buildTreeView(t, root, cfg), nil
}

// buildTreeView recursively builds the TreeView for group g. Interpretation
// failure for g yields a nil View, not an error.
func buildTreeView(t *store.Tree, g *store.Group, cfg config) *TreeView {
	tv := &TreeView{Path: g.GroupPath}
	if v, err := interpretGroup(t, g, cfg); err == nil {
		tv.View = v
	}

	for _, childPath := range g.Children {
		if child, ok := t.Group(childPath); ok {
			if tv.Children == nil {
				tv.Children = make(map[string]*TreeView)
			}
			tv.Children[leafName(childPath)] = buildTreeView(t, child, cfg)
		}
	}
	return tv
}

// interpretGroup applies the convention chain to a group's direct array children
// and assembles the View. It returns an error on inconsistency.
func interpretGroup(t *store.Tree, g *store.Group, cfg config) (*View, error) {
	arrays := directArrays(t, g)

	var conv Convention
	for _, c := range cfg.conventions {
		if c.Detect(g, arrays) {
			conv = c
			break
		}
	}
	if conv == nil {
		return nil, fmt.Errorf("dataset: no convention matched group %q", g.GroupPath)
	}

	refs, dims, err := conv.Infer(g, arrays)
	if err != nil {
		return nil, fmt.Errorf("dataset: group %q (%s convention): %w", g.GroupPath, conv.Name(), err)
	}

	v := &View{
		Path:     g.GroupPath,
		Dims:     dims,
		DataVars: make(map[string]ArrayRef),
		Coords:   make(map[string]ArrayRef),
		Attrs:    g.GroupAttrs,
		tree:     t,
	}

	// Consistency (a dim name with two sizes, a coord whose shape mismatches its
	// dim) is the convention's responsibility: Infer reports it as an error above,
	// which interpretGroup has already surfaced. The default XarrayConvention
	// folds every array's axes into the group dim set and errors on a conflicting
	// size, so a coord that disagrees with another array on its dim cannot produce
	// a clean View. interpretGroup therefore trusts Infer and only sorts refs into
	// the role maps here.
	for _, ref := range refs {
		name := leafName(ref.Path)
		switch ref.Role {
		case RoleCoord:
			v.Coords[name] = ref
		default:
			v.DataVars[name] = ref
		}
	}

	return v, nil
}

// directArrays returns the direct array children of group g, in the tree's
// listing order (g.Children is in store listing order).
func directArrays(t *store.Tree, g *store.Group) []*store.Array {
	var out []*store.Array
	for _, childPath := range g.Children {
		if a, ok := t.Array(childPath); ok {
			out = append(out, a)
		}
	}
	return out
}
