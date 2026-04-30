package expr

import "fmt"

// Walk performs a post-order, read-only traversal of e. Children are visited
// before the node itself, and the first error returned by fn aborts the walk.
// fn is never called on a nil expression.
func Walk(e Expr, fn func(Expr) error) error {
	if e == nil {
		return nil
	}
	for _, c := range e.Children() {
		if err := Walk(c, fn); err != nil {
			return err
		}
	}
	return fn(e)
}

// Rewrite performs a post-order rewrite of e. Each child is rewritten first,
// the parent is rebuilt with the new children via withChildren, and finally
// fn is invoked on the (possibly rebuilt) parent to produce its replacement.
// fn is responsible for what it returns; Rewrite does not recurse into the
// replacement, so callers that want a fixed-point pass should run Rewrite
// in a loop until no node changes.
func Rewrite(e Expr, fn func(Expr) Expr) Expr {
	if e == nil {
		return nil
	}
	children := e.Children()
	if len(children) == 0 {
		return fn(e)
	}
	rewritten := make([]Expr, len(children))
	changed := false
	for i, c := range children {
		nc := Rewrite(c, fn)
		rewritten[i] = nc
		if nc != c {
			changed = true
		}
	}
	if changed {
		e = withChildren(e, rewritten)
	}
	return fn(e)
}

// withChildren reconstructs e with the supplied children. It is the single
// place that knows about each non-leaf node's shape; adding a new node type
// requires updating this switch. The default case panics deliberately so
// that a missed case shows up immediately during development rather than
// silently dropping rewrites.
func withChildren(e Expr, children []Expr) Expr {
	switch n := e.(type) {
	case Eq:
		return Eq{Left: children[0], Right: children[1]}
	case Gt:
		return Gt{Left: children[0], Right: children[1]}
	case BinaryNode:
		return BinaryNode{Op: n.Op, Left: children[0], Right: children[1]}
	case UnaryNode:
		return UnaryNode{Op: n.Op, Inner: children[0]}
	case AggNode:
		return AggNode{Op: n.Op, Inner: children[0]}
	case AliasNode:
		return AliasNode{Name: n.Name, Inner: children[0]}
	case CastNode:
		return CastNode{Inner: children[0], Type: n.Type}
	default:
		panic(fmt.Sprintf("expr.withChildren: unhandled node type %T — update visit.go when adding new nodes", e))
	}
}
