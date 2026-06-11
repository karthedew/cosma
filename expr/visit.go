package expr

import "fmt"

// Walk performs a post-order, read-only traversal of n. Children are visited
// before the node itself, and the first error returned by fn aborts the walk.
// fn is never called on a nil node.
func Walk(n ExprNode, fn func(ExprNode) error) error {
	if n == nil {
		return nil
	}
	for _, c := range n.Children() {
		if err := Walk(c, fn); err != nil {
			return err
		}
	}
	return fn(n)
}

// Rewrite performs a post-order rewrite of n. Each child is rewritten first, the
// parent is rebuilt with the new children via withChildren, and finally fn is
// invoked on the (possibly rebuilt) parent to produce its replacement. Rewrite
// does not recurse into the replacement, so callers wanting a fixed point should
// loop until no node changes.
func Rewrite(n ExprNode, fn func(ExprNode) ExprNode) ExprNode {
	if n == nil {
		return nil
	}
	children := n.Children()
	if len(children) == 0 {
		return fn(n)
	}
	rewritten := make([]ExprNode, len(children))
	changed := false
	for i, c := range children {
		nc := Rewrite(c, fn)
		rewritten[i] = nc
		if nc != c {
			changed = true
		}
	}
	if changed {
		n = withChildren(n, rewritten)
	}
	return fn(n)
}

// withChildren reconstructs n with the supplied children. It is the single
// place that knows about each non-leaf node's shape; adding a new node type
// requires updating this switch. The default case panics deliberately so a
// missed case surfaces immediately rather than silently dropping rewrites.
func withChildren(n ExprNode, children []ExprNode) ExprNode {
	switch x := n.(type) {
	case BinaryNode:
		return BinaryNode{Op: x.Op, Left: children[0], Right: children[1]}
	case UnaryNode:
		return UnaryNode{Op: x.Op, Inner: children[0]}
	case AliasNode:
		return AliasNode{Name: x.Name, Inner: children[0]}
	case CastNode:
		return CastNode{Inner: children[0], Type: x.Type}
	default:
		panic(fmt.Sprintf("expr.withChildren: unhandled node type %T — update visit.go when adding new nodes", n))
	}
}
