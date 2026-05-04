package expr

import (
	"github.com/apache/arrow/go/v18/arrow"

	"github.com/karthedew/cosma/schema"
)

// Expr is the sealed interface implemented by every node in the expression
// tree. The unexported exprNode marker prevents external packages from
// introducing surprise node types, which keeps Eval and other dispatch
// switches exhaustive.
//
// Children returns this node's direct child expressions in evaluation order
// and may return nil for leaves. It is the contract that Walk and Rewrite
// rely on, so every node type must return a slice that matches the order
// withChildren expects when reconstructing the node.
//
// DataType resolves the node's Arrow output type against the input schema.
// It is how binding validates a tree without evaluating it.
type Expr interface {
	String() string
	Children() []Expr
	DataType(s *schema.Schema) (arrow.DataType, error)
	exprNode()
}
