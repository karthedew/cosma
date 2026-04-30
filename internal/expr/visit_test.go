package expr

import (
	"errors"
	"reflect"
	"testing"

	"github.com/apache/arrow/go/v18/arrow"
)

func sampleTree() Expr {
	return AliasNode{
		Name: "above",
		Inner: BinaryNode{
			Op: BinaryOpGt,
			Left: BinaryNode{
				Op:    BinaryOpAdd,
				Left:  ColumnNode{Name: "price"},
				Right: LiteralNode{Value: int64(10), Type: arrow.PrimitiveTypes.Int64},
			},
			Right: LiteralNode{Value: int64(100), Type: arrow.PrimitiveTypes.Int64},
		},
	}
}

func TestStringSnapshot(t *testing.T) {
	got := sampleTree().String()
	want := "((price + 10) > 100) as above"
	if got != want {
		t.Fatalf("String() = %q, want %q", got, want)
	}
}

func TestWalkPostOrder(t *testing.T) {
	var visited []string
	err := Walk(sampleTree(), func(e Expr) error {
		visited = append(visited, e.String())
		return nil
	})
	if err != nil {
		t.Fatalf("Walk: %v", err)
	}
	want := []string{
		"price",
		"10",
		"(price + 10)",
		"100",
		"((price + 10) > 100)",
		"((price + 10) > 100) as above",
	}
	if !reflect.DeepEqual(visited, want) {
		t.Fatalf("post-order visit = %v, want %v", visited, want)
	}
}

func TestWalkPropagatesError(t *testing.T) {
	sentinel := errors.New("stop")
	count := 0
	err := Walk(sampleTree(), func(e Expr) error {
		count++
		if e.String() == "(price + 10)" {
			return sentinel
		}
		return nil
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("Walk err = %v, want %v", err, sentinel)
	}
	// Stopped after the inner add was visited: price, 10, add → 3 calls.
	if count != 3 {
		t.Fatalf("visited %d nodes, want 3", count)
	}
}

func TestRewriteIdentity(t *testing.T) {
	in := sampleTree()
	out := Rewrite(in, func(e Expr) Expr { return e })
	if out.String() != in.String() {
		t.Fatalf("identity rewrite changed tree: got %q, want %q", out.String(), in.String())
	}
}

func TestRewriteRenameColumn(t *testing.T) {
	out := Rewrite(sampleTree(), func(e Expr) Expr {
		if c, ok := e.(ColumnNode); ok && c.Name == "price" {
			return ColumnNode{Name: "cost"}
		}
		return e
	})
	got := out.String()
	want := "((cost + 10) > 100) as above"
	if got != want {
		t.Fatalf("rewrite produced %q, want %q", got, want)
	}
}

func TestRewriteCoversEveryNonLeafNode(t *testing.T) {
	// Tree exercising every withChildren case so a missed type panics here
	// rather than in production. Adding a new non-leaf node type means
	// extending this tree (and withChildren).
	tree := AliasNode{
		Name: "out",
		Inner: BinaryNode{
			Op: BinaryOpAnd,
			Left: UnaryNode{
				Op: UnaryOpNot,
				Inner: BinaryNode{
					Op:    BinaryOpEq,
					Left:  ColumnNode{Name: "x"},
					Right: LiteralNode{Value: int64(1), Type: arrow.PrimitiveTypes.Int64},
				},
			},
			Right: AggNode{
				Op: AggOpSum,
				Inner: CastNode{
					Inner: ColumnNode{Name: "y"},
					Type:  arrow.PrimitiveTypes.Float64,
				},
			},
		},
	}
	// Force reconstruction at every level by replacing a leaf, which
	// propagates "changed" back up to the root.
	out := Rewrite(tree, func(e Expr) Expr {
		if c, ok := e.(ColumnNode); ok && c.Name == "y" {
			return ColumnNode{Name: "z"}
		}
		return e
	})
	if got, want := out.String(), "(not((x == 1)) and sum(cast(z as float64))) as out"; got != want {
		t.Fatalf("rewrite produced %q, want %q", got, want)
	}
	// The legacy Eq/Gt nodes also reconstruct cleanly via withChildren.
	legacy := Eq{Left: ColumnNode{Name: "a"}, Right: LiteralNode{Value: 5}}
	rebuilt := Rewrite(legacy, func(e Expr) Expr {
		if c, ok := e.(ColumnNode); ok && c.Name == "a" {
			return ColumnNode{Name: "b"}
		}
		return e
	})
	if got, want := rebuilt.String(), "b == 5"; got != want {
		t.Fatalf("legacy rewrite produced %q, want %q", got, want)
	}
	legacyGt := Gt{Left: ColumnNode{Name: "a"}, Right: LiteralNode{Value: 5}}
	rebuiltGt := Rewrite(legacyGt, func(e Expr) Expr {
		if c, ok := e.(ColumnNode); ok && c.Name == "a" {
			return ColumnNode{Name: "b"}
		}
		return e
	})
	if got, want := rebuiltGt.String(), "b > 5"; got != want {
		t.Fatalf("legacy Gt rewrite produced %q, want %q", got, want)
	}
}
