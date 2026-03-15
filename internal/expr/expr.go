package expr

import (
	"fmt"
)

type Expr interface {
	String() string
}

type Col struct {
	Name string
}

func (c Col) String() string {
	return c.Name
}

type Lit struct {
	Value any
}

func (l Lit) String() string {
	return fmt.Sprintf("%v", l.Value)
}

type Eq struct {
	Left  Expr
	Right Expr
}

func (e Eq) String() string {
	return fmt.Sprintf("%s == %s", e.Left, e.Right)
}

type Gt struct {
	Left  Expr
	Right Expr
}

func (g Gt) String() string {
	return fmt.Sprintf("%s > %s", g.Left, g.Right)
}
