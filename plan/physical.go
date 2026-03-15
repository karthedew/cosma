package plan

import "github.com/apache/arrow/go/v18/arrow"

type PhysicalNode interface {
	Name() string
	Schema() *arrow.Schema
	Children() []PhysicalNode
}

type PhysicalPlan struct {
	Root PhysicalNode
}

func NewPhysicalPlan(root PhysicalNode) *PhysicalPlan {
	return &PhysicalPlan{Root: root}
}
