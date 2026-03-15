package dataframe

import (
	"testing"

	"github.com/karthedew/cosma/internal/expr"
	"github.com/karthedew/cosma/plan"
)

func TestLazyPlanBuild(t *testing.T) {
	ids, err := NewSeries("a", []int32{1, 2, 3})
	if err != nil {
		t.Fatalf("NewSeries a: %v", err)
	}
	vals, err := NewSeries("b", []string{"x", "y", "z"})
	if err != nil {
		t.Fatalf("NewSeries b: %v", err)
	}

	df, err := New([]*Series{ids, vals})
	if err != nil {
		t.Fatalf("New dataframe: %v", err)
	}

	lf := df.Lazy().
		Filter(expr.Gt{Left: expr.Col{Name: "a"}, Right: expr.Lit{Value: 10}}).
		Select("a").
		Limit(5)

	pl, err := lf.Plan()
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}

	if _, ok := pl.Root.(*plan.LimitNode); !ok {
		t.Fatalf("expected root LimitNode")
	}
}
