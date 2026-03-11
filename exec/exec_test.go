package exec

import "testing"

func TestExecutorNotImplemented(t *testing.T) {
	exec := NewExecutor()
	if exec == nil {
		t.Fatalf("expected executor")
	}
	if err := exec.Execute(&Pipeline{}); err == nil {
		t.Fatalf("expected Execute error")
	}
}

func TestPlannerNotImplemented(t *testing.T) {
	planner := NewPlanner()
	if planner == nil {
		t.Fatalf("expected planner")
	}
	if err := planner.Plan(); err == nil {
		t.Fatalf("expected Plan error")
	}
}
