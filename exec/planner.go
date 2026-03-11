package exec

import "fmt"

// Planner will convert expr trees into a physical pipeline.
// TODO: implement logical -> physical planning.
type Planner struct{}

func NewPlanner() *Planner { return &Planner{} }

func (p *Planner) Plan() error {
	return fmt.Errorf("exec.Planner.Plan not implemented")
}
