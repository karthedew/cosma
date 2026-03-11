package exec

import "fmt"

// Executor runs pipelines with concurrency controls.
type Executor struct {
	// TODO: worker pool config
}

func NewExecutor() *Executor { return &Executor{} }

func (e *Executor) Execute(p *Pipeline) error {
	return fmt.Errorf("exec.Executor.Execute not implemented")
}
