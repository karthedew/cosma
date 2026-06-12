package compute

import (
	"runtime"
	"sync/atomic"
)

// parallelism is the number of goroutines used by parallel kernels.
// 0 means "use GOMAXPROCS at call time."
var parallelism atomic.Int32

// SetParallelism sets the number of goroutines that EvalParallel and
// GroupReduceParallel will use. A value of 0 (the default) means GOMAXPROCS is
// queried at each call, so it tracks runtime changes. A value of 1 disables
// all parallelism (serial execution). Negative values are clamped to 1.
func SetParallelism(n int) {
	if n < 0 {
		n = 1
	}
	parallelism.Store(int32(n))
}

// Parallelism returns the configured degree of parallelism. 0 means
// "GOMAXPROCS at call time."
func Parallelism() int {
	return int(parallelism.Load())
}

// workers returns the effective worker count for the current call.
func workers() int {
	p := Parallelism()
	if p <= 0 {
		return runtime.GOMAXPROCS(0)
	}
	return p
}
