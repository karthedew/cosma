package compute

import (
	"sync"
	"time"
)

// OpMetrics captures timing and throughput for a single parallel kernel
// invocation. It is updated by EvalParallel and GroupReduceParallel and can be
// read via LastMetrics().
type OpMetrics struct {
	// Op is a short human-readable label for the operation (e.g. "filter",
	// "groupby").
	Op string

	// Workers is the number of goroutines that ran.
	Workers int

	// Rows is the number of logical rows processed.
	Rows int64

	// Elapsed is the wall-clock time from start of fan-out to completion of
	// all workers.
	Elapsed time.Duration
}

// lastMetricsMu guards lastMetrics.
var lastMetricsMu sync.RWMutex
var lastMetricsVal OpMetrics

// storeMetrics replaces the globally-visible last metrics.
func storeMetrics(m OpMetrics) {
	lastMetricsMu.Lock()
	lastMetricsVal = m
	lastMetricsMu.Unlock()
}

// LastMetrics returns the OpMetrics recorded by the most recent parallel
// kernel call in this process. It is a snapshot; concurrent calls may
// overwrite it.
func LastMetrics() OpMetrics {
	lastMetricsMu.RLock()
	defer lastMetricsMu.RUnlock()
	return lastMetricsVal
}
