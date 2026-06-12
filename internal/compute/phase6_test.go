package compute

import (
	"context"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/karthedew/cosma/expr"
)

// TB1: SetParallelism / Parallelism round-trip.
func TestTB1_SetParallelism(t *testing.T) {
	t.Parallel()

	// Default is 0 (GOMAXPROCS at call time).
	orig := Parallelism()
	defer SetParallelism(orig)

	SetParallelism(4)
	if got := Parallelism(); got != 4 {
		t.Fatalf("Parallelism() = %d, want 4", got)
	}

	SetParallelism(1)
	if got := Parallelism(); got != 1 {
		t.Fatalf("Parallelism() = %d after SetParallelism(1), want 1", got)
	}

	// Negative clamps to 1.
	SetParallelism(-3)
	if got := Parallelism(); got != 1 {
		t.Fatalf("Parallelism() = %d after SetParallelism(-3), want 1", got)
	}

	// 0 restores GOMAXPROCS-at-call behaviour.
	SetParallelism(0)
	if got := Parallelism(); got != 0 {
		t.Fatalf("Parallelism() = %d after SetParallelism(0), want 0", got)
	}
	// workers() should be positive.
	if w := workers(); w <= 0 {
		t.Fatalf("workers() = %d, want > 0", w)
	}
}

// TB2: EvalParallel produces the same filtered records as the serial path.
func TestTB2_EvalParallel_Correctness(t *testing.T) {
	t.Parallel()

	mem := memory.DefaultAllocator
	const batchSize = 100
	const numBatches = 8

	// Build numBatches record batches each with batchSize rows.
	batches := make([]arrow.Record, numBatches)
	for b := 0; b < numBatches; b++ {
		vals := make([]int64, batchSize)
		for i := range vals {
			vals[i] = int64(b*batchSize + i)
		}
		batches[b] = int64Record(t, "v", vals)
	}
	defer func() {
		for _, r := range batches {
			r.Release()
		}
	}()

	// Predicate: v > 400
	pred := expr.Col("v").Gt(expr.Lit(int64(400))).Node

	// Serial reference.
	var serialRecs []arrow.Record
	for _, rec := range batches {
		mask, err := Eval(pred, rec, mem)
		if err != nil {
			t.Fatalf("serial Eval: %v", err)
		}
		out, err := FilterRecord(rec, mask, mem)
		mask.Release()
		if err != nil {
			t.Fatalf("serial FilterRecord: %v", err)
		}
		serialRecs = append(serialRecs, out)
	}
	defer func() {
		for _, r := range serialRecs {
			r.Release()
		}
	}()

	// Parallel path.
	SetParallelism(4)
	defer SetParallelism(0)

	parallelRecs, err := EvalParallel(context.Background(), pred, batches, mem)
	if err != nil {
		t.Fatalf("EvalParallel: %v", err)
	}
	defer func() {
		for _, r := range parallelRecs {
			r.Release()
		}
	}()

	if len(parallelRecs) != len(serialRecs) {
		t.Fatalf("len(parallel) = %d, len(serial) = %d", len(parallelRecs), len(serialRecs))
	}
	for i := range serialRecs {
		if parallelRecs[i].NumRows() != serialRecs[i].NumRows() {
			t.Fatalf("batch %d: parallel rows %d != serial rows %d",
				i, parallelRecs[i].NumRows(), serialRecs[i].NumRows())
		}
		// Verify column values match.
		pc := parallelRecs[i].Column(0).(*array.Int64)
		sc := serialRecs[i].Column(0).(*array.Int64)
		for j := 0; j < pc.Len(); j++ {
			if pc.Value(j) != sc.Value(j) {
				t.Fatalf("batch %d row %d: parallel %d != serial %d", i, j, pc.Value(j), sc.Value(j))
			}
		}
	}
}

// TB3: GroupReduceParallel produces results identical to GroupReduce.
func TestTB3_GroupReduceParallel_Correctness(t *testing.T) {
	t.Parallel()

	const n = 10_000
	const numGroups = 5

	// Build an int64 chunked column and group-id assignment.
	vals := make([]int64, n)
	groupIDs := make([]int, n)
	for i := range vals {
		vals[i] = int64(i)
		groupIDs[i] = i % numGroups
	}

	bld := array.NewInt64Builder(memory.DefaultAllocator)
	defer bld.Release()
	bld.AppendValues(vals, nil)
	arr := bld.NewArray()
	defer arr.Release()
	chunked := arrow.NewChunked(arr.DataType(), []arrow.Array{arr})
	defer chunked.Release()

	serial, err := GroupReduce(groupIDs, numGroups, chunked)
	if err != nil {
		t.Fatalf("GroupReduce: %v", err)
	}

	SetParallelism(4)
	defer SetParallelism(0)

	parallel, err := GroupReduceParallel(groupIDs, numGroups, chunked)
	if err != nil {
		t.Fatalf("GroupReduceParallel: %v", err)
	}

	if len(parallel) != len(serial) {
		t.Fatalf("len(parallel) = %d, len(serial) = %d", len(parallel), len(serial))
	}
	for g := range serial {
		s, p := serial[g], parallel[g]
		if s.Count != p.Count {
			t.Fatalf("group %d: Count serial=%d parallel=%d", g, s.Count, p.Count)
		}
		if s.Sum != p.Sum {
			t.Fatalf("group %d: Sum serial=%v parallel=%v", g, s.Sum, p.Sum)
		}
		if s.Min != p.Min {
			t.Fatalf("group %d: Min serial=%v parallel=%v", g, s.Min, p.Min)
		}
		if s.Max != p.Max {
			t.Fatalf("group %d: Max serial=%v parallel=%v", g, s.Max, p.Max)
		}
		// Mean: compare within floating-point tolerance.
		if diff := s.Mean - p.Mean; diff > 1e-9 || diff < -1e-9 {
			t.Fatalf("group %d: Mean serial=%v parallel=%v", g, s.Mean, p.Mean)
		}
	}
}

// TB4: LastMetrics is populated and plausible after EvalParallel.
func TestTB4_LastMetrics(t *testing.T) {
	t.Parallel()

	mem := memory.DefaultAllocator
	const batchSize = 200
	const numBatches = 4

	batches := make([]arrow.Record, numBatches)
	for b := 0; b < numBatches; b++ {
		vals := make([]int64, batchSize)
		for i := range vals {
			vals[i] = int64(i)
		}
		batches[b] = int64Record(t, "x", vals)
	}
	defer func() {
		for _, r := range batches {
			r.Release()
		}
	}()

	SetParallelism(2)
	defer SetParallelism(0)

	pred := expr.Col("x").Gt(expr.Lit(int64(50))).Node
	out, err := EvalParallel(context.Background(), pred, batches, mem)
	if err != nil {
		t.Fatalf("EvalParallel: %v", err)
	}
	defer func() {
		for _, r := range out {
			r.Release()
		}
	}()

	m := LastMetrics()
	if m.Op != "filter" {
		t.Fatalf("Op = %q, want \"filter\"", m.Op)
	}
	if m.Workers < 1 {
		t.Fatalf("Workers = %d, want >= 1", m.Workers)
	}
	wantRows := int64(numBatches * batchSize)
	if m.Rows != wantRows {
		t.Fatalf("Rows = %d, want %d", m.Rows, wantRows)
	}
	if m.Elapsed <= 0 {
		t.Fatalf("Elapsed = %v, want > 0", m.Elapsed)
	}
	if m.Elapsed > 10*time.Second {
		t.Fatalf("Elapsed = %v, suspiciously large", m.Elapsed)
	}
}
