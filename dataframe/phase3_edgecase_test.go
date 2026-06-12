package dataframe_test

// Phase 3 edge-case validation tests.
// Tests A–D probe specific contract corners of Take, Join, and context
// cancellation introduced in Phase 3.
// Run with: go test -run TestPhase3 ./dataframe/

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"
	"github.com/apache/arrow/go/v18/arrow/memory"
	"github.com/stretchr/testify/require"

	"github.com/karthedew/cosma/dataframe"
	"github.com/karthedew/cosma/internal/compute"
)

// TestPhase3A_TakeAllNullChunk verifies that Take correctly propagates nulls
// when the source chunked array's first chunk is entirely null.
//
// Layout: chunk0 = [null, null, null], chunk1 = [10, 20].
// Taking indices [0, 1, 2, 3, 4] should yield [null, null, null, 10, 20].
// Taking only chunk0 indices [0, 1] should yield [null, null].
func TestPhase3A_TakeAllNullChunk(t *testing.T) {
	t.Parallel()
	mem := memory.NewGoAllocator()

	// Build chunk0: three explicit nulls.
	b0 := array.NewInt64Builder(mem)
	defer b0.Release()
	b0.AppendNull()
	b0.AppendNull()
	b0.AppendNull()
	chunk0 := b0.NewArray()
	defer chunk0.Release()

	// Build chunk1: two non-null values.
	b1 := array.NewInt64Builder(mem)
	defer b1.Release()
	b1.Append(10)
	b1.Append(20)
	chunk1 := b1.NewArray()
	defer chunk1.Release()

	chunked := arrow.NewChunked(arrow.PrimitiveTypes.Int64, []arrow.Array{chunk0, chunk1})
	defer chunked.Release()

	// Take all five rows in order — should preserve all three nulls.
	out, err := compute.Take(chunked, []int64{0, 1, 2, 3, 4}, mem)
	require.NoError(t, err)
	defer out.Release()

	col := out.(*array.Int64)
	require.Equal(t, 5, col.Len())

	// First three positions come from the all-null chunk.
	require.True(t, col.IsNull(0), "index 0 (all-null chunk) must be null")
	require.True(t, col.IsNull(1), "index 1 (all-null chunk) must be null")
	require.True(t, col.IsNull(2), "index 2 (all-null chunk) must be null")

	// Positions 3 and 4 come from chunk1 and must have values.
	require.False(t, col.IsNull(3), "index 3 must not be null")
	require.Equal(t, int64(10), col.Value(3))
	require.False(t, col.IsNull(4), "index 4 must not be null")
	require.Equal(t, int64(20), col.Value(4))

	// Take only indices from the all-null chunk — output must be all null.
	out2, err := compute.Take(chunked, []int64{2, 0}, mem)
	require.NoError(t, err)
	defer out2.Release()

	col2 := out2.(*array.Int64)
	require.Equal(t, 2, col2.Len())
	require.True(t, col2.IsNull(0), "reversed index 2 (null chunk) must be null")
	require.True(t, col2.IsNull(1), "reversed index 0 (null chunk) must be null")
}

// TestPhase3B_JoinOnFloat64Key verifies that Join works on a float64 key
// column and that NaN keys do NOT match each other (SQL semantics: NaN != NaN,
// just as NULL != NULL).
//
// If NaN matches NaN this test flags it as a bug because the join
// implementation uses a Go map[any]... and NaN in Go maps never matches any
// lookup (NaN != NaN via IEEE-754), so the correct behaviour should be
// zero NaN matches.
func TestPhase3B_JoinOnFloat64Key(t *testing.T) {
	t.Parallel()

	// --- normal float64 join ---
	leftKeys, err := dataframe.NewSeries("score", []float64{1.5, 2.5, 3.5})
	require.NoError(t, err)
	leftVals, err := dataframe.NewSeries("lv", []int64{10, 20, 30})
	require.NoError(t, err)
	left, err := dataframe.New([]*dataframe.Series{leftKeys, leftVals})
	require.NoError(t, err)

	rightKeys, err := dataframe.NewSeries("score", []float64{2.5, 3.5, 9.9})
	require.NoError(t, err)
	rightVals, err := dataframe.NewSeries("rv", []int64{200, 300, 999})
	require.NoError(t, err)
	right, err := dataframe.New([]*dataframe.Series{rightKeys, rightVals})
	require.NoError(t, err)

	got, err := left.Join(context.Background(), right, "score", "inner")
	require.NoError(t, err, "Join on float64 key must succeed")

	// Only rows with score 2.5 and 3.5 match.
	require.EqualValues(t, 2, got.NumRows(), "inner join should produce 2 rows")
	require.Equal(t, []string{"score", "lv", "rv"}, got.Columns())

	// --- NaN key semantics ---
	nan := math.NaN()

	leftNaNKeys, err := dataframe.NewSeries("k", []float64{nan, 1.0})
	require.NoError(t, err)
	leftNaNVals, err := dataframe.NewSeries("lv", []int64{100, 200})
	require.NoError(t, err)
	leftNaN, err := dataframe.New([]*dataframe.Series{leftNaNKeys, leftNaNVals})
	require.NoError(t, err)

	rightNaNKeys, err := dataframe.NewSeries("k", []float64{nan, 1.0})
	require.NoError(t, err)
	rightNaNVals, err := dataframe.NewSeries("rv", []int64{999, 111})
	require.NoError(t, err)
	rightNaN, err := dataframe.New([]*dataframe.Series{rightNaNKeys, rightNaNVals})
	require.NoError(t, err)

	nanJoin, err := leftNaN.Join(context.Background(), rightNaN, "k", "inner")
	require.NoError(t, err)

	// NaN must NOT match NaN. Only k=1.0 should produce a row.
	// If nanJoin has 2 rows the implementation matched NaN to NaN — that is a bug.
	if nanJoin.NumRows() == 2 {
		t.Fatal("BUG: NaN matched NaN in join key (violates SQL NULL!=NULL semantics); got 2 rows, want 1")
	}
	require.EqualValues(t, 1, nanJoin.NumRows(),
		"NaN key must NOT match NaN; only k=1.0 should produce a result row")
}

// TestPhase3C_LeftJoinSchema verifies that after a left join the output schema
// is exactly [left_cols..., right_non_key_cols...] — the right key column is
// dropped and no phantom columns are added.
//
// Left:  [id int64, lv string]   (2 cols)
// Right: [id int64, rv int64]    (2 cols, key = id)
//
// Expected output schema: [id, lv, rv]  (3 cols — right key dropped)
func TestPhase3C_LeftJoinSchema(t *testing.T) {
	t.Parallel()

	leftID, err := dataframe.NewSeries("id", []int64{1, 2, 3})
	require.NoError(t, err)
	leftVal, err := dataframe.NewSeries("lv", []string{"a", "b", "c"})
	require.NoError(t, err)
	left, err := dataframe.New([]*dataframe.Series{leftID, leftVal})
	require.NoError(t, err)

	rightID, err := dataframe.NewSeries("id", []int64{1, 3})
	require.NoError(t, err)
	rightVal, err := dataframe.NewSeries("rv", []int64{100, 300})
	require.NoError(t, err)
	right, err := dataframe.New([]*dataframe.Series{rightID, rightVal})
	require.NoError(t, err)

	got, err := left.Join(context.Background(), right, "id", "left")
	require.NoError(t, err)

	// Schema must be exactly [id, lv, rv] — right key dropped, nothing extra.
	require.Equal(t, []string{"id", "lv", "rv"}, got.Columns(),
		"left join schema must be [left_cols..., right_non_key_cols...]; right key must be dropped")

	// Row count must equal the left frame's row count (left join keeps all left rows).
	require.EqualValues(t, 3, got.NumRows())

	// id=2 has no right match; rv must be null for that row.
	rvVals, rvNulls := nullableInt64Col(t, got, "rv")
	require.Equal(t, []bool{false, true, false}, rvNulls,
		"rv must be null only for the unmatched left row (id=2)")
	require.Equal(t, int64(100), rvVals[0])
	require.Equal(t, int64(300), rvVals[2])
}

// TestPhase3D_JoinContextCancelMidProbe starts a join between two large
// DataFrames (50 000 rows each) and cancels the context after 1 ms via
// time.AfterFunc. The join must return a non-nil error that satisfies
// errors.Is(err, context.Canceled) rather than completing silently.
//
// The join probes ctx.Err() every 4096 rows, so a 50K-row table provides
// ~12 check-points. With a 1 ms cancel the context will be cancelled well
// before the probe loop finishes (a 50K-row join takes tens of milliseconds
// on any machine).
func TestPhase3D_JoinContextCancelMidProbe(t *testing.T) {
	t.Parallel()

	const n = 50_000

	ids := make([]int64, n)
	for i := range ids {
		ids[i] = int64(i)
	}

	leftID, err := dataframe.NewSeries("id", ids)
	require.NoError(t, err)
	leftVal, err := dataframe.NewSeries("lv", ids) // reuse same slice value
	require.NoError(t, err)
	left, err := dataframe.New([]*dataframe.Series{leftID, leftVal})
	require.NoError(t, err)

	rightID, err := dataframe.NewSeries("id", ids)
	require.NoError(t, err)
	rightVal, err := dataframe.NewSeries("rv", ids)
	require.NoError(t, err)
	right, err := dataframe.New([]*dataframe.Series{rightID, rightVal})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	// Cancel after 1 ms — the build phase finishes in microseconds; the probe
	// loop over 50K rows takes far longer, so we cancel mid-probe.
	time.AfterFunc(1*time.Millisecond, cancel)
	defer cancel()

	_, err = left.Join(ctx, right, "id", "inner")
	require.Error(t, err, "Join must return an error when context is cancelled mid-probe")
	require.ErrorIs(t, err, context.Canceled,
		"Join error must wrap context.Canceled when context was cancelled")
}
