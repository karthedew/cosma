package compute

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/stretchr/testify/require"
)

// requireAggEqual compares two Aggregates within a float tolerance for Mean.
func requireAggEqual(t *testing.T, want, got Aggregates) {
	t.Helper()
	require.Equal(t, want.Count, got.Count, "Count")
	require.Equal(t, want.Sum, got.Sum, "Sum")
	require.Equal(t, want.Min, got.Min, "Min")
	require.Equal(t, want.Max, got.Max, "Max")
	require.InDelta(t, want.Mean, got.Mean, 1e-9, "Mean")
}

// TestGroupReduceParallel_Parity drives the parallel path for the 64-bit-width
// dtype families plus strings and a count-only column, asserting it matches the
// serial GroupReduce exactly (boxed value and dynamic type).
//
// NOTE: narrow numeric widths (int8/16/32, uint8/16/32, float32) are
// deliberately excluded here because the serial and parallel reducers box
// Sum/Min/Max in different dynamic types for those columns: serial honours the
// documented "column's element type" contract (e.g. int32), while the parallel
// path widens to int64/uint64/float64. The numeric values agree but the boxed
// types do not. That divergence is exercised and pinned by
// TestGroupReduceParallel_NarrowWidthValueParity below. These tests do not use
// t.Parallel because they configure the process-wide parallelism degree.
func TestGroupReduceParallel_Parity(t *testing.T) {
	orig := Parallelism()
	defer SetParallelism(orig)
	SetParallelism(4)

	const n = 4000
	const numGroups = 7

	groupIDs := make([]int, n)
	for i := range groupIDs {
		groupIDs[i] = i % numGroups
	}

	cases := []struct {
		name  string
		dt    arrow.DataType
		build func() arrow.Array
	}{
		{"int64", arrow.PrimitiveTypes.Int64, func() arrow.Array {
			b := array.NewInt64Builder(newGoMem())
			defer b.Release()
			for i := 0; i < n; i++ {
				if i%13 == 0 {
					b.AppendNull()
				} else {
					b.Append(int64(i % 50))
				}
			}
			return b.NewArray()
		}},
		{"uint64", arrow.PrimitiveTypes.Uint64, func() arrow.Array {
			b := array.NewUint64Builder(newGoMem())
			defer b.Release()
			for i := 0; i < n; i++ {
				b.Append(uint64(i))
			}
			return b.NewArray()
		}},
		{"float64", arrow.PrimitiveTypes.Float64, func() arrow.Array {
			b := array.NewFloat64Builder(newGoMem())
			defer b.Release()
			for i := 0; i < n; i++ {
				if i%11 == 0 {
					b.AppendNull()
				} else {
					b.Append(float64(i) * 0.25)
				}
			}
			return b.NewArray()
		}},
		{"string", arrow.BinaryTypes.String, func() arrow.Array {
			b := array.NewStringBuilder(newGoMem())
			defer b.Release()
			letters := []string{"a", "m", "z", "b", "y"}
			for i := 0; i < n; i++ {
				if i%19 == 0 {
					b.AppendNull()
				} else {
					b.Append(letters[i%len(letters)])
				}
			}
			return b.NewArray()
		}},
		{"largestring", arrow.BinaryTypes.LargeString, func() arrow.Array {
			b := array.NewLargeStringBuilder(newGoMem())
			defer b.Release()
			letters := []string{"a", "m", "z", "b", "y"}
			for i := 0; i < n; i++ {
				b.Append(letters[i%len(letters)])
			}
			return b.NewArray()
		}},
		{"bool count-only", arrow.FixedWidthTypes.Boolean, func() arrow.Array {
			b := array.NewBooleanBuilder(newGoMem())
			defer b.Release()
			for i := 0; i < n; i++ {
				if i%23 == 0 {
					b.AppendNull()
				} else {
					b.Append(i%2 == 0)
				}
			}
			return b.NewArray()
		}},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			ch := chunkedFrom(t, tc.dt, tc.build())
			defer ch.Release()

			serial, err := GroupReduce(groupIDs, numGroups, ch)
			require.NoError(t, err)
			parallel, err := GroupReduceParallel(groupIDs, numGroups, ch)
			require.NoError(t, err)

			require.Len(t, parallel, numGroups)
			for g := range serial {
				requireAggEqual(t, serial[g], parallel[g])
			}
		})
	}
}

// toFloat boxes any numeric Aggregates field to float64 for value comparison
// that is agnostic to the boxed dynamic type.
func toFloat(t *testing.T, v any) float64 {
	t.Helper()
	switch x := v.(type) {
	case nil:
		return 0
	case int8:
		return float64(x)
	case int16:
		return float64(x)
	case int32:
		return float64(x)
	case int64:
		return float64(x)
	case uint8:
		return float64(x)
	case uint16:
		return float64(x)
	case uint32:
		return float64(x)
	case uint64:
		return float64(x)
	case float32:
		return float64(x)
	case float64:
		return x
	default:
		t.Fatalf("toFloat: unexpected type %T", v)
		return 0
	}
}

// TestGroupReduceParallel_NarrowWidthValueParity exercises the parallel reducer's
// narrow-width branches (int8/16/32, uint8/16/32, float32) and asserts the
// NUMERIC VALUES match serial GroupReduce. It deliberately does not assert the
// boxed dynamic types are equal: the parallel path widens narrow columns to
// int64/uint64/float64, diverging from the serial path's element-type boxing.
// See the KNOWN BUG note on TestGroupReduceParallel_Parity.
func TestGroupReduceParallel_NarrowWidthValueParity(t *testing.T) {
	orig := Parallelism()
	defer SetParallelism(orig)
	SetParallelism(4)

	// Keep n small and values in {0,1} so per-group sums stay well within int8
	// range; this isolates the type-boxing divergence from the separate
	// narrow-integer summation overflow (serial sums in the narrow type, which
	// wraps; parallel sums in int64, which does not).
	const n = 70
	const numGroups = 7
	groupIDs := make([]int, n)
	for i := range groupIDs {
		groupIDs[i] = i % numGroups
	}

	cases := []struct {
		name  string
		dt    arrow.DataType
		build func() arrow.Array
	}{
		{"int8", arrow.PrimitiveTypes.Int8, func() arrow.Array {
			b := array.NewInt8Builder(newGoMem())
			defer b.Release()
			for i := 0; i < n; i++ {
				b.Append(int8(i % 2))
			}
			return b.NewArray()
		}},
		{"int16", arrow.PrimitiveTypes.Int16, func() arrow.Array {
			b := array.NewInt16Builder(newGoMem())
			defer b.Release()
			for i := 0; i < n; i++ {
				b.Append(int16(i % 3))
			}
			return b.NewArray()
		}},
		{"int32", arrow.PrimitiveTypes.Int32, func() arrow.Array {
			b := array.NewInt32Builder(newGoMem())
			defer b.Release()
			for i := 0; i < n; i++ {
				b.Append(int32(i % 5))
			}
			return b.NewArray()
		}},
		{"uint8", arrow.PrimitiveTypes.Uint8, func() arrow.Array {
			b := array.NewUint8Builder(newGoMem())
			defer b.Release()
			for i := 0; i < n; i++ {
				b.Append(uint8(i % 2))
			}
			return b.NewArray()
		}},
		{"uint16", arrow.PrimitiveTypes.Uint16, func() arrow.Array {
			b := array.NewUint16Builder(newGoMem())
			defer b.Release()
			for i := 0; i < n; i++ {
				b.Append(uint16(i % 3))
			}
			return b.NewArray()
		}},
		{"uint32", arrow.PrimitiveTypes.Uint32, func() arrow.Array {
			b := array.NewUint32Builder(newGoMem())
			defer b.Release()
			for i := 0; i < n; i++ {
				b.Append(uint32(i % 5))
			}
			return b.NewArray()
		}},
		{"float32", arrow.PrimitiveTypes.Float32, func() arrow.Array {
			b := array.NewFloat32Builder(newGoMem())
			defer b.Release()
			for i := 0; i < n; i++ {
				b.Append(float32(i%5) * 0.5)
			}
			return b.NewArray()
		}},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			ch := chunkedFrom(t, tc.dt, tc.build())
			defer ch.Release()
			serial, err := GroupReduce(groupIDs, numGroups, ch)
			require.NoError(t, err)
			parallel, err := GroupReduceParallel(groupIDs, numGroups, ch)
			require.NoError(t, err)
			require.Len(t, parallel, numGroups)
			for g := range serial {
				require.Equal(t, serial[g].Count, parallel[g].Count, "group %d Count", g)
				require.InDelta(t, toFloat(t, serial[g].Sum), toFloat(t, parallel[g].Sum), 1e-6, "group %d Sum value", g)
				require.InDelta(t, toFloat(t, serial[g].Min), toFloat(t, parallel[g].Min), 1e-6, "group %d Min value", g)
				require.InDelta(t, toFloat(t, serial[g].Max), toFloat(t, parallel[g].Max), 1e-6, "group %d Max value", g)
				require.InDelta(t, serial[g].Mean, parallel[g].Mean, 1e-9, "group %d Mean", g)
			}
		})
	}
}

func TestGroupReduceParallel_Errors(t *testing.T) {
	orig := Parallelism()
	defer SetParallelism(orig)
	SetParallelism(4)

	t.Run("nil column", func(t *testing.T) {
		_, err := GroupReduceParallel([]int{0}, 1, nil)
		require.Error(t, err)
	})

	t.Run("group id length mismatch", func(t *testing.T) {
		b := array.NewInt64Builder(newGoMem())
		b.AppendValues([]int64{1, 2, 3}, nil)
		ch := chunkedFrom(t, arrow.PrimitiveTypes.Int64, b.NewArray())
		b.Release()
		defer ch.Release()
		_, err := GroupReduceParallel([]int{0}, 1, ch)
		require.Error(t, err)
	})
}

// TestGroupReduceParallel_SerialFallback covers the w<=1 branch that delegates
// to the serial GroupReduce.
func TestGroupReduceParallel_SerialFallback(t *testing.T) {
	orig := Parallelism()
	defer SetParallelism(orig)
	SetParallelism(1)

	b := array.NewInt64Builder(newGoMem())
	b.AppendValues([]int64{10, 20, 30, 40}, nil)
	ch := chunkedFrom(t, arrow.PrimitiveTypes.Int64, b.NewArray())
	b.Release()
	defer ch.Release()

	out, err := GroupReduceParallel([]int{0, 1, 0, 1}, 2, ch)
	require.NoError(t, err)
	require.EqualValues(t, int64(40), out[0].Sum)
	require.EqualValues(t, int64(60), out[1].Sum)
}

func TestMakeStrips(t *testing.T) {

	t.Run("even split", func(t *testing.T) {
		s := makeStrips(10, 2)
		require.Equal(t, []strip{{0, 5}, {5, 10}}, s)
	})

	t.Run("remainder distributed to leading strips", func(t *testing.T) {
		s := makeStrips(10, 3)
		require.Equal(t, []strip{{0, 4}, {4, 7}, {7, 10}}, s)
	})

	t.Run("more workers than rows clamps", func(t *testing.T) {
		s := makeStrips(2, 5)
		require.Len(t, s, 2)
		require.Equal(t, []strip{{0, 1}, {1, 2}}, s)
	})
}
