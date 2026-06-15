package schema

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// allArrowDTypeCases enumerates every Arrow data type the translate switch
// handles, paired with the Cosma DType it must map to. Each entry exercises one
// branch of DTypeFromArrow.
func allArrowDTypeCases() []struct {
	name string
	dt   arrow.DataType
	want DType
} {
	return []struct {
		name string
		dt   arrow.DataType
		want DType
	}{
		{"null", arrow.Null, Null},
		{"bool", arrow.FixedWidthTypes.Boolean, Bool},
		{"int8", arrow.PrimitiveTypes.Int8, Int8},
		{"int16", arrow.PrimitiveTypes.Int16, Int16},
		{"int32", arrow.PrimitiveTypes.Int32, Int32},
		{"int64", arrow.PrimitiveTypes.Int64, Int64},
		{"uint8", arrow.PrimitiveTypes.Uint8, UInt8},
		{"uint16", arrow.PrimitiveTypes.Uint16, UInt16},
		{"uint32", arrow.PrimitiveTypes.Uint32, UInt32},
		{"uint64", arrow.PrimitiveTypes.Uint64, UInt64},
		{"float16", arrow.FixedWidthTypes.Float16, Float16},
		{"float32", arrow.PrimitiveTypes.Float32, Float32},
		{"float64", arrow.PrimitiveTypes.Float64, Float64},
		{"utf8", arrow.BinaryTypes.String, Utf8},
		{"large_utf8", arrow.BinaryTypes.LargeString, LargeUtf8},
		{"string_view", arrow.BinaryTypes.StringView, StringView},
		{"binary", arrow.BinaryTypes.Binary, Binary},
		{"large_binary", arrow.BinaryTypes.LargeBinary, LargeBinary},
		{"fixed_size_binary", &arrow.FixedSizeBinaryType{ByteWidth: 16}, FixedSizeBinary},
		{"binary_view", arrow.BinaryTypes.BinaryView, BinaryView},
		{"date32", arrow.FixedWidthTypes.Date32, Date32},
		{"date64", arrow.FixedWidthTypes.Date64, Date64},
		{"time32", arrow.FixedWidthTypes.Time32s, Time32},
		{"time64", arrow.FixedWidthTypes.Time64ns, Time64},
		{"timestamp", arrow.FixedWidthTypes.Timestamp_us, Timestamp},
		{"duration", arrow.FixedWidthTypes.Duration_ns, Duration},
		{"interval_months", arrow.FixedWidthTypes.MonthInterval, IntervalMonth},
		{"interval_day_time", arrow.FixedWidthTypes.DayTimeInterval, IntervalDayTime},
		{"interval_month_day_nano", arrow.FixedWidthTypes.MonthDayNanoInterval, IntervalMDN},
		{"decimal128", &arrow.Decimal128Type{Precision: 38, Scale: 10}, Decimal128},
		{"decimal256", &arrow.Decimal256Type{Precision: 76, Scale: 20}, Decimal256},
		{"list", arrow.ListOf(arrow.PrimitiveTypes.Int32), List},
		{"large_list", arrow.LargeListOf(arrow.PrimitiveTypes.Int32), LargeList},
		{"fixed_size_list", arrow.FixedSizeListOf(3, arrow.PrimitiveTypes.Int32), FixedSizeList},
		{"list_view", arrow.ListViewOf(arrow.PrimitiveTypes.Int32), ListView},
		{"large_list_view", arrow.LargeListViewOf(arrow.PrimitiveTypes.Int32), LargeListView},
		{"struct", arrow.StructOf(arrow.Field{Name: "x", Type: arrow.PrimitiveTypes.Int64}), Struct},
		{"map", arrow.MapOf(arrow.BinaryTypes.String, arrow.PrimitiveTypes.Int64), Map},
		{
			"sparse_union",
			arrow.UnionOf(arrow.SparseMode,
				[]arrow.Field{{Name: "a", Type: arrow.PrimitiveTypes.Int32}},
				[]arrow.UnionTypeCode{0}),
			SparseUnion,
		},
		{
			"dense_union",
			arrow.UnionOf(arrow.DenseMode,
				[]arrow.Field{{Name: "a", Type: arrow.PrimitiveTypes.Int32}},
				[]arrow.UnionTypeCode{0}),
			DenseUnion,
		},
		{
			"dictionary",
			&arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int32, ValueType: arrow.BinaryTypes.String},
			Dictionary,
		},
		{
			"run_end_encoded",
			arrow.RunEndEncodedOf(arrow.PrimitiveTypes.Int32, arrow.PrimitiveTypes.Int64),
			RunEndEncoded,
		},
	}
}

func TestDTypeFromArrowAllBranches(t *testing.T) {
	t.Parallel()
	for _, c := range allArrowDTypeCases() {
		c := c
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			got, err := DTypeFromArrow(c.dt)
			require.NoError(t, err)
			assert.Equal(t, c.want, got)
		})
	}
}

func TestDTypeFromArrowErrors(t *testing.T) {
	t.Parallel()

	t.Run("nil type", func(t *testing.T) {
		t.Parallel()
		_, err := DTypeFromArrow(nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "nil arrow type")
	})

	t.Run("unsupported type", func(t *testing.T) {
		t.Parallel()
		// Decimal32 is a valid Arrow type whose ID is not handled by the
		// translate switch, so it must fall through to the default branch.
		dt := &arrow.Decimal32Type{Precision: 9, Scale: 2}
		_, err := DTypeFromArrow(dt)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unsupported arrow type")
	})
}

func TestFieldFromArrow(t *testing.T) {
	t.Parallel()

	t.Run("ok", func(t *testing.T) {
		t.Parallel()
		f, err := FieldFromArrow("x", arrow.PrimitiveTypes.Int32)
		require.NoError(t, err)
		assert.Equal(t, "x", f.Name)
		assert.Equal(t, Int32, f.Type)
		assert.False(t, f.Nullable, "FieldFromArrow builds non-nullable fields")
		assert.Same(t, arrow.PrimitiveTypes.Int32, f.ArrowType)
	})

	t.Run("nil arrow type", func(t *testing.T) {
		t.Parallel()
		_, err := FieldFromArrow("x", nil)
		require.Error(t, err)
	})

	t.Run("unsupported arrow type", func(t *testing.T) {
		t.Parallel()
		_, err := FieldFromArrow("x", &arrow.Decimal32Type{Precision: 9, Scale: 2})
		require.Error(t, err)
	})
}

// TestFromArrowToArrowRoundTrip walks every supported Arrow type through
// FromArrow -> ToArrow and asserts the resulting Arrow schema is identical,
// proving ArrowType is carried through losslessly.
func TestFromArrowToArrowRoundTrip(t *testing.T) {
	t.Parallel()
	for _, c := range allArrowDTypeCases() {
		c := c
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			arr := arrow.NewSchema([]arrow.Field{
				{Name: "f", Type: c.dt, Nullable: true},
			}, nil)

			cs, err := FromArrow(arr)
			require.NoError(t, err)

			f, ok := cs.Field("f")
			require.True(t, ok)
			assert.Equal(t, c.want, f.Type)
			assert.True(t, f.Nullable)
			require.NotNil(t, f.ArrowType)

			back, err := ToArrow(cs)
			require.NoError(t, err)
			assert.True(t, back.Equal(arr), "round trip mismatch: got %s want %s", back, arr)
		})
	}
}

func TestFromArrowPreservesOrderAndNullability(t *testing.T) {
	t.Parallel()
	arr := arrow.NewSchema([]arrow.Field{
		{Name: "a", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "b", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "c", Type: arrow.FixedWidthTypes.Boolean, Nullable: false},
	}, nil)

	cs, err := FromArrow(arr)
	require.NoError(t, err)
	require.Equal(t, 3, cs.Len())

	fields := cs.Fields()
	assert.Equal(t, "a", fields[0].Name)
	assert.Equal(t, "b", fields[1].Name)
	assert.Equal(t, "c", fields[2].Name)
	assert.False(t, fields[0].Nullable)
	assert.True(t, fields[1].Nullable)
	assert.False(t, fields[2].Nullable)
}

func TestFromArrowEmptySchema(t *testing.T) {
	t.Parallel()
	arr := arrow.NewSchema([]arrow.Field{}, nil)
	cs, err := FromArrow(arr)
	require.NoError(t, err)
	assert.Equal(t, 0, cs.Len())

	back, err := ToArrow(cs)
	require.NoError(t, err)
	assert.True(t, back.Equal(arr))
}

func TestFromArrowPropagatesFieldError(t *testing.T) {
	t.Parallel()
	arr := arrow.NewSchema([]arrow.Field{
		{Name: "ok", Type: arrow.PrimitiveTypes.Int64},
		{Name: "bad", Type: &arrow.Decimal32Type{Precision: 9, Scale: 2}},
	}, nil)
	_, err := FromArrow(arr)
	require.Error(t, err)
	assert.Contains(t, err.Error(), `"bad"`)
}

func TestFromArrowNil(t *testing.T) {
	t.Parallel()
	_, err := FromArrow(nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "nil")
}

func TestToArrowErrors(t *testing.T) {
	t.Parallel()

	t.Run("nil schema", func(t *testing.T) {
		t.Parallel()
		_, err := ToArrow(nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "nil")
	})

	t.Run("field with nil arrow type", func(t *testing.T) {
		t.Parallel()
		// A field constructed directly (not via FromArrow/FieldFromArrow) may
		// carry a nil ArrowType, which ToArrow must reject.
		s := New(Field{Name: "x", Type: Int64})
		_, err := ToArrow(s)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "nil arrow type")
		assert.Contains(t, err.Error(), `"x"`)
	})
}
