package scan

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/karthedew/cosma/carray"
	"github.com/karthedew/cosma/schema"
)

// values_internal_test.go covers the dtype->arrow mapping, the cell-width table,
// and the selection-position helpers (selAxisIsIndex / selAxisPositions), all
// package-internal.

func TestArrowTypeForDType(t *testing.T) {
	t.Parallel()

	cases := []struct {
		dt   schema.DType
		want arrow.DataType
	}{
		{schema.Bool, arrow.FixedWidthTypes.Boolean},
		{schema.Int8, arrow.PrimitiveTypes.Int8},
		{schema.Int16, arrow.PrimitiveTypes.Int16},
		{schema.Int32, arrow.PrimitiveTypes.Int32},
		{schema.Int64, arrow.PrimitiveTypes.Int64},
		{schema.UInt8, arrow.PrimitiveTypes.Uint8},
		{schema.UInt16, arrow.PrimitiveTypes.Uint16},
		{schema.UInt32, arrow.PrimitiveTypes.Uint32},
		{schema.UInt64, arrow.PrimitiveTypes.Uint64},
		{schema.Float32, arrow.PrimitiveTypes.Float32},
		{schema.Float64, arrow.PrimitiveTypes.Float64},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(string(tc.dt), func(t *testing.T) {
			t.Parallel()
			got, err := arrowTypeForDType(tc.dt)
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestArrowTypeForDTypeUnsupported(t *testing.T) {
	t.Parallel()
	for _, dt := range []schema.DType{schema.String, schema.Float16, schema.Date32, schema.List, ""} {
		dt := dt
		t.Run(string(dt), func(t *testing.T) {
			t.Parallel()
			_, err := arrowTypeForDType(dt)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "not a supported cell-scan value type")
		})
	}
}

func TestDTypeWidth(t *testing.T) {
	t.Parallel()

	cases := []struct {
		dt   schema.DType
		want int
	}{
		{schema.Bool, 1},
		{schema.Int8, 1},
		{schema.UInt8, 1},
		{schema.Int16, 2},
		{schema.UInt16, 2},
		{schema.Int32, 4},
		{schema.UInt32, 4},
		{schema.Float32, 4},
		{schema.Int64, 8},
		{schema.UInt64, 8},
		{schema.Float64, 8},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(string(tc.dt), func(t *testing.T) {
			t.Parallel()
			got, err := dtypeWidth(tc.dt)
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestDTypeWidthUnsupported(t *testing.T) {
	t.Parallel()
	for _, dt := range []schema.DType{schema.String, schema.Float16, schema.Date64, ""} {
		dt := dt
		t.Run(string(dt), func(t *testing.T) {
			t.Parallel()
			_, err := dtypeWidth(dt)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "no fixed cell width")
		})
	}
}

func TestSelAxisIsIndex(t *testing.T) {
	t.Parallel()

	sel := carray.Selection{Axes: []carray.AxisSel{
		{Index: ip(2)},
		{All: true},
		{Slice: &carray.Slice{Start: 0, Stop: 3, Step: 1}},
	}}

	assert.True(t, selAxisIsIndex(sel, 0))
	assert.False(t, selAxisIsIndex(sel, 1))
	assert.False(t, selAxisIsIndex(sel, 2))
	// Out-of-range axis (short Axes slice) means "all" -> not an index.
	assert.False(t, selAxisIsIndex(sel, 3))
	assert.False(t, selAxisIsIndex(carray.Selection{}, 0))
}

func TestSelAxisPositions(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		sel  carray.Selection
		axis int
		size int64
		want []int64
	}{
		{
			"index",
			carray.Selection{Axes: []carray.AxisSel{{Index: ip(3)}}},
			0, 10, []int64{3},
		},
		{
			"slice-step1",
			carray.Selection{Axes: []carray.AxisSel{{Slice: &carray.Slice{Start: 1, Stop: 4, Step: 1}}}},
			0, 10, []int64{1, 2, 3},
		},
		{
			"slice-stepN",
			carray.Selection{Axes: []carray.AxisSel{{Slice: &carray.Slice{Start: 0, Stop: 9, Step: 3}}}},
			0, 10, []int64{0, 3, 6},
		},
		{
			"slice-empty-stop-le-start",
			carray.Selection{Axes: []carray.AxisSel{{Slice: &carray.Slice{Start: 5, Stop: 5, Step: 1}}}},
			0, 10, nil,
		},
		{
			"slice-bad-step",
			carray.Selection{Axes: []carray.AxisSel{{Slice: &carray.Slice{Start: 0, Stop: 5, Step: 0}}}},
			0, 10, nil,
		},
		{
			"indices",
			carray.Selection{Axes: []carray.AxisSel{{Indices: []int64{4, 1, 7}}}},
			0, 10, []int64{4, 1, 7},
		},
		{
			"all-explicit",
			carray.Selection{Axes: []carray.AxisSel{{All: true}}},
			0, 3, []int64{0, 1, 2},
		},
		{
			"all-short-axes",
			carray.Selection{},
			0, 4, []int64{0, 1, 2, 3},
		},
		{
			"all-axis-out-of-range",
			carray.Selection{Axes: []carray.AxisSel{{Index: ip(0)}}},
			1, 2, []int64{0, 1},
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := selAxisPositions(tc.sel, tc.axis, tc.size)
			assert.Equal(t, tc.want, got)
		})
	}
}

// TestSelAxisPositionsIndicesIsCopy confirms the returned slice for Indices is a
// fresh copy (mutating it must not corrupt the selection).
func TestSelAxisPositionsIndicesIsCopy(t *testing.T) {
	t.Parallel()
	idx := []int64{1, 2, 3}
	sel := carray.Selection{Axes: []carray.AxisSel{{Indices: idx}}}
	got := selAxisPositions(sel, 0, 10)
	got[0] = 99
	assert.Equal(t, int64(1), idx[0])
}
