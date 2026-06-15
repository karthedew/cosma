package scan

import (
	"encoding/binary"
	"math"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fillvalue_internal_test.go covers coerceFillValue / encodeFillValue across
// every supported dtype plus the rejection paths (wrong Go type, fractional
// float on an integer column, unsupported dtype). These are package-internal
// helpers, so the test lives in package scan.

func TestCoerceFillValueByDType(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		v    any
		dt   arrow.DataType
		want any
	}{
		// Booleans round-trip exactly.
		{"bool-true", true, arrow.FixedWidthTypes.Boolean, true},
		{"bool-false", false, arrow.FixedWidthTypes.Boolean, false},

		// Integer columns accept integral floats (the JSON-decoded shape) and
		// narrow to the column's concrete Go type.
		{"int8-from-float", float64(5), arrow.PrimitiveTypes.Int8, int8(5)},
		{"int16-from-float", float64(-300), arrow.PrimitiveTypes.Int16, int16(-300)},
		{"int32-from-float", float64(70000), arrow.PrimitiveTypes.Int32, int32(70000)},
		{"int64-from-float", float64(1 << 40), arrow.PrimitiveTypes.Int64, int64(1 << 40)},
		{"uint8-from-float", float64(200), arrow.PrimitiveTypes.Uint8, uint8(200)},
		{"uint16-from-float", float64(60000), arrow.PrimitiveTypes.Uint16, uint16(60000)},
		{"uint32-from-float", float64(4000000000), arrow.PrimitiveTypes.Uint32, uint32(4000000000)},
		{"uint64-from-float", float64(1 << 40), arrow.PrimitiveTypes.Uint64, uint64(1 << 40)},

		// Integer columns also accept native Go integers.
		{"int32-from-int", int(42), arrow.PrimitiveTypes.Int32, int32(42)},
		{"int64-from-int64", int64(-9), arrow.PrimitiveTypes.Int64, int64(-9)},

		// Float columns accept floats and integral or non-integral values.
		{"float32-from-float", float64(1.5), arrow.PrimitiveTypes.Float32, float32(1.5)},
		{"float64-from-float", float64(2.25), arrow.PrimitiveTypes.Float64, float64(2.25)},
		{"float64-from-int", int64(7), arrow.PrimitiveTypes.Float64, float64(7)},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := coerceFillValue(tc.v, tc.dt)
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestCoerceFillValueErrors(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		v    any
		dt   arrow.DataType
	}{
		{"bool-wrong-type", float64(1), arrow.FixedWidthTypes.Boolean},
		{"int-from-string", "nope", arrow.PrimitiveTypes.Int32},
		{"int-from-bool", true, arrow.PrimitiveTypes.Int64},
		{"int8-fractional-float", float64(1.5), arrow.PrimitiveTypes.Int8},
		{"int64-fractional-float", float64(3.14), arrow.PrimitiveTypes.Int64},
		{"uint16-fractional-float", float64(2.5), arrow.PrimitiveTypes.Uint16},
		{"float-from-string", "x", arrow.PrimitiveTypes.Float64},
		{"unsupported-dtype", float64(1), arrow.BinaryTypes.String},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := coerceFillValue(tc.v, tc.dt)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "not assignable to value type")
		})
	}
}

// TestFillIntAllGoIntTypes drives every fillInt branch by feeding each native Go
// integer kind into an int64 column.
func TestFillIntAllGoIntTypes(t *testing.T) {
	t.Parallel()
	dt := arrow.PrimitiveTypes.Int64
	cases := []struct {
		name string
		v    any
	}{
		{"int", int(1)},
		{"int8", int8(2)},
		{"int16", int16(3)},
		{"int32", int32(4)},
		{"int64", int64(5)},
		{"uint", uint(6)},
		{"uint8", uint8(7)},
		{"uint16", uint16(8)},
		{"uint32", uint32(9)},
		{"uint64", uint64(10)},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := coerceFillValue(tc.v, dt)
			require.NoError(t, err)
			assert.IsType(t, int64(0), got)
		})
	}
}

// TestFillFloatAllGoTypes drives every fillFloat branch via a float64 column.
func TestFillFloatAllGoTypes(t *testing.T) {
	t.Parallel()
	dt := arrow.PrimitiveTypes.Float64
	cases := []struct {
		name string
		v    any
		want float64
	}{
		{"float64", float64(1.5), 1.5},
		{"float32", float32(2.5), 2.5},
		{"int64", int64(3), 3},
		{"int32", int32(4), 4},
		{"int", int(5), 5},
		{"uint64", uint64(6), 6},
		{"uint", uint(7), 7},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := coerceFillValue(tc.v, dt)
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}

	// A Go type fillFloat does not list (uint8) is rejected on a float column.
	_, err := coerceFillValue(uint8(3), dt)
	require.Error(t, err)
}

func TestFillIntFractionalFloat32Rejected(t *testing.T) {
	t.Parallel()
	// float32 with a fractional part is rejected; an integral float32 is accepted.
	got, err := coerceFillValue(float32(8), arrow.PrimitiveTypes.Int32)
	require.NoError(t, err)
	assert.Equal(t, int32(8), got)

	_, err = coerceFillValue(float32(8.5), arrow.PrimitiveTypes.Int32)
	require.Error(t, err)
}

func TestEncodeFillValueRoundTrip(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		v    any
		dt   arrow.DataType
		want []byte
	}{
		{"bool-true", true, arrow.FixedWidthTypes.Boolean, []byte{1}},
		{"bool-false", false, arrow.FixedWidthTypes.Boolean, []byte{0}},
		{"int8", int8(-1), arrow.PrimitiveTypes.Int8, []byte{0xff}},
		{"uint8", uint8(0x7f), arrow.PrimitiveTypes.Uint8, []byte{0x7f}},
		{"int16", int16(0x0102), arrow.PrimitiveTypes.Int16, []byte{0x02, 0x01}},
		{"uint16", uint16(0x0102), arrow.PrimitiveTypes.Uint16, []byte{0x02, 0x01}},
		{"int32", int32(0x01020304), arrow.PrimitiveTypes.Int32, []byte{0x04, 0x03, 0x02, 0x01}},
		{"uint32", uint32(0x01020304), arrow.PrimitiveTypes.Uint32, []byte{0x04, 0x03, 0x02, 0x01}},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := encodeFillValue(tc.v, tc.dt)
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestEncodeFillValueWideTypes(t *testing.T) {
	t.Parallel()

	t.Run("int64", func(t *testing.T) {
		t.Parallel()
		got, err := encodeFillValue(int64(-2), arrow.PrimitiveTypes.Int64)
		require.NoError(t, err)
		require.Len(t, got, 8)
		assert.Equal(t, uint64(0xfffffffffffffffe), binary.LittleEndian.Uint64(got))
	})

	t.Run("uint64", func(t *testing.T) {
		t.Parallel()
		got, err := encodeFillValue(uint64(1<<40), arrow.PrimitiveTypes.Uint64)
		require.NoError(t, err)
		assert.Equal(t, uint64(1<<40), binary.LittleEndian.Uint64(got))
	})

	t.Run("float32", func(t *testing.T) {
		t.Parallel()
		got, err := encodeFillValue(float32(1.5), arrow.PrimitiveTypes.Float32)
		require.NoError(t, err)
		assert.Equal(t, float32(1.5), math.Float32frombits(binary.LittleEndian.Uint32(got)))
	})

	t.Run("float64", func(t *testing.T) {
		t.Parallel()
		got, err := encodeFillValue(float64(-3.25), arrow.PrimitiveTypes.Float64)
		require.NoError(t, err)
		assert.Equal(t, float64(-3.25), math.Float64frombits(binary.LittleEndian.Uint64(got)))
	})
}

// TestEncodeFillValueWrongGoType exercises every dtype's type-assertion guard:
// encodeFillValue expects the concrete Go scalar coerceFillValue produces, so an
// untyped float64 (the raw JSON shape) is rejected by every typed branch.
func TestEncodeFillValueWrongGoType(t *testing.T) {
	t.Parallel()

	dts := []arrow.DataType{
		arrow.FixedWidthTypes.Boolean,
		arrow.PrimitiveTypes.Int8,
		arrow.PrimitiveTypes.Uint8,
		arrow.PrimitiveTypes.Int16,
		arrow.PrimitiveTypes.Uint16,
		arrow.PrimitiveTypes.Int32,
		arrow.PrimitiveTypes.Uint32,
		arrow.PrimitiveTypes.Float32,
		arrow.PrimitiveTypes.Int64,
		arrow.PrimitiveTypes.Uint64,
		arrow.PrimitiveTypes.Float64,
	}
	for _, dt := range dts {
		dt := dt
		t.Run(dt.String(), func(t *testing.T) {
			t.Parallel()
			// A bare string is assignable to none of the typed branches.
			_, err := encodeFillValue("not-a-scalar", dt)
			require.Error(t, err)
		})
	}
}

func TestEncodeFillValueUnsupportedDType(t *testing.T) {
	t.Parallel()
	_, err := encodeFillValue(int64(1), arrow.BinaryTypes.String)
	require.Error(t, err)
}

// TestCoerceThenEncodeFloat64 wires the two halves together: coerce the raw
// JSON-shaped float64 then encode, and confirm the bytes decode back to the
// original value for an integer and a float column.
func TestCoerceThenEncode(t *testing.T) {
	t.Parallel()

	t.Run("int32", func(t *testing.T) {
		t.Parallel()
		v, err := coerceFillValue(float64(-7), arrow.PrimitiveTypes.Int32)
		require.NoError(t, err)
		b, err := encodeFillValue(v, arrow.PrimitiveTypes.Int32)
		require.NoError(t, err)
		assert.Equal(t, int32(-7), int32(binary.LittleEndian.Uint32(b)))
	})

	t.Run("float64", func(t *testing.T) {
		t.Parallel()
		v, err := coerceFillValue(float64(9.5), arrow.PrimitiveTypes.Float64)
		require.NoError(t, err)
		b, err := encodeFillValue(v, arrow.PrimitiveTypes.Float64)
		require.NoError(t, err)
		assert.Equal(t, 9.5, math.Float64frombits(binary.LittleEndian.Uint64(b)))
	})
}
