package zarr

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/karthedew/cosma/schema"
	"github.com/karthedew/cosma/store"
)

// dtypeInfo is the parsed result of a Zarr dtype: the logical Cosma type, byte
// order, and (for fixed-size binary) the element width in bytes. ByteWidth is
// 0 when the type carries no fixed element width here.
type dtypeInfo struct {
	DType      schema.DType
	Endianness store.Endianness
	ByteWidth  int
}

// parseV2DType maps a NumPy dtype string (the v2 ".zarray" dtype field) to a
// dtypeInfo. It supports the numeric, boolean, byte-string, and datetime kinds
// Cosma understands; unicode (<UN) and structured dtypes error loudly.
//
// A NumPy dtype string is an optional byte-order char ("<" little, ">" big, "|"
// not-applicable / single byte) followed by a kind char and an item size:
// "<f8", ">i4", "|b1", "|u1", "|S16", "<M8[ns]". Single-byte and "|"-ordered
// types are treated as LittleEndian (byte order is irrelevant for width 1).
func parseV2DType(s string) (dtypeInfo, error) {
	if s == "" {
		return dtypeInfo{}, fmt.Errorf("zarr: empty v2 dtype")
	}
	// Structured dtypes decode from JSON as a list, not a string; if a string
	// somehow describes a record (contains a comma or parens), reject it.
	if strings.ContainsAny(s, ",()[]") && !strings.Contains(s, "[") {
		return dtypeInfo{}, fmt.Errorf("zarr: structured dtype %q is unsupported", s)
	}

	order := s[0]
	var endian store.Endianness
	switch order {
	case '<', '|':
		endian = store.LittleEndian
	case '>':
		endian = store.BigEndian
	case '=':
		// native order; Cosma fixtures are little-endian hosts, treat as little
		endian = store.LittleEndian
	default:
		return dtypeInfo{}, fmt.Errorf("zarr: malformed v2 dtype %q (bad byte-order char)", s)
	}
	rest := s[1:]
	if rest == "" {
		return dtypeInfo{}, fmt.Errorf("zarr: malformed v2 dtype %q", s)
	}
	kind := rest[0]

	// Datetime: "<M8[ns]" etc. Kind 'M' with a unit in brackets.
	if kind == 'M' {
		return dtypeInfo{DType: schema.Timestamp, Endianness: endian}, nil
	}
	if kind == 'm' {
		return dtypeInfo{DType: schema.Duration, Endianness: endian}, nil
	}

	sizeStr := rest[1:]
	// Strip any datetime unit suffix defensively (shouldn't reach here for M/m).
	if i := strings.IndexByte(sizeStr, '['); i >= 0 {
		sizeStr = sizeStr[:i]
	}

	switch kind {
	case 'b': // boolean, "|b1"
		return dtypeInfo{DType: schema.Bool, Endianness: store.LittleEndian, ByteWidth: 1}, nil
	case 'S': // fixed-length byte string, "|S16"
		width, err := strconv.Atoi(sizeStr)
		if err != nil || width < 0 {
			return dtypeInfo{}, fmt.Errorf("zarr: malformed |S dtype %q", s)
		}
		return dtypeInfo{DType: schema.FixedSizeBinary, Endianness: store.LittleEndian, ByteWidth: width}, nil
	case 'U': // unicode string — not supported
		return dtypeInfo{}, fmt.Errorf("zarr: unicode dtype %q is unsupported", s)
	case 'V': // raw void / structured
		return dtypeInfo{}, fmt.Errorf("zarr: void/structured dtype %q is unsupported", s)
	case 'i', 'u', 'f', 'c':
		size, err := strconv.Atoi(sizeStr)
		if err != nil {
			return dtypeInfo{}, fmt.Errorf("zarr: malformed numeric dtype %q", s)
		}
		dt, ok := numericDType(kind, size)
		if !ok {
			return dtypeInfo{}, fmt.Errorf("zarr: unsupported numeric dtype %q", s)
		}
		if size == 1 {
			endian = store.LittleEndian
		}
		return dtypeInfo{DType: dt, Endianness: endian, ByteWidth: size}, nil
	default:
		return dtypeInfo{}, fmt.Errorf("zarr: unsupported v2 dtype kind %q in %q", string(kind), s)
	}
}

// numericDType maps a NumPy numeric kind+size to a schema.DType.
func numericDType(kind byte, size int) (schema.DType, bool) {
	switch kind {
	case 'i':
		switch size {
		case 1:
			return schema.Int8, true
		case 2:
			return schema.Int16, true
		case 4:
			return schema.Int32, true
		case 8:
			return schema.Int64, true
		}
	case 'u':
		switch size {
		case 1:
			return schema.UInt8, true
		case 2:
			return schema.UInt16, true
		case 4:
			return schema.UInt32, true
		case 8:
			return schema.UInt64, true
		}
	case 'f':
		switch size {
		case 2:
			return schema.Float16, true
		case 4:
			return schema.Float32, true
		case 8:
			return schema.Float64, true
		}
	}
	return "", false
}

// parseV3DType maps a Zarr v3 "data_type" name to a schema.DType. v3 byte order
// is carried separately by the bytes codec, so this returns no endianness; the
// caller fills Array.Endianness from the codec list (default little).
func parseV3DType(name string) (schema.DType, error) {
	switch name {
	case "bool":
		return schema.Bool, nil
	case "int8":
		return schema.Int8, nil
	case "int16":
		return schema.Int16, nil
	case "int32":
		return schema.Int32, nil
	case "int64":
		return schema.Int64, nil
	case "uint8":
		return schema.UInt8, nil
	case "uint16":
		return schema.UInt16, nil
	case "uint32":
		return schema.UInt32, nil
	case "uint64":
		return schema.UInt64, nil
	case "float16":
		return schema.Float16, nil
	case "float32":
		return schema.Float32, nil
	case "float64":
		return schema.Float64, nil
	case "complex64", "complex128":
		return "", fmt.Errorf("zarr: complex dtype %q is unsupported", name)
	case "string", "bytes":
		return "", fmt.Errorf("zarr: variable-length dtype %q is unsupported", name)
	default:
		return "", fmt.Errorf("zarr: unsupported v3 data_type %q", name)
	}
}
