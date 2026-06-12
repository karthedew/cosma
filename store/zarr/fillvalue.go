package zarr

import (
	"encoding/base64"
	"math"

	"github.com/karthedew/cosma/schema"
)

// parseV2FillValue normalizes a v2 ".zarray" fill_value into a Go value carried
// on Array.FillValue. The store layer assigns no further meaning; nil means the
// fill value is undefined (JSON null), which higher layers treat per their
// missing-chunk policy.
//
// v2 quirks handled:
//   - JSON null            -> nil (undefined)
//   - "NaN"/"Infinity"/"-Infinity" string for floats -> the float64 special
//   - base64 string for |SN (fixed byte-string) dtype -> decoded []byte
//   - plain JSON number    -> the number, narrowed to the dtype's Go kind
//   - JSON bool            -> bool
func parseV2FillValue(raw any, dt dtypeInfo) (any, error) {
	if raw == nil {
		return nil, nil
	}

	switch dt.DType {
	case schema.FixedSizeBinary:
		// fill value is a base64-encoded byte string
		if s, ok := raw.(string); ok {
			b, err := base64.StdEncoding.DecodeString(s)
			if err != nil {
				return nil, err
			}
			return b, nil
		}
		return raw, nil

	case schema.Float16, schema.Float32, schema.Float64:
		if s, ok := raw.(string); ok {
			return floatFromString(s), nil
		}
		if f, ok := raw.(float64); ok {
			return f, nil
		}
		return raw, nil

	case schema.Bool:
		return raw, nil

	default:
		// integer kinds: JSON decodes numbers as float64; keep float64 here and
		// let consumers narrow. Pass through anything else verbatim.
		return raw, nil
	}
}

// parseV3FillValue normalizes a v3 "zarr.json" fill_value. v3 fill values are
// typed per the spec: numbers for numeric arrays, the strings "NaN"/"Infinity"/
// "-Infinity" for non-finite floats, true/false for bool, and (for raw/bytes)
// an array of byte integers. null is not a legal v3 fill value, but is tolerated
// here as undefined for robustness.
func parseV3FillValue(raw any, dt schema.DType) (any, error) {
	if raw == nil {
		return nil, nil
	}

	switch dt {
	case schema.Float16, schema.Float32, schema.Float64:
		if s, ok := raw.(string); ok {
			return floatFromString(s), nil
		}
		return raw, nil

	case schema.FixedSizeBinary:
		// v3 raw fill is a JSON array of byte values
		if arr, ok := raw.([]any); ok {
			b := make([]byte, len(arr))
			for i, e := range arr {
				if f, ok := e.(float64); ok {
					b[i] = byte(int64(f))
				}
			}
			return b, nil
		}
		return raw, nil

	default:
		return raw, nil
	}
}

// floatFromString maps the Zarr special-float strings to float64; any other
// string is returned as a quiet NaN (the spec defines only the three).
func floatFromString(s string) float64 {
	switch s {
	case "NaN":
		return math.NaN()
	case "Infinity":
		return math.Inf(1)
	case "-Infinity":
		return math.Inf(-1)
	default:
		return math.NaN()
	}
}
