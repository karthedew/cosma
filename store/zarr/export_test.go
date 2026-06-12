package zarr

import "github.com/karthedew/cosma/schema"

// ParseV2DTypeForTest exposes the v2 dtype parser to the external test package
// so unsupported-dtype error paths can be exercised directly.
func ParseV2DTypeForTest(s string) (schema.DType, error) {
	info, err := parseV2DType(s)
	return info.DType, err
}

// ParseV3DTypeForTest exposes the v3 dtype parser to the external test package.
func ParseV3DTypeForTest(name string) (schema.DType, error) {
	return parseV3DType(name)
}
