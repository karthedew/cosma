package store

import "github.com/karthedew/cosma/schema"

// MemoryOrder is the in-memory layout of a chunk's cells: row-major (C) or
// column-major (Fortran).
type MemoryOrder int

const (
	// OrderC is row-major (C) order: the last axis varies fastest.
	OrderC MemoryOrder = iota
	// OrderF is column-major (Fortran) order: the first axis varies fastest.
	OrderF
)

// String returns "C" or "F".
func (o MemoryOrder) String() string {
	if o == OrderF {
		return "F"
	}
	return "C"
}

// Endianness is the byte order of multi-byte cell values in a chunk.
//
// Zarr v2 encodes byte order in the dtype string (e.g. ">f8"); Zarr v3 encodes
// it in the bytes codec. Drivers normalize both into this field so the decode
// pipeline has exactly one place to look.
type Endianness int

const (
	// LittleEndian is least-significant-byte-first.
	LittleEndian Endianness = iota
	// BigEndian is most-significant-byte-first.
	BigEndian
)

// String returns "little" or "big".
func (e Endianness) String() string {
	if e == BigEndian {
		return "big"
	}
	return "little"
}

// CodecSpec is one entry in an array's codec pipeline, passed through verbatim
// from the store metadata. The store layer does not interpret it; decoding
// (Z5) keys a registry on ID and reads Config.
type CodecSpec struct {
	ID     string
	Config map[string]any
}

// Attrs is an array or group's free-form attribute map, carried through
// unchanged from the store. The typed accessors return (zero, false) when the
// key is absent or the stored value is not assignable to the requested type.
type Attrs map[string]any

// String returns the string value at key.
func (a Attrs) String(key string) (string, bool) {
	v, ok := a[key].(string)
	return v, ok
}

// Strings returns the []string value at key. A []any whose elements are all
// strings is also accepted (the common shape after JSON decoding).
func (a Attrs) Strings(key string) ([]string, bool) {
	switch v := a[key].(type) {
	case []string:
		return v, true
	case []any:
		out := make([]string, len(v))
		for i, e := range v {
			s, ok := e.(string)
			if !ok {
				return nil, false
			}
			out[i] = s
		}
		return out, true
	default:
		return nil, false
	}
}

// Int returns the integer value at key. Float values with no fractional part
// (the shape JSON decoding produces) are accepted and truncated.
func (a Attrs) Int(key string) (int64, bool) {
	switch v := a[key].(type) {
	case int64:
		return v, true
	case int:
		return int64(v), true
	case int32:
		return int64(v), true
	case float64:
		if v == float64(int64(v)) {
			return int64(v), true
		}
		return 0, false
	default:
		return 0, false
	}
}

// Float returns the floating-point value at key. Integer values are accepted
// and widened.
func (a Attrs) Float(key string) (float64, bool) {
	switch v := a[key].(type) {
	case float64:
		return v, true
	case float32:
		return float64(v), true
	case int64:
		return float64(v), true
	case int:
		return float64(v), true
	default:
		return 0, false
	}
}

// Bool returns the boolean value at key.
func (a Attrs) Bool(key string) (bool, bool) {
	v, ok := a[key].(bool)
	return v, ok
}

// Object is the common surface of the two node kinds in a store hierarchy.
type Object interface {
	// Path is the absolute, slash-separated path of the node ("/" for root).
	Path() string
	// Name is the final path segment ("" for root).
	Name() string
	// Kind reports whether the node is a group or an array.
	Kind() ObjectKind
	// Attrs returns the node's attribute map (may be nil).
	Attrs() Attrs
}

// Group is an interior node.
type Group struct {
	GroupPath  string
	GroupAttrs Attrs
	// Children are the absolute paths of direct children, in store listing
	// order. Drivers need not populate it from Meta: Tree discovers children
	// via Store.List and fills this field on its own copy of the group (zarr
	// v2 group metadata, for example, carries no child listing). It is set on
	// every *Group obtained from a Tree.
	Children []string
}

// Path implements Object.
func (g *Group) Path() string { return g.GroupPath }

// Name implements Object.
func (g *Group) Name() string { return baseName(g.GroupPath) }

// Kind implements Object.
func (g *Group) Kind() ObjectKind { return KindGroup }

// Attrs implements Object.
func (g *Group) Attrs() Attrs { return g.GroupAttrs }

// Array is a leaf node describing one chunked n-dimensional array. The fields
// are populated from store metadata only; no chunk bytes are read to build it.
type Array struct {
	ArrayPath  string
	ArrayAttrs Attrs

	Shape      []int64 // cell extent per axis
	ChunkShape []int64 // chunk extent per axis (same rank as Shape)

	DType      schema.DType // logical cell type, reusing cosma/schema
	Endianness Endianness   // byte order of multi-byte cells

	Codecs    []CodecSpec // decode pipeline, store order
	FillValue any         // value for missing cells; nil = undefined
	Order     MemoryOrder // C or F in-chunk layout

	// DimNames names each axis, or is nil when the store does not name dims.
	// Pass-through only — the store layer assigns no meaning to dim names.
	DimNames []string
}

// Path implements Object.
func (a *Array) Path() string { return a.ArrayPath }

// Name implements Object.
func (a *Array) Name() string { return baseName(a.ArrayPath) }

// Kind implements Object.
func (a *Array) Kind() ObjectKind { return KindArray }

// Attrs implements Object.
func (a *Array) Attrs() Attrs { return a.ArrayAttrs }

// Ndim returns the array's rank.
func (a *Array) Ndim() int { return len(a.Shape) }
