package ndingest

import (
	"fmt"

	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/karthedew/cosma/schema"
	"github.com/karthedew/cosma/store"
)

// DecodeChunk turns the raw stored bytes of one chunk into a typed buffer ready
// for cell access: little-endian, C-order, full (padded) chunk length.
//
// The pipeline, in order:
//  1. Run the codec chain in reverse of the store's encode order (the store lists
//     codecs encode-first; decode is the mirror image). Each codec ID is resolved
//     against the registry; an unknown ID errors loudly rather than passing bytes
//     through untouched.
//  2. Byte-swap in place when a.Endianness == store.BigEndian and the dtype width
//     is > 1.
//  3. Transpose F-order chunks to C-order when a.Order == store.OrderF.
//  4. Validate the decoded length equals prod(a.ChunkShape) * width. Zarr stores
//     full, padded chunks (edge chunks included), so the expected length is always
//     the full chunk. A mismatch is a hard error (corrupt/truncated chunk), never
//     a silent truncation.
//
// alloc is used for the output buffer (engine convention); it is permitted to be
// nil, in which case the default Go allocator is used.
func DecodeChunk(a *store.Array, raw []byte, alloc memory.Allocator) ([]byte, error) {
	if a == nil {
		return nil, fmt.Errorf("ndingest: nil array")
	}
	if alloc == nil {
		alloc = memory.NewGoAllocator()
	}

	width, err := dtypeWidth(a.DType)
	if err != nil {
		return nil, err
	}

	decoded, err := runCodecs(a.Codecs, raw)
	if err != nil {
		return nil, err
	}

	// Validate before any in-place mutation so corrupt input fails fast and the
	// byteswap/transpose steps can assume a well-formed buffer.
	want := chunkBytes(a.ChunkShape, width)
	if len(decoded) != want {
		return nil, fmt.Errorf(
			"ndingest: decoded chunk length %d != expected %d (chunk shape %v, dtype %s width %d)",
			len(decoded), want, a.ChunkShape, a.DType, width,
		)
	}

	// Copy into an allocator-owned buffer before mutating: codec outputs may be
	// shared (e.g. the bytes no-op returns the input slice).
	out := copyToAllocator(alloc, decoded)

	if a.Endianness == store.BigEndian && width > 1 {
		byteswap(out, width)
	}

	if a.Order == store.OrderF && len(a.ChunkShape) > 1 {
		transposed := transposeFToC(alloc, out, a.ChunkShape, width)
		alloc.Free(out)
		out = transposed
	}

	return out, nil
}

// runCodecs applies the codec chain in reverse store order. An empty chain
// returns raw unchanged (uncompressed chunk).
func runCodecs(codecs []store.CodecSpec, raw []byte) ([]byte, error) {
	buf := raw
	for i := len(codecs) - 1; i >= 0; i-- {
		spec := codecs[i]
		fn, ok := lookup(spec.ID)
		if !ok {
			return nil, fmt.Errorf("ndingest: unknown codec %q (no decoder registered)", spec.ID)
		}
		out, err := fn(buf, spec.Config)
		if err != nil {
			return nil, err
		}
		buf = out
	}
	return buf, nil
}

// copyToAllocator returns an allocator-owned copy of src. The Arrow allocator
// hands out a raw []byte we own outright, matching the engine's buffer
// convention without forcing the caller to manage an arrow.Buffer here.
func copyToAllocator(alloc memory.Allocator, src []byte) []byte {
	dst := alloc.Allocate(len(src))
	copy(dst, src)
	return dst
}

// chunkBytes returns prod(shape) * width. A 0-d (scalar) chunk has an empty
// shape and a product of 1.
func chunkBytes(shape []int64, width int) int {
	n := 1
	for _, d := range shape {
		n *= int(d)
	}
	return n * width
}

// byteswap reverses the bytes of every width-sized element of buf in place. buf's
// length is assumed to be a multiple of width (guaranteed by the prior length
// check).
func byteswap(buf []byte, width int) {
	for off := 0; off+width <= len(buf); off += width {
		for i, j := off, off+width-1; i < j; i, j = i+1, j-1 {
			buf[i], buf[j] = buf[j], buf[i]
		}
	}
}

// transposeFToC reorders a column-major (Fortran) chunk buffer into row-major
// (C) order. shape is the chunk shape; width is the element byte width. It
// allocates a fresh buffer of the same length and returns it.
//
// Cell with multi-index idx sits at the F-order linear position
//
//	sum_k idx[k] * Fstride[k]   where Fstride[0]=1, Fstride[k]=prod(shape[:k])
//
// and at the C-order position
//
//	sum_k idx[k] * Cstride[k]   where Cstride[last]=1, Cstride[k]=prod(shape[k+1:]).
//
// We walk cells in C order (so the destination is a straight append) and gather
// each from its F-order source offset. The result is allocated from alloc, and
// the caller frees src.
func transposeFToC(alloc memory.Allocator, src []byte, shape []int64, width int) []byte {
	rank := len(shape)
	n := 1
	for _, d := range shape {
		n *= int(d)
	}
	dst := alloc.Allocate(len(src))

	fstride := make([]int, rank)
	stride := 1
	for k := 0; k < rank; k++ {
		fstride[k] = stride
		stride *= int(shape[k])
	}

	idx := make([]int, rank)
	for c := 0; c < n; c++ {
		// F-order source linear offset for the current multi-index.
		fpos := 0
		for k := 0; k < rank; k++ {
			fpos += idx[k] * fstride[k]
		}
		copy(dst[c*width:(c+1)*width], src[fpos*width:fpos*width+width])

		// Increment the multi-index in C order (last axis fastest).
		for k := rank - 1; k >= 0; k-- {
			idx[k]++
			if idx[k] < int(shape[k]) {
				break
			}
			idx[k] = 0
		}
	}
	return dst
}

// dtypeWidth returns the byte width of one cell for the fixed-width primitive
// types the chunk-decode pipeline supports. FixedSizeBinary is rejected: its
// element width is not carried on store.Array yet, so its chunk length cannot be
// validated here. Variable-length and nested types are out of scope.
func dtypeWidth(dt schema.DType) (int, error) {
	switch dt {
	case schema.Bool, schema.Int8, schema.UInt8:
		return 1, nil
	case schema.Int16, schema.UInt16, schema.Float16:
		return 2, nil
	case schema.Int32, schema.UInt32, schema.Float32,
		schema.Date32, schema.Time32:
		return 4, nil
	case schema.Int64, schema.UInt64, schema.Float64,
		schema.Date64, schema.Time64, schema.Timestamp, schema.Duration:
		return 8, nil
	case schema.FixedSizeBinary:
		return 0, fmt.Errorf("ndingest: fixed_size_binary chunk decode is unsupported " +
			"(store.Array does not carry the element byte width yet)")
	default:
		return 0, fmt.Errorf("ndingest: dtype %q has no fixed chunk-cell width (unsupported for chunk decode)", dt)
	}
}
