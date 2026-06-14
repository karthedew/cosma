// Package ndingest is Cosma's chunk-decode seam for n-dimensional (Zarr-style)
// array stores, mirroring internal/ingest's role for tabular files. It turns the
// raw bytes of one stored chunk into a typed, little-endian, C-order buffer:
// codec decompression, byte-order normalization, and F-order transpose, with a
// hard length check against the array's declared chunk geometry.
//
// Decoding is driven entirely by the store.Array metadata the driver already
// produced (DType, Endianness, Order, Codecs). The codec registry is keyed on
// CodecSpec.ID so future codecs (blosc in Z8, and beyond) are drop-ins: register
// an ID and DecodeChunk picks it up with no change to decode.go.
package ndingest

import (
	"bytes"
	"compress/gzip"
	"compress/zlib"
	"encoding/binary"
	"fmt"
	"io"
	"sync"

	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
)

// DecodeFunc decompresses one chunk payload. src is the bytes-to-bytes codec
// input (the raw stored payload for the outermost codec, or the previous codec's
// output deeper in the pipeline); config is the CodecSpec.Config carried verbatim
// from the store metadata (e.g. the numcodecs lz4 has no decode-relevant config,
// but gzip/zstd may). Implementations must not retain src.
type DecodeFunc func(src []byte, config map[string]any) ([]byte, error)

var (
	registryMu sync.RWMutex
	registry   = map[string]DecodeFunc{}
)

// Register installs a decoder under a codec ID. Registering a new ID is the only
// step needed to support a new codec — DecodeChunk keys this registry by
// CodecSpec.ID and needs no change. A nil fn or empty id panics (programmer
// error); re-registering an ID overwrites the prior entry.
func Register(id string, fn DecodeFunc) {
	if id == "" {
		panic("ndingest: Register with empty codec id")
	}
	if fn == nil {
		panic("ndingest: Register with nil DecodeFunc for id " + id)
	}
	registryMu.Lock()
	registry[id] = fn
	registryMu.Unlock()
}

// lookup returns the decoder for id, or (nil, false) when none is registered.
func lookup(id string) (DecodeFunc, bool) {
	registryMu.RLock()
	fn, ok := registry[id]
	registryMu.RUnlock()
	return fn, ok
}

func init() {
	Register("gzip", decodeGzip)
	Register("zlib", decodeZlib)
	Register("zstd", decodeZstd)
	Register("lz4", decodeLZ4)
	// The Zarr v3 "bytes" codec is the array-to-bytes (endianness) codec. The
	// driver normalizes its endian into store.Array.Endianness, so at the
	// bytes-to-bytes pipeline level it is a no-op pass-through.
	Register("bytes", decodeBytesNoop)
}

func decodeBytesNoop(src []byte, _ map[string]any) ([]byte, error) {
	return src, nil
}

func decodeGzip(src []byte, _ map[string]any) ([]byte, error) {
	r, err := gzip.NewReader(bytes.NewReader(src))
	if err != nil {
		return nil, fmt.Errorf("ndingest: gzip: %w", err)
	}
	defer r.Close()
	out, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("ndingest: gzip read: %w", err)
	}
	return out, nil
}

func decodeZlib(src []byte, _ map[string]any) ([]byte, error) {
	r, err := zlib.NewReader(bytes.NewReader(src))
	if err != nil {
		return nil, fmt.Errorf("ndingest: zlib: %w", err)
	}
	defer r.Close()
	out, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("ndingest: zlib read: %w", err)
	}
	return out, nil
}

// zstdDecoder is a shared, concurrency-safe streaming decoder. A zstd.Decoder
// created with no writer is safe for concurrent DecodeAll calls.
var (
	zstdOnce sync.Once
	zstdDec  *zstd.Decoder
	zstdErr  error
)

func decodeZstd(src []byte, _ map[string]any) ([]byte, error) {
	zstdOnce.Do(func() {
		zstdDec, zstdErr = zstd.NewReader(nil)
	})
	if zstdErr != nil {
		return nil, fmt.Errorf("ndingest: zstd reader: %w", zstdErr)
	}
	out, err := zstdDec.DecodeAll(src, nil)
	if err != nil {
		return nil, fmt.Errorf("ndingest: zstd: %w", err)
	}
	return out, nil
}

// decodeLZ4 decodes the numcodecs LZ4 format: a 4-byte little-endian
// uncompressed-size header followed by a single raw LZ4 block (not the LZ4 frame
// format). The header size is verified against the lz4 block decode to catch
// truncation.
func decodeLZ4(src []byte, _ map[string]any) ([]byte, error) {
	if len(src) < 4 {
		return nil, fmt.Errorf("ndingest: lz4: payload too short for size header (%d bytes)", len(src))
	}
	want := int(binary.LittleEndian.Uint32(src[:4]))
	if want < 0 {
		return nil, fmt.Errorf("ndingest: lz4: invalid uncompressed size header")
	}
	dst := make([]byte, want)
	n, err := lz4.UncompressBlock(src[4:], dst)
	if err != nil {
		return nil, fmt.Errorf("ndingest: lz4: %w", err)
	}
	if n != want {
		return nil, fmt.Errorf("ndingest: lz4: decoded %d bytes, header declared %d", n, want)
	}
	return dst, nil
}
