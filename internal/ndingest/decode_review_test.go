package ndingest_test

import (
	"bytes"
	"compress/zlib"
	"encoding/binary"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/karthedew/cosma/internal/ndingest"
	"github.com/karthedew/cosma/schema"
	"github.com/karthedew/cosma/store"
)

// These tests close two coverage gaps found while reviewing Z5: the standalone
// "zlib" codec (the committed fixtures only exercise zlib as a blosc *inner*
// codec, which uses blosc's own decoder, not ndingest's decodeZlib), and
// big-endian byteswap at element widths other than 8 (the be_f8 fixture only
// covers width 8). Both build their payloads directly, so no fixture regen.

// TestDecodeZlibCodec round-trips a raw zlib stream through the registered
// "zlib" codec, the shape numcodecs Zlib produces for a v2 compressor.
func TestDecodeZlibCodec(t *testing.T) {
	want := []int64{-5, 0, 7, 42, 1 << 40}
	plain := make([]byte, len(want)*8)
	for i, v := range want {
		binary.LittleEndian.PutUint64(plain[i*8:], uint64(v))
	}

	var buf bytes.Buffer
	zw := zlib.NewWriter(&buf)
	if _, err := zw.Write(plain); err != nil {
		t.Fatalf("zlib write: %v", err)
	}
	if err := zw.Close(); err != nil {
		t.Fatalf("zlib close: %v", err)
	}

	a := &store.Array{
		ArrayPath:  "/z",
		Shape:      []int64{int64(len(want))},
		ChunkShape: []int64{int64(len(want))},
		DType:      schema.Int64,
		Codecs:     []store.CodecSpec{{ID: "zlib"}},
	}
	out, err := ndingest.DecodeChunk(a, buf.Bytes(), memory.NewGoAllocator())
	if err != nil {
		t.Fatalf("DecodeChunk: %v", err)
	}
	if got := asInt64(out); !eqInt64(got, want) {
		t.Errorf("zlib decode = %v, want %v", got, want)
	}
}

// TestDecodeBigEndianNarrowWidths exercises the byteswap path at widths 2 and 4
// (int16, int32), not just the width-8 be_f8 fixture, since the swap loop is
// width-generic and an off-by-one there would only surface at a narrow width.
func TestDecodeBigEndianNarrowWidths(t *testing.T) {
	t.Run("int32", func(t *testing.T) {
		want := []int32{-1, 0, 1, 0x01020304, 0x7fffffff}
		raw := make([]byte, len(want)*4)
		for i, v := range want {
			binary.BigEndian.PutUint32(raw[i*4:], uint32(v))
		}
		a := &store.Array{
			ArrayPath:  "/i32",
			Shape:      []int64{int64(len(want))},
			ChunkShape: []int64{int64(len(want))},
			DType:      schema.Int32,
			Endianness: store.BigEndian,
		}
		out, err := ndingest.DecodeChunk(a, raw, memory.NewGoAllocator())
		if err != nil {
			t.Fatalf("DecodeChunk: %v", err)
		}
		for i := range want {
			got := int32(binary.LittleEndian.Uint32(out[i*4:]))
			if got != want[i] {
				t.Errorf("int32[%d] = %d, want %d", i, got, want[i])
			}
		}
	})

	t.Run("int16", func(t *testing.T) {
		want := []int16{-1, 0, 1, 0x0102, 0x7fff}
		raw := make([]byte, len(want)*2)
		for i, v := range want {
			binary.BigEndian.PutUint16(raw[i*2:], uint16(v))
		}
		a := &store.Array{
			ArrayPath:  "/i16",
			Shape:      []int64{int64(len(want))},
			ChunkShape: []int64{int64(len(want))},
			DType:      schema.Int16,
			Endianness: store.BigEndian,
		}
		out, err := ndingest.DecodeChunk(a, raw, memory.NewGoAllocator())
		if err != nil {
			t.Fatalf("DecodeChunk: %v", err)
		}
		for i := range want {
			got := int16(binary.LittleEndian.Uint16(out[i*2:]))
			if got != want[i] {
				t.Errorf("int16[%d] = %d, want %d", i, got, want[i])
			}
		}
	})
}

// TestDecodeScalarChunk decodes a 0-d (scalar) array's single-cell chunk: the
// chunkBytes/length-validation path with an empty shape, which the fixture-based
// tests reach only indirectly through the cell scan.
func TestDecodeScalarChunk(t *testing.T) {
	raw := make([]byte, 8)
	val := int64(-123)
	binary.LittleEndian.PutUint64(raw, uint64(val))
	a := &store.Array{
		ArrayPath:  "/s",
		Shape:      nil,
		ChunkShape: nil,
		DType:      schema.Int64,
	}
	out, err := ndingest.DecodeChunk(a, raw, memory.NewGoAllocator())
	if err != nil {
		t.Fatalf("DecodeChunk: %v", err)
	}
	if got := asInt64(out); len(got) != 1 || got[0] != -123 {
		t.Errorf("scalar decode = %v, want [-123]", got)
	}
}
