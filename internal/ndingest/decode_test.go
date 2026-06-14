package ndingest_test

import (
	"context"
	"encoding/binary"
	"math"
	"path/filepath"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/karthedew/cosma/internal/ndingest"
	"github.com/karthedew/cosma/store"
	"github.com/karthedew/cosma/store/zarr"
)

// Golden values, mirroring store/zarr/testdata/generate.py (CODEC_I8 / CODEC_F8).
var (
	codecI8 = []int64{-3, -1, 0, 1, 2, 7, 100, 200}
	codecF8 = []float64{1.5, -2.5, 3.25, 0.0, 4.5, -6.75, 7.0, 8.125}
)

func testdataStore(t *testing.T, name string) store.Store {
	t.Helper()
	dir := filepath.Join("..", "..", "store", "zarr", "testdata", name)
	s, err := zarr.OpenFS(dir)
	if err != nil {
		t.Fatalf("OpenFS(%s): %v", name, err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

// decodeFullArray reads and decodes every chunk of a 1-D array in coord order
// and concatenates the results into one little-endian C-order buffer. The
// fixtures used here all have shapes that divide evenly into chunks, so simple
// concatenation reconstructs the array (cell-scan assembly is Z7's job, not
// this test's).
func decodeFullArray(t *testing.T, s store.Store, path string, alloc memory.Allocator) []byte {
	t.Helper()
	obj, err := s.Meta(context.Background(), path)
	if err != nil {
		t.Fatalf("Meta(%s): %v", path, err)
	}
	a, ok := obj.(*store.Array)
	if !ok {
		t.Fatalf("%s is not an array", path)
	}
	if len(a.Shape) != 1 {
		t.Fatalf("decodeFullArray only handles 1-D arrays, got shape %v", a.Shape)
	}

	nChunks := (a.Shape[0] + a.ChunkShape[0] - 1) / a.ChunkShape[0]
	var out []byte
	for c := int64(0); c < nChunks; c++ {
		raw, err := s.ReadChunk(context.Background(), store.ChunkKey{ArrayPath: path, Coord: []int64{c}})
		if err != nil {
			t.Fatalf("ReadChunk(%s, %d): %v", path, c, err)
		}
		dec, err := ndingest.DecodeChunk(a, raw, alloc)
		if err != nil {
			t.Fatalf("DecodeChunk(%s, %d): %v", path, c, err)
		}
		out = append(out, dec...)
		alloc.Free(dec)
	}
	return out
}

func asInt64(b []byte) []int64 {
	out := make([]int64, len(b)/8)
	for i := range out {
		out[i] = int64(binary.LittleEndian.Uint64(b[i*8:]))
	}
	return out
}

func asFloat64(b []byte) []float64 {
	out := make([]float64, len(b)/8)
	for i := range out {
		out[i] = math.Float64frombits(binary.LittleEndian.Uint64(b[i*8:]))
	}
	return out
}

func eqInt64(a, b []int64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func eqFloat64(a, b []float64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// TestDecodeCodecsV2 round-trips every v2-compressed fixture array (gzip, zstd,
// lz4) back to the known generator values.
func TestDecodeCodecsV2(t *testing.T) {
	s := testdataStore(t, "v2_codecs")
	alloc := memory.NewGoAllocator()

	for _, cname := range []string{"gzip", "zstd", "lz4"} {
		t.Run(cname+"_i8", func(t *testing.T) {
			got := asInt64(decodeFullArray(t, s, "/"+cname+"_i8", alloc))
			if !eqInt64(got, codecI8) {
				t.Errorf("%s_i8 = %v, want %v", cname, got, codecI8)
			}
		})
		t.Run(cname+"_f8", func(t *testing.T) {
			got := asFloat64(decodeFullArray(t, s, "/"+cname+"_f8", alloc))
			if !eqFloat64(got, codecF8) {
				t.Errorf("%s_f8 = %v, want %v", cname, got, codecF8)
			}
		})
	}
}

// TestDecodeCodecsV3 round-trips the v3 codec-chain fixtures (bytes -> gzip|zstd).
func TestDecodeCodecsV3(t *testing.T) {
	s := testdataStore(t, "v3_codecs")
	alloc := memory.NewGoAllocator()

	for _, cname := range []string{"gzip", "zstd"} {
		t.Run(cname+"_i8", func(t *testing.T) {
			got := asInt64(decodeFullArray(t, s, "/"+cname+"_i8", alloc))
			if !eqInt64(got, codecI8) {
				t.Errorf("%s_i8 = %v, want %v", cname, got, codecI8)
			}
		})
		t.Run(cname+"_f8", func(t *testing.T) {
			got := asFloat64(decodeFullArray(t, s, "/"+cname+"_f8", alloc))
			if !eqFloat64(got, codecF8) {
				t.Errorf("%s_f8 = %v, want %v", cname, got, codecF8)
			}
		})
	}
}

// TestDecodeBigEndian decodes the committed big-endian float64 fixture and
// checks the byteswap produced the right values.
func TestDecodeBigEndian(t *testing.T) {
	s := testdataStore(t, "v2_dtypes")
	alloc := memory.NewGoAllocator()

	got := asFloat64(decodeFullArray(t, s, "/be_f8", alloc))
	want := []float64{1.5, 2.5, 3.5, 4.5}
	if !eqFloat64(got, want) {
		t.Errorf("be_f8 = %v, want %v", got, want)
	}

	// The little-endian twin holds the same values: a cross-check that the
	// byteswap path and the straight-through path agree.
	le := asFloat64(decodeFullArray(t, s, "/f8", alloc))
	if !eqFloat64(le, want) {
		t.Errorf("f8 (LE) = %v, want %v", le, want)
	}
}

// TestDecodeFOrderMatchesCOrder decodes the F-order fixture and its C-order twin
// and asserts the transposed F-order buffer is byte-identical to the C-order one.
func TestDecodeFOrderMatchesCOrder(t *testing.T) {
	s := testdataStore(t, "v2_order")
	alloc := memory.NewGoAllocator()
	ctx := context.Background()

	decodeSingleChunk := func(path string) []byte {
		obj, err := s.Meta(ctx, path)
		if err != nil {
			t.Fatalf("Meta(%s): %v", path, err)
		}
		a := obj.(*store.Array)
		raw, err := s.ReadChunk(ctx, store.ChunkKey{ArrayPath: path, Coord: []int64{0, 0}})
		if err != nil {
			t.Fatalf("ReadChunk(%s): %v", path, err)
		}
		dec, err := ndingest.DecodeChunk(a, raw, alloc)
		if err != nil {
			t.Fatalf("DecodeChunk(%s): %v", path, err)
		}
		return dec
	}

	fOrder := decodeSingleChunk("/f_order")
	cOrder := decodeSingleChunk("/c_order")

	gotF := asInt64(fOrder)
	gotC := asInt64(cOrder)
	if !eqInt64(gotF, gotC) {
		t.Fatalf("F-order decoded %v != C-order decoded %v", gotF, gotC)
	}
	// Generator wrote np.arange(24).reshape(4,6); C-order is 0..23 in row-major.
	want := make([]int64, 24)
	for i := range want {
		want[i] = int64(i)
	}
	if !eqInt64(gotC, want) {
		t.Errorf("c_order = %v, want %v", gotC, want)
	}
	alloc.Free(fOrder)
	alloc.Free(cOrder)
}

// TestDecodeAllocatorDiscipline confirms DecodeChunk's output is the only
// allocator-held buffer after a decode that goes through the transpose path:
// the intermediate is freed, and freeing the result returns the allocator to
// zero.
func TestDecodeAllocatorDiscipline(t *testing.T) {
	s := testdataStore(t, "v2_order")
	checked := memory.NewCheckedAllocator(memory.NewGoAllocator())
	ctx := context.Background()

	obj, _ := s.Meta(ctx, "/f_order")
	a := obj.(*store.Array)
	raw, err := s.ReadChunk(ctx, store.ChunkKey{ArrayPath: "/f_order", Coord: []int64{0, 0}})
	if err != nil {
		t.Fatalf("ReadChunk: %v", err)
	}
	out, err := ndingest.DecodeChunk(a, raw, checked)
	if err != nil {
		t.Fatalf("DecodeChunk: %v", err)
	}
	checked.Free(out)
	checked.AssertSize(t, 0)
}

// TestDecodeTruncatedChunk: a chunk shorter than the declared geometry errors
// (after decompression) rather than silently truncating.
func TestDecodeTruncatedChunk(t *testing.T) {
	a := &store.Array{
		ArrayPath:  "/x",
		Shape:      []int64{4},
		ChunkShape: []int64{4},
		DType:      "int64",
	}
	// 3 int64s where 4 are expected.
	raw := make([]byte, 3*8)
	if _, err := ndingest.DecodeChunk(a, raw, memory.NewGoAllocator()); err == nil {
		t.Fatal("expected error on short chunk, got nil")
	}
}

// TestDecodeBitFlippedCompressed: a corrupt compressed payload errors in the
// codec rather than panicking.
func TestDecodeBitFlippedCompressed(t *testing.T) {
	s := testdataStore(t, "v2_codecs")
	ctx := context.Background()
	obj, _ := s.Meta(ctx, "/gzip_i8")
	a := obj.(*store.Array)
	raw, err := s.ReadChunk(ctx, store.ChunkKey{ArrayPath: "/gzip_i8", Coord: []int64{0}})
	if err != nil {
		t.Fatalf("ReadChunk: %v", err)
	}
	raw[len(raw)/2] ^= 0xFF // flip a byte in the gzip stream
	if _, err := ndingest.DecodeChunk(a, raw, memory.NewGoAllocator()); err == nil {
		t.Fatal("expected error on corrupt gzip payload, got nil")
	}
}

// TestRegistryExtension: a newly registered codec ID is picked up by
// DecodeChunk with no change to decode.go.
func TestRegistryExtension(t *testing.T) {
	// A trivial codec that XORs every byte with 0x5A (its own inverse).
	ndingest.Register("xor5a-test", func(src []byte, _ map[string]any) ([]byte, error) {
		out := make([]byte, len(src))
		for i, b := range src {
			out[i] = b ^ 0x5A
		}
		return out, nil
	})

	a := &store.Array{
		ArrayPath:  "/x",
		Shape:      []int64{2},
		ChunkShape: []int64{2},
		DType:      "int64",
		Codecs:     []store.CodecSpec{{ID: "xor5a-test"}},
	}
	want := []int64{42, -7}
	plain := make([]byte, 16)
	binary.LittleEndian.PutUint64(plain[0:], uint64(want[0]))
	binary.LittleEndian.PutUint64(plain[8:], uint64(want[1]))
	encoded := make([]byte, 16)
	for i, b := range plain {
		encoded[i] = b ^ 0x5A
	}

	out, err := ndingest.DecodeChunk(a, encoded, memory.NewGoAllocator())
	if err != nil {
		t.Fatalf("DecodeChunk: %v", err)
	}
	if got := asInt64(out); !eqInt64(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

// TestUnknownCodec: an unregistered codec ID errors loudly.
func TestUnknownCodec(t *testing.T) {
	a := &store.Array{
		ArrayPath:  "/x",
		Shape:      []int64{1},
		ChunkShape: []int64{1},
		DType:      "int64",
		Codecs:     []store.CodecSpec{{ID: "no-such-codec"}},
	}
	if _, err := ndingest.DecodeChunk(a, make([]byte, 8), memory.NewGoAllocator()); err == nil {
		t.Fatal("expected error for unknown codec, got nil")
	}
}

// TestFixedSizeBinaryUnsupported: the width-less type is rejected clearly.
func TestFixedSizeBinaryUnsupported(t *testing.T) {
	a := &store.Array{
		ArrayPath:  "/x",
		Shape:      []int64{1},
		ChunkShape: []int64{1},
		DType:      "fixed_size_binary",
	}
	if _, err := ndingest.DecodeChunk(a, make([]byte, 4), memory.NewGoAllocator()); err == nil {
		t.Fatal("expected unsupported error for fixed_size_binary, got nil")
	}
}
