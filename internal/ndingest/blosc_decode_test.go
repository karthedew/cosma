package ndingest_test

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"
)

// TestDecodeBloscV2 round-trips every (cname x shuffle) blosc fixture array
// through the real zarr driver (ReadChunk -> DecodeChunk) back to the known
// generator values CODEC_I8 / CODEC_F8. The "blosc" codec is registered by
// internal/ndingest's init importing the blosc subpackage, so these decode
// end-to-end with no test wiring beyond opening the fixture store.
//
// Fixture provenance: store/zarr/testdata/generate.py:v2_blosc (zarr==3.0.8,
// numcodecs==0.16.5). The blosclz_shuffle combo is the Zarr v2 default
// (blosc-blosclz with byte shuffle) and is the most important case.
func TestDecodeBloscV2(t *testing.T) {
	s := testdataStore(t, "v2_blosc")
	alloc := memory.NewGoAllocator()

	combos := []string{
		"blosclz_shuffle",
		"lz4_shuffle",
		"zstd_shuffle",
		"lz4_noshuffle",
		"blosclz_bitshuffle",
		"zlib_shuffle",
	}
	for _, label := range combos {
		t.Run(label+"_i8", func(t *testing.T) {
			got := asInt64(decodeFullArray(t, s, "/"+label+"_i8", alloc))
			if !eqInt64(got, codecI8) {
				t.Errorf("%s_i8 = %v, want %v", label, got, codecI8)
			}
		})
		t.Run(label+"_f8", func(t *testing.T) {
			got := asFloat64(decodeFullArray(t, s, "/"+label+"_f8", alloc))
			if !eqFloat64(got, codecF8) {
				t.Errorf("%s_f8 = %v, want %v", label, got, codecF8)
			}
		})
		// The big array forces a genuinely compressed (non-memcpy) frame so the
		// inner-codec block path and the shuffle inversion both run; the small
		// arrays above are incompressible and stored memcpy'd.
		t.Run(label+"_big_i8", func(t *testing.T) {
			got := asInt64(decodeFullArray(t, s, "/"+label+"_big_i8", alloc))
			if !eqInt64(got, bloscBigI8) {
				t.Errorf("%s_big_i8 mismatch: len(got)=%d", label, len(got))
			}
		})
	}

	// A chunk forced (via a small blosc blocksize) into many blocks exercises the
	// per-block offset table and block loop, not just a single full-chunk block.
	t.Run("multiblock_i8", func(t *testing.T) {
		got := asInt64(decodeFullArray(t, s, "/multiblock_i8", alloc))
		if !eqInt64(got, bloscBigI8) {
			t.Errorf("multiblock_i8 mismatch: len(got)=%d", len(got))
		}
	})
}

// bloscBigI8 mirrors store/zarr/testdata/generate.py:BLOSC_BIG_I8 — value i is
// (i % 17) - 4 over 1024 elements.
var bloscBigI8 = func() []int64 {
	v := make([]int64, 1024)
	for i := range v {
		v[i] = int64(i%17) - 4
	}
	return v
}()
