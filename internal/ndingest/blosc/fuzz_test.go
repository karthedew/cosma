package blosc

import (
	"os"
	"path/filepath"
	"testing"
)

// FuzzDecode drives the full frame decoder (header parse, offset-table and
// block-math, inner codecs, shuffle inversion) with arbitrary bytes. The only
// invariant under fuzzing is that Decode must never panic and never over-read:
// hostile or truncated input must surface as an error. Real fixture frames seed
// the corpus so the fuzzer explores mutations of valid frames, not just noise.
func FuzzDecode(f *testing.F) {
	seedDir := filepath.Join("..", "..", "..", "store", "zarr", "testdata", "v2_blosc")
	for _, name := range []string{
		"blosclz_shuffle_big_i8", "lz4_shuffle_big_i8", "zstd_shuffle_big_i8",
		"lz4_noshuffle_big_i8", "blosclz_bitshuffle_big_i8", "zlib_shuffle_big_i8",
		"multiblock_i8", "blosclz_shuffle_i8",
	} {
		if b, err := os.ReadFile(filepath.Join(seedDir, name, "0")); err == nil {
			f.Add(b)
		}
	}
	// A few hand-built edge cases.
	f.Add([]byte{})
	f.Add(make([]byte, headerSize))

	f.Fuzz(func(t *testing.T, data []byte) {
		// Must return (possibly an error) without panicking.
		out, err := Decode(data)
		if err == nil {
			// On success the output length must equal the header's declared
			// nbytes — a basic consistency check that also guards against a
			// decoder that silently under/over-fills.
			h, perr := parseHeader(data)
			if perr == nil && len(out) != h.nbytes {
				t.Fatalf("Decode returned %d bytes but header nbytes=%d", len(out), h.nbytes)
			}
		}
	})
}

// FuzzBlosclz drives the blosclz block decompressor directly with arbitrary
// input against a fixed-size destination. It must never panic or write out of
// bounds regardless of how malformed the compressed stream is.
func FuzzBlosclz(f *testing.F) {
	f.Add([]byte{}, uint16(0))
	f.Add([]byte{0x00, 0x41}, uint16(8))        // 1-byte literal "A"
	f.Add([]byte{0x1f, 0x41}, uint16(64))       // 32-byte literal run header
	f.Add([]byte{0xe0, 0x00, 0x00}, uint16(64)) // a back-reference opcode

	f.Fuzz(func(t *testing.T, src []byte, dstLen uint16) {
		// Cap the destination so the fuzzer cannot request huge allocations.
		n := int(dstLen) % 4096
		dst := make([]byte, n)
		_, _ = blosclzDecompress(src, dst) // must not panic
	})
}
