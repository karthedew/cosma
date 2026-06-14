package blosc

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// fixturePath resolves a chunk file under the committed v2_blosc fixture store.
func fixturePath(name string) string {
	return filepath.Join("..", "..", "..", "store", "zarr", "testdata", "v2_blosc", name, "0")
}

// readFixture reads a fixture chunk frame, failing the test on error.
func readFixture(t *testing.T, name string) []byte {
	t.Helper()
	b, err := os.ReadFile(fixturePath(name))
	if err != nil {
		t.Fatalf("read fixture %s: %v", name, err)
	}
	return b
}

// makeFrame builds a minimal valid blosc1 header for header-parser tests.
func makeFrame(version, flags, typesize byte, nbytes, blocksize, cbytes uint32, tail []byte) []byte {
	f := make([]byte, headerSize+len(tail))
	f[0] = version
	f[1] = 1 // versionlz
	f[2] = flags
	f[3] = typesize
	binary.LittleEndian.PutUint32(f[4:8], nbytes)
	binary.LittleEndian.PutUint32(f[8:12], blocksize)
	binary.LittleEndian.PutUint32(f[12:16], cbytes)
	copy(f[headerSize:], tail)
	return f
}

// TestParseHeaderValid parses a real fixture header and checks the geometry.
func TestParseHeaderValid(t *testing.T) {
	frame := readFixture(t, "blosclz_shuffle_big_i8")
	h, err := parseHeader(frame)
	if err != nil {
		t.Fatalf("parseHeader: %v", err)
	}
	if h.typesize != 8 {
		t.Errorf("typesize = %d, want 8", h.typesize)
	}
	if h.nbytes != 4096 {
		t.Errorf("nbytes = %d, want 4096", h.nbytes)
	}
	if h.compressor != compBloscLZ {
		t.Errorf("compressor = %d, want blosclz(%d)", h.compressor, compBloscLZ)
	}
}

// TestRejectBlosc2 rejects a frame whose format version is above 2.
func TestRejectBlosc2(t *testing.T) {
	frame := makeFrame(3, 0, 8, 8, 8, headerSize+8, make([]byte, 8))
	_, err := Decode(frame)
	if err == nil || !strings.Contains(err.Error(), "version") {
		t.Fatalf("expected version-rejection error, got %v", err)
	}
}

// TestRejectVersionZero rejects a zero version byte.
func TestRejectVersionZero(t *testing.T) {
	frame := makeFrame(0, 0, 8, 8, 8, headerSize+8, make([]byte, 8))
	if _, err := Decode(frame); err == nil {
		t.Fatal("expected error for version 0, got nil")
	}
}

// TestTruncatedHeader: a frame shorter than 16 bytes errors cleanly.
func TestTruncatedHeader(t *testing.T) {
	for n := 0; n < headerSize; n++ {
		if _, err := Decode(make([]byte, n)); err == nil {
			t.Fatalf("expected error for %d-byte frame, got nil", n)
		}
	}
}

// TestZeroNbytes: an nbytes==0 frame decodes to an empty buffer.
func TestZeroNbytes(t *testing.T) {
	frame := makeFrame(2, 0, 8, 0, 0, headerSize, nil)
	out, err := Decode(frame)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if len(out) != 0 {
		t.Errorf("expected empty output, got %d bytes", len(out))
	}
}

// TestMemcpyRoundTrip: a memcpy frame returns the stored bytes verbatim.
func TestMemcpyRoundTrip(t *testing.T) {
	body := []byte{1, 2, 3, 4, 5, 6, 7, 8}
	frame := makeFrame(2, flagMemcpy, 8, uint32(len(body)), uint32(len(body)), uint32(headerSize+len(body)), body)
	out, err := Decode(frame)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if string(out) != string(body) {
		t.Errorf("memcpy out = %v, want %v", out, body)
	}
}

// TestTruncatedMemcpy: a memcpy frame whose body is shorter than nbytes errors.
func TestTruncatedMemcpy(t *testing.T) {
	frame := makeFrame(2, flagMemcpy, 8, 64, 64, headerSize+64, make([]byte, 8))
	if _, err := Decode(frame); err == nil {
		t.Fatal("expected error for truncated memcpy body, got nil")
	}
}

// TestSnappyUnsupported: a snappy-coded frame returns a clear unsupported error
// rather than panicking. We forge a non-memcpy header with the snappy compressor
// code and a one-block offset table pointing at a bogus payload.
func TestSnappyUnsupported(t *testing.T) {
	flags := byte(compSnappy << 5) // compressor in bits 5-7, no memcpy, no split control
	// nbytes=8, blocksize=8 -> 1 block; offset table (1 int32) then a 4-byte
	// clen=0 stream. With typesize=1, no splitting.
	tail := make([]byte, 4+4) // offset entry + clen prefix
	off := uint32(headerSize + 4)
	binary.LittleEndian.PutUint32(tail[0:4], off)
	binary.LittleEndian.PutUint32(tail[4:8], 0) // clen = 0
	frame := makeFrame(2, flags, 1, 8, 8, uint32(headerSize+len(tail)), tail)
	_, err := Decode(frame)
	if err == nil || !strings.Contains(err.Error(), "snappy") {
		t.Fatalf("expected snappy-unsupported error, got %v", err)
	}
}

// TestTruncatedFramesNeverPanic feeds every prefix of every fixture frame to
// Decode: each must either succeed or return an error, never panic.
func TestTruncatedFramesNeverPanic(t *testing.T) {
	names := []string{
		"blosclz_shuffle_big_i8", "lz4_shuffle_big_i8", "zstd_shuffle_big_i8",
		"lz4_noshuffle_big_i8", "blosclz_bitshuffle_big_i8", "zlib_shuffle_big_i8",
		"multiblock_i8",
	}
	for _, name := range names {
		frame := readFixture(t, name)
		for n := 0; n <= len(frame); n++ {
			func() {
				defer func() {
					if r := recover(); r != nil {
						t.Fatalf("%s[:%d] panicked: %v", name, n, r)
					}
				}()
				_, _ = Decode(frame[:n])
			}()
		}
	}
}

// TestBitFlipNeverPanics flips each byte of a fixture frame and confirms Decode
// errors or succeeds but never panics on the corrupted input.
func TestBitFlipNeverPanics(t *testing.T) {
	frame := append([]byte(nil), readFixture(t, "blosclz_shuffle_big_i8")...)
	for i := range frame {
		orig := frame[i]
		frame[i] ^= 0xFF
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("bit-flip at %d panicked: %v", i, r)
				}
			}()
			_, _ = Decode(frame)
		}()
		frame[i] = orig
	}
}
