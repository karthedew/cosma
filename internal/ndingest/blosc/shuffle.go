package blosc

import (
	"fmt"
	"sync"

	"github.com/klauspost/compress/zstd"
)

// unByteShuffle inverts blosc's byte-shuffle: the shuffled buffer stores byte 0
// of every element first, then byte 1 of every element, and so on. The inverse
// regroups the bytes back into element order. It returns a fresh buffer.
//
// blosc applies the shuffle only over the largest typesize-aligned prefix of the
// buffer; any trailing bytes (nbytes % typesize) are left in place. We mirror
// that so partial-element tails round-trip exactly.
func unByteShuffle(src []byte, typesize int) []byte {
	n := len(src)
	count := n / typesize // number of whole elements
	dst := make([]byte, n)

	for j := 0; j < typesize; j++ {
		// src[j*count : j*count+count] holds byte j of every element.
		base := j * count
		for i := 0; i < count; i++ {
			dst[i*typesize+j] = src[base+i]
		}
	}
	// Copy the unshuffled tail (bytes beyond count*typesize) verbatim.
	if rem := count * typesize; rem < n {
		copy(dst[rem:], src[rem:])
	}
	return dst
}

// unBitShuffle inverts blosc's bitshuffle over one decompressed block, in place.
// It is a faithful scalar port of c-blosc's bshuf_untrans_bit_elem_scal, which
// is the composition of a byte-bitrow transpose followed by a per-8-element bit
// shuffle (TRANS_BIT_8X8). bitshuffle operates on whole 8-element groups; the
// trailing (size % 8) elements are stored as a plain byte-shuffle by c-blosc, so
// we invert those with unByteShuffle over the tail.
//
// blk is one block's bytes; typesize is the element width. Returns a fresh
// buffer of the same length.
func unBitShuffle(blk []byte, typesize int) ([]byte, error) {
	n := len(blk)
	size := n / typesize // number of elements in this block
	if size == 0 {
		// Fewer than one element: nothing to unshuffle.
		out := make([]byte, n)
		copy(out, blk)
		return out, nil
	}

	main := size - size%8 // elements handled by true bitshuffle (multiple of 8)
	out := make([]byte, n)

	if main > 0 {
		mainBytes := main * typesize
		tmp := make([]byte, mainBytes)
		// Step 1: byte-bitrow transpose (in -> tmp).
		bshufTransByteBitrow(blk[:mainBytes], tmp, main, typesize)
		// Step 2: shuffle bits within 8-element blocks (tmp -> out).
		bshufShuffleBitEightelem(tmp, out[:mainBytes], main, typesize)
	}

	// Trailing elements (size % 8) were byte-shuffled, not bit-shuffled, over the
	// remaining region. c-blosc shuffles those last (size-main) elements as a
	// standalone byte-shuffle; invert it the same way.
	if main < size {
		tailElems := size - main
		tail := unByteShuffle(blk[main*typesize:], typesize)
		copy(out[main*typesize:], tail[:tailElems*typesize])
	}
	return out, nil
}

// bshufTransByteBitrow ports c-blosc's bshuf_trans_byte_bitrow_scal: a transpose
// over the (elem_size * 8) bit-rows. size must be a multiple of 8.
func bshufTransByteBitrow(in, out []byte, size, elemSize int) {
	nbyteRow := size / 8
	for jj := 0; jj < elemSize; jj++ {
		for ii := 0; ii < nbyteRow; ii++ {
			for kk := 0; kk < 8; kk++ {
				out[ii*8*elemSize+jj*8+kk] = in[(jj*8+kk)*nbyteRow+ii]
			}
		}
	}
}

// bshufShuffleBitEightelem ports c-blosc's bshuf_shuffle_bit_eightelem_scal:
// within each 8-element block it reads typesize int64 words, transposes their
// 8x8 bit matrices (TRANS_BIT_8X8), and scatters the bytes back. size must be a
// multiple of 8.
func bshufShuffleBitEightelem(in, out []byte, size, elemSize int) {
	nbyte := elemSize * size
	for jj := 0; jj < 8*elemSize; jj += 8 {
		for ii := 0; ii+8*elemSize-1 < nbyte; ii += 8 * elemSize {
			x := le64(in[ii+jj:])
			x = transBit8x8(x)
			for kk := 0; kk < 8; kk++ {
				out[ii+jj/8+kk*elemSize] = byte(x)
				x >>= 8
			}
		}
	}
}

// transBit8x8 is the TRANS_BIT_8X8 macro: it transposes the 8x8 bit matrix
// packed into a 64-bit word.
func transBit8x8(x uint64) uint64 {
	var t uint64
	t = (x ^ (x >> 7)) & 0x00AA00AA00AA00AA
	x = x ^ t ^ (t << 7)
	t = (x ^ (x >> 14)) & 0x0000CCCC0000CCCC
	x = x ^ t ^ (t << 14)
	t = (x ^ (x >> 28)) & 0x00000000F0F0F0F0
	x = x ^ t ^ (t << 28)
	return x
}

// le64 reads a little-endian uint64 from b (b must have >= 8 bytes). The
// reference casts the buffer to int64*, which on the supported little-endian
// targets is a little-endian load; we make that explicit.
func le64(b []byte) uint64 {
	return uint64(b[0]) | uint64(b[1])<<8 | uint64(b[2])<<16 | uint64(b[3])<<24 |
		uint64(b[4])<<32 | uint64(b[5])<<40 | uint64(b[6])<<48 | uint64(b[7])<<56
}

// sharedZstd returns a process-wide, concurrency-safe zstd decoder. A zstd
// decoder created with no writer is safe for concurrent DecodeAll calls.
var (
	zstdOnce sync.Once
	zstdDec  *zstd.Decoder
	zstdErr  error
)

func sharedZstd() (*zstd.Decoder, error) {
	zstdOnce.Do(func() {
		zstdDec, zstdErr = zstd.NewReader(nil)
	})
	if zstdErr != nil {
		return nil, fmt.Errorf("zstd reader: %w", zstdErr)
	}
	return zstdDec, nil
}
