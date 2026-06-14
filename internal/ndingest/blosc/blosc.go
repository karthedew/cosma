// Package blosc implements a pure-Go decoder for the blosc v1 (blosc1) container
// format, the default compressor of Zarr v2 stores.
//
// A blosc1 frame is a 16-byte header followed by a per-block int32 offset table
// and a sequence of independently compressed (or memcpy'd) blocks. Each block is
// at most blocksize bytes; the inner compressor is one of blosclz, lz4/lz4hc,
// zstd, zlib, or snappy, selected by the header flags. After the blocks are
// concatenated, a byte-shuffle or bitshuffle transform (keyed by the element
// typesize) is inverted to recover the original buffer.
//
// The decoder is bounds-checked throughout: every offset, block length, and
// inner-codec output is validated against the header geometry so truncated or
// adversarial input returns an error rather than panicking. It is registered
// with internal/ndingest under the codec ID "blosc"; the registration lives in
// register.go to keep this package free of any import on its parent.
package blosc

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

	kzlib "github.com/klauspost/compress/zlib"
	"github.com/pierrec/lz4/v4"
)

// headerSize is the fixed blosc1 header length in bytes.
const headerSize = 16

// maxNbytes caps the uncompressed size a single frame may declare, bounding the
// output allocation against a hostile header (the decoder eats untrusted bytes).
// 2 GiB comfortably exceeds any real Zarr chunk while staying within int on all
// targets; legitimate frames are far smaller.
const maxNbytes = 1 << 31

// Flag bits in header byte 2.
const (
	flagShuffle    = 0x01 // bit0: byte-shuffle was applied
	flagMemcpy     = 0x02 // bit1: blocks stored uncompressed (memcpy'd)
	flagBitShuffle = 0x04 // bit2: bitshuffle was applied
	flagDontSplit  = 0x10 // bit4: block NOT split into per-typesize sub-streams
)

// Compressor codes carried in flags bits 5-7.
const (
	compBloscLZ = 0
	compLZ4     = 1 // also lz4hc; same block format
	compSnappy  = 2
	compZlib    = 3
	compZstd    = 4
)

// header holds the parsed 16-byte blosc1 header.
type header struct {
	version    byte
	versionlz  byte
	flags      byte
	typesize   int
	nbytes     int // uncompressed size
	blocksize  int
	cbytes     int // total compressed size (whole frame)
	compressor int // 0..7 from flags bits 5-7
}

// parseHeader reads and validates the 16-byte blosc1 header. It rejects blosc2
// frames (version != 1) and any geometry that would not fit the input.
func parseHeader(src []byte) (header, error) {
	var h header
	if len(src) < headerSize {
		return h, fmt.Errorf("blosc: frame too short for header (%d bytes, need %d)", len(src), headerSize)
	}
	h.version = src[0]
	h.versionlz = src[1]
	h.flags = src[2]
	h.typesize = int(src[3])
	h.nbytes = int(binary.LittleEndian.Uint32(src[4:8]))
	h.blocksize = int(binary.LittleEndian.Uint32(src[8:12]))
	h.cbytes = int(binary.LittleEndian.Uint32(src[12:16]))
	h.compressor = int(h.flags>>5) & 0x07

	// The blosc1 chunk container reports BLOSC_VERSION_FORMAT in byte 0, which
	// is 2 in current c-blosc (1 in very old releases). blosc2 uses a higher
	// format version and an incompatible chunk layout; reject it with a clear
	// error rather than misparsing.
	if h.version == 0 || h.version > 2 {
		return h, fmt.Errorf("blosc: unsupported frame format version %d (only blosc1 containers, format version 1-2, are supported)", h.version)
	}
	if h.nbytes < 0 || h.nbytes > maxNbytes {
		return h, fmt.Errorf("blosc: declared uncompressed size %d out of range", h.nbytes)
	}
	if h.nbytes > 0 && h.blocksize <= 0 {
		return h, fmt.Errorf("blosc: invalid blocksize %d for nbytes %d", h.blocksize, h.nbytes)
	}
	if h.cbytes < headerSize || h.cbytes > len(src) {
		return h, fmt.Errorf("blosc: cbytes %d outside frame bounds (have %d)", h.cbytes, len(src))
	}
	return h, nil
}

// Decode decompresses a complete blosc1 frame and returns the original
// uncompressed buffer. It parses the header, walks the per-block offset table,
// decompresses each block with the header-selected inner codec (honoring the
// memcpy flag), concatenates them, and inverts any byte- or bit-shuffle.
func Decode(src []byte) ([]byte, error) {
	h, err := parseHeader(src)
	if err != nil {
		return nil, err
	}

	if h.nbytes == 0 {
		return []byte{}, nil
	}

	// MEMCPYED: the original (un-shuffled) buffer is stored verbatim immediately
	// after the 16-byte header — no offset table, and the shuffle is NOT applied
	// (c-blosc skips both the filter and the codec for incompressible data). This
	// is common for the small chunks zarr emits.
	if h.flags&flagMemcpy != 0 {
		if headerSize+h.nbytes > len(src) {
			return nil, fmt.Errorf("blosc: memcpy frame too short (need %d bytes, have %d)", headerSize+h.nbytes, len(src))
		}
		out := make([]byte, h.nbytes)
		copy(out, src[headerSize:headerSize+h.nbytes])
		return out, nil
	}

	nblocks := (h.nbytes + h.blocksize - 1) / h.blocksize

	// The int32 offset table immediately follows the header.
	tableEnd := headerSize + nblocks*4
	if tableEnd > len(src) {
		return nil, fmt.Errorf("blosc: frame too short for %d-block offset table", nblocks)
	}

	out := make([]byte, h.nbytes)
	var blockScratch []byte // reused per-block decompression buffer

	for b := 0; b < nblocks; b++ {
		off := int(int32(binary.LittleEndian.Uint32(src[headerSize+b*4 : headerSize+b*4+4])))
		if off < tableEnd || off > len(src) {
			return nil, fmt.Errorf("blosc: block %d offset %d outside frame", b, off)
		}

		// Uncompressed length of this block: full blocksize except the last,
		// which is the remainder.
		blockOut := h.blocksize
		if b == nblocks-1 {
			blockOut = h.nbytes - b*h.blocksize
		}
		dstStart := b * h.blocksize

		// Decompress into a per-block scratch buffer; the shuffle is inverted
		// per block (c-blosc applies filters block-by-block) before the bytes
		// land in the output.
		dst := blockScratch
		if cap(dst) < blockOut {
			dst = make([]byte, blockOut)
			blockScratch = dst
		}
		dst = dst[:blockOut]

		// A block is split into `typesize` independently compressed sub-streams
		// unless the dont-split flag is set (and typesize > 1). c-blosc splits so
		// each shuffled byte-plane compresses on its own; zstd and tiny blocks
		// carry dont_split. Each sub-stream is prefixed by its own little-endian
		// int32 compressed length and decompresses to blockOut/nstreams bytes,
		// the last taking the remainder.
		nstreams := 1
		if h.flags&flagDontSplit == 0 && h.typesize > 1 {
			nstreams = h.typesize
		}
		neblock := blockOut / nstreams
		if neblock == 0 {
			// Block smaller than typesize: cannot split, treat as one stream.
			nstreams = 1
			neblock = blockOut
		}

		cursor := off
		for s := 0; s < nstreams; s++ {
			streamOut := neblock
			if s == nstreams-1 {
				streamOut = blockOut - s*neblock // last stream gets the leftover
			}
			if cursor+4 > len(src) {
				return nil, fmt.Errorf("blosc: block %d stream %d truncated before length prefix", b, s)
			}
			clen := int(int32(binary.LittleEndian.Uint32(src[cursor : cursor+4])))
			payloadStart := cursor + 4
			if clen < 0 || payloadStart+clen > len(src) {
				return nil, fmt.Errorf("blosc: block %d stream %d compressed length %d outside frame", b, s, clen)
			}
			payload := src[payloadStart : payloadStart+clen]
			sub := dst[s*neblock : s*neblock+streamOut]
			if err := decodeBlock(h.compressor, payload, sub); err != nil {
				return nil, fmt.Errorf("blosc: block %d stream %d: %w", b, s, err)
			}
			cursor = payloadStart + clen
		}

		// Invert this block's shuffle filter, if any, copying into the output.
		// bitshuffle takes precedence over byte-shuffle (they are mutually
		// exclusive in practice; the order mirrors c-blosc).
		switch {
		case h.typesize > 1 && blockOut >= h.typesize && h.flags&flagBitShuffle != 0:
			un, err := unBitShuffle(dst, h.typesize)
			if err != nil {
				return nil, fmt.Errorf("blosc: block %d: %w", b, err)
			}
			copy(out[dstStart:dstStart+blockOut], un)
		case h.typesize > 1 && blockOut >= h.typesize && h.flags&flagShuffle != 0:
			copy(out[dstStart:dstStart+blockOut], unByteShuffle(dst, h.typesize))
		default:
			copy(out[dstStart:dstStart+blockOut], dst)
		}
	}

	return out, nil
}

// decodeBlock decompresses a single inner-codec block into dst, which is sized
// to the block's expected uncompressed length.
func decodeBlock(compressor int, payload, dst []byte) error {
	switch compressor {
	case compBloscLZ:
		n, err := blosclzDecompress(payload, dst)
		if err != nil {
			return err
		}
		if n != len(dst) {
			return fmt.Errorf("blosclz: decoded %d bytes, expected %d", n, len(dst))
		}
		return nil

	case compLZ4:
		n, err := lz4.UncompressBlock(payload, dst)
		if err != nil {
			return fmt.Errorf("lz4: %w", err)
		}
		if n != len(dst) {
			return fmt.Errorf("lz4: decoded %d bytes, expected %d", n, len(dst))
		}
		return nil

	case compZstd:
		dec, err := sharedZstd()
		if err != nil {
			return err
		}
		got, err := dec.DecodeAll(payload, dst[:0])
		if err != nil {
			return fmt.Errorf("zstd: %w", err)
		}
		if len(got) != len(dst) {
			return fmt.Errorf("zstd: decoded %d bytes, expected %d", len(got), len(dst))
		}
		return nil

	case compZlib:
		r, err := kzlib.NewReader(bytes.NewReader(payload))
		if err != nil {
			return fmt.Errorf("zlib: %w", err)
		}
		defer r.Close()
		n, err := io.ReadFull(r, dst)
		if err != nil && err != io.EOF {
			return fmt.Errorf("zlib: %w", err)
		}
		if n != len(dst) {
			return fmt.Errorf("zlib: decoded %d bytes, expected %d", n, len(dst))
		}
		return nil

	case compSnappy:
		return fmt.Errorf("snappy inner codec unsupported (no snappy dependency; rare in zarr)")

	default:
		return fmt.Errorf("unknown inner compressor code %d", compressor)
	}
}
