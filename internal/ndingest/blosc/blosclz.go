package blosc

import "fmt"

// blosclz constants, mirroring the c-blosc reference (blosclz.c). MAX_DISTANCE
// bounds the 13-bit near distance; MAX_FARDISTANCE the 16-bit extended one.
const (
	blosclzMaxDistance = 8191
	blosclzMaxCopy     = 32
)

// blosclzDecompress decompresses one blosclz-compressed block into dst and
// returns the number of bytes written. blosclz is the small LZ77 variant built
// into c-blosc; this is a faithful, bounds-checked port of the reference
// blosclz_decompress (BLOSCLZ_SAFE path), which is the only decoder blosc1
// frames need.
//
// Stream layout (per the reference):
//   - The first control byte is masked with 31, so the first op is always a
//     literal run of (ctrl&31)+1 bytes (the compressor guarantees this).
//   - In the loop, a control byte >= 32 is a back-reference: bits 5-7 hold
//     (length-1) with 3 added later (min match 3); a stored length nibble of 6
//     (i.e. 7-1) triggers a 255-continued length extension. The low 5 bits are
//     the high bits of the distance; one more byte gives the low 8 bits. When
//     that offset byte is 255 and the near-distance field is saturated
//     (ofs == 31<<8), two more big-endian bytes give a far distance, rebased by
//     MAX_DISTANCE.
//   - A control byte < 32 is a literal run of ctrl+1 bytes copied verbatim.
//
// dst must be sized to the block's expected uncompressed length; every write and
// back-reference is bounds-checked so truncated or hostile input errors cleanly
// instead of panicking or over-reading.
func blosclzDecompress(src, dst []byte) (int, error) {
	srcLen := len(src)
	dstLen := len(dst)
	if srcLen == 0 {
		return 0, nil
	}

	ip := 0 // input cursor
	op := 0 // output cursor

	ctrl := int(src[ip]) & 31
	ip++
	loop := true

	for loop {
		ref := op
		length := ctrl >> 5
		ofs := (ctrl & 31) << 8

		if ctrl >= 32 {
			length--
			ref -= ofs

			if length == 7-1 {
				for {
					if ip >= srcLen {
						return 0, fmt.Errorf("blosclz: truncated reading match length")
					}
					code := int(src[ip])
					ip++
					length += code
					if code != 255 {
						break
					}
				}
			}
			if ip >= srcLen {
				return 0, fmt.Errorf("blosclz: truncated reading match offset")
			}
			code := int(src[ip])
			ip++
			ref -= code

			if code == 255 && ofs == (31<<8) {
				if ip+1 >= srcLen {
					return 0, fmt.Errorf("blosclz: truncated reading far distance")
				}
				ofs = int(src[ip]) << 8
				ip++
				ofs += int(src[ip])
				ip++
				ref = op - ofs - blosclzMaxDistance
			}

			// Bounds checks (BLOSCLZ_SAFE): the match plus its +3 minimum must
			// fit, and the reference (after the -- below) must stay in output.
			if op+length+3 > dstLen {
				return 0, fmt.Errorf("blosclz: match overruns output (op=%d len=%d dst=%d)", op, length+3, dstLen)
			}
			if ref-1 < 0 {
				return 0, fmt.Errorf("blosclz: back-reference before output start")
			}

			if ip < srcLen {
				ctrl = int(src[ip])
				ip++
			} else {
				loop = false
			}

			if ref == op {
				// Run of a single repeated byte (ref[-1]).
				b := dst[ref-1]
				n := length + 3
				for i := 0; i < n; i++ {
					dst[op+i] = b
				}
				op += n
			} else {
				ref--
				n := length + 3
				// Byte-by-byte to honor overlapping matches (ref close to op).
				for i := 0; i < n; i++ {
					dst[op+i] = dst[ref+i]
				}
				op += n
			}
		} else {
			// Literal run of ctrl+1 bytes.
			n := ctrl + 1
			if op+n > dstLen {
				return 0, fmt.Errorf("blosclz: literal run overruns output (op=%d len=%d dst=%d)", op, n, dstLen)
			}
			if ip+n > srcLen {
				return 0, fmt.Errorf("blosclz: literal run overruns input (ip=%d len=%d src=%d)", ip, n, srcLen)
			}
			copy(dst[op:op+n], src[ip:ip+n])
			op += n
			ip += n

			if ip < srcLen {
				ctrl = int(src[ip])
				ip++
			} else {
				loop = false
			}
		}
	}

	return op, nil
}
