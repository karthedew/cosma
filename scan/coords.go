package scan

import (
	"context"
	"errors"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/karthedew/cosma/internal/ndingest"
	"github.com/karthedew/cosma/store"
)

// coords.go implements WithCoords: replacing a kept dimension's integer index
// column with that dimension's coordinate VALUES. A coordinate is a 1-D store
// array whose extent matches the dim it labels. Values are loaded lazily — each
// coordinate chunk is read and decoded only when a data cell first needs an
// index inside it — and each decoded chunk is cached for the scan's lifetime, so
// the many data cells sharing a coordinate index along a dim decode that chunk
// once. The loader is consumed only from the single drain goroutine
// (arrayReader.Next -> nextBatch -> cellRows.record), never the parallel fetch
// workers, so the cache needs no locking.

// coordDim resolves coordinate values for one coord-valued dimension.
type coordDim struct {
	tree     *store.Tree
	mem      memory.Allocator
	arr      *store.Array // the 1-D coordinate array
	chunkLen int64        // arr.ChunkShape[0]
	width    int          // bytes per coordinate value
	// fill is the pre-encoded little-endian fill value, or nil when the coord
	// array has no fill value (a missing chunk then errors).
	fill []byte
	// cache maps a coordinate chunk index to its decoded little-endian bytes.
	// Buffers are mem-owned and freed by release().
	cache map[int64][]byte
}

// coordLoader holds one coordDim per coord-valued kept column, keyed by the
// kept-column position (index into arrayPlan.keptAxes / cellRows.idx).
type coordLoader struct {
	byKept map[int]*coordDim
}

// newCoordLoader builds the loader for a plan's coord-valued kept columns. It
// performs no chunk I/O; coordinate chunks are read lazily during the scan.
func newCoordLoader(p *arrayPlan, mem memory.Allocator) (*coordLoader, error) {
	if mem == nil {
		mem = memory.DefaultAllocator
	}
	l := &coordLoader{byKept: map[int]*coordDim{}}
	for i := range p.keptAxes {
		path := p.keptCoordPath[i]
		if path == "" {
			continue
		}
		carr, ok := p.tree.Array(path)
		if !ok {
			return nil, fmt.Errorf("scan: coordinate array %q not found", path)
		}
		w, err := dtypeWidth(carr.DType)
		if err != nil {
			return nil, fmt.Errorf("scan: coordinate %q: %w", path, err)
		}
		cd := &coordDim{
			tree:     p.tree,
			mem:      mem,
			arr:      carr,
			chunkLen: carr.ChunkShape[0],
			width:    w,
			cache:    map[int64][]byte{},
		}
		if carr.FillValue != nil {
			coerced, err := coerceFillValue(carr.FillValue, p.keptCoordType[i])
			if err != nil {
				return nil, fmt.Errorf("scan: coordinate %q fill value: %w", path, err)
			}
			fb, err := encodeFillValue(coerced, p.keptCoordType[i])
			if err != nil {
				return nil, fmt.Errorf("scan: coordinate %q fill value: %w", path, err)
			}
			cd.fill = fb
		}
		l.byKept[i] = cd
	}
	return l, nil
}

// isCoord reports whether kept-column position keptPos is coord-valued.
func (l *coordLoader) isCoord(keptPos int) bool {
	_, ok := l.byKept[keptPos]
	return ok
}

// appendValue appends the coordinate value at the given global dim index of the
// coord-valued kept column keptPos into the typed builder.
func (l *coordLoader) appendValue(keptPos int, fb array.Builder, globalIndex int64) error {
	return l.byKept[keptPos].appendValue(fb, globalIndex)
}

// release frees every cached decoded coordinate chunk.
func (l *coordLoader) release() {
	if l == nil {
		return
	}
	for _, cd := range l.byKept {
		for k, buf := range cd.cache {
			cd.mem.Free(buf)
			delete(cd.cache, k)
		}
	}
}

// appendValue decodes the coordinate value at globalIndex and appends it to fb.
func (c *coordDim) appendValue(fb array.Builder, globalIndex int64) error {
	chunkIdx := globalIndex / c.chunkLen
	buf, err := c.chunk(chunkIdx)
	if err != nil {
		return err
	}
	off := int(globalIndex-chunkIdx*c.chunkLen) * c.width
	appendDecodedValue(fb, buf, off)
	return nil
}

// chunk returns the decoded bytes of coordinate chunk chunkIdx, reading and
// decoding (or filling) it once and caching the result.
func (c *coordDim) chunk(chunkIdx int64) ([]byte, error) {
	if b, ok := c.cache[chunkIdx]; ok {
		return b, nil
	}
	raw, err := c.tree.Store().ReadChunk(context.Background(),
		store.ChunkKey{ArrayPath: c.arr.ArrayPath, Coord: []int64{chunkIdx}})
	if err != nil {
		if errors.Is(err, store.ErrChunkMissing) {
			b, ferr := c.fillChunk()
			if ferr != nil {
				return nil, ferr
			}
			c.cache[chunkIdx] = b
			return b, nil
		}
		return nil, fmt.Errorf("scan: coordinate %q chunk %d: %w", c.arr.ArrayPath, chunkIdx, err)
	}
	dec, err := ndingest.DecodeChunk(c.arr, raw, c.mem)
	if err != nil {
		return nil, fmt.Errorf("scan: coordinate %q chunk %d decode: %w", c.arr.ArrayPath, chunkIdx, err)
	}
	c.cache[chunkIdx] = dec
	return dec, nil
}

// fillChunk materializes a full coordinate chunk of the fill value, or errors
// when the coordinate array has no fill value.
func (c *coordDim) fillChunk() ([]byte, error) {
	if c.fill == nil {
		return nil, fmt.Errorf(
			"scan: coordinate %q has a missing chunk and no fill value "+
				"(a future WithMissingAsNull option will map missing chunks to null)",
			c.arr.ArrayPath)
	}
	n := int(c.chunkLen)
	buf := c.mem.Allocate(n * c.width)
	for i := 0; i < n; i++ {
		copy(buf[i*c.width:], c.fill)
	}
	return buf, nil
}
