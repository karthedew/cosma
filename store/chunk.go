package store

// ChunkKey is the logical identity of a chunk: which array, and the chunk's
// coordinate in the chunk grid (not the cell grid). Coord has one entry per
// array dimension; a 0-d (scalar) array has the empty coord.
type ChunkKey struct {
	ArrayPath string
	Coord     []int64
}

// ChunkRef is the physical location of a chunk's bytes. It is produced by
// drivers that can name a chunk's storage location without reading it (the
// kerchunk / manifest use case). A Length < 0 means "from Offset to the end of
// the object".
type ChunkRef struct {
	Key    ChunkKey
	URL    string
	Offset int64
	Length int64 // < 0 means to end
	Codecs []CodecSpec
}

// ChunkResolver is an optional interface a Store may implement to expose the
// physical location of a chunk (ChunkRef) separately from its bytes. Drivers
// backed by a manifest of byte ranges implement it; in-memory or path-based
// drivers may implement it with synthetic URLs. Callers must type-assert for
// it and tolerate its absence.
type ChunkResolver interface {
	ResolveChunk(key ChunkKey) (ChunkRef, error)
}
