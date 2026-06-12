// Package store is Layer 1 of Cosma's n-dimensional store stack: a
// format-agnostic, faithful mirror of a chunked store's hierarchy.
//
// It carries no coordinates, no alignment, and no convention semantics — those
// belong to Layer 2 (cosma/dataset). A driver implements the four-method Store
// interface and everything above it (Tree, Group, Array, walks, printing) is
// generic. Drivers expose raw, still-compressed chunk bytes; decoding and
// fill-value materialization are the concern of higher layers.
package store

import (
	"context"
	"errors"
)

// ErrChunkMissing is returned by Store.ReadChunk when the requested chunk does
// not exist in the store. Missing chunks are normal (sparse arrays); deciding
// what a missing chunk means — fill value, null, error — is the decoder's job,
// not the store's. Callers should test for it with errors.Is.
var ErrChunkMissing = errors.New("store: chunk missing")

// ObjectKind discriminates the two node kinds in a store hierarchy.
type ObjectKind int

const (
	// KindGroup is an interior node that may contain children.
	KindGroup ObjectKind = iota
	// KindArray is a leaf node holding a chunked n-dimensional array.
	KindArray
)

// String returns the lowercase name of the kind ("group" or "array").
func (k ObjectKind) String() string {
	switch k {
	case KindGroup:
		return "group"
	case KindArray:
		return "array"
	default:
		return "unknown"
	}
}

// Entry is one direct child of a group as reported by Store.List. Name is the
// final path segment only (not an absolute path).
type Entry struct {
	Name string
	Kind ObjectKind
}

// Store is the driver contract: a dumb, faithful catalog of a chunked store.
// All methods must be safe for concurrent use by multiple goroutines.
//
// Paths are absolute, slash-separated, with "/" as the root. List and Meta walk
// metadata only; ReadChunk is the sole byte-reading method and returns chunk
// payloads exactly as stored (still compressed/encoded).
type Store interface {
	// List returns the direct children of the group at path. The root group is
	// addressed as "/". Listing a non-group (or missing path) returns an error.
	List(ctx context.Context, path string) ([]Entry, error)

	// Meta returns the metadata Object (Group or Array) at path.
	Meta(ctx context.Context, path string) (Object, error)

	// ReadChunk returns the raw, still-compressed bytes of one chunk. A chunk
	// that does not exist returns ErrChunkMissing; fill-value handling is left
	// to the decoder.
	ReadChunk(ctx context.Context, key ChunkKey) ([]byte, error)

	// Close releases any driver-held resources. It is safe to call once after
	// the store is no longer needed.
	Close() error
}
