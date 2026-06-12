// Package memstore is a map-backed, in-memory implementation of store.Store for
// tests. It offers a fluent builder so higher layers (Z4–Z9) can assemble store
// fixtures without disk. It also implements store.ChunkResolver with synthetic
// URLs so the ChunkResolver seam is exercised.
package memstore

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/karthedew/cosma/store"
)

// chunkKey is the string form of a logical chunk identity, used as a map key.
func chunkKey(arrayPath string, coord []int64) string {
	parts := make([]string, len(coord))
	for i, c := range coord {
		parts[i] = fmt.Sprintf("%d", c)
	}
	return arrayPath + "\x00" + strings.Join(parts, ".")
}

// Store is an in-memory store. It is safe for concurrent reads after building;
// builder methods are not safe to interleave with reads.
type Store struct {
	mu       sync.RWMutex
	objects  map[string]store.Object // absolute path -> Group or Array
	children map[string][]store.Entry
	chunks   map[string][]byte // chunkKey -> raw bytes
}

// New returns an empty in-memory store with a root group.
func New() *Store {
	s := &Store{
		objects:  make(map[string]store.Object),
		children: make(map[string][]store.Entry),
		chunks:   make(map[string][]byte),
	}
	s.objects["/"] = &store.Group{GroupPath: "/"}
	return s
}

// AddGroup adds a group at path with the given attrs and returns the store for
// chaining. Intermediate parent groups must already exist; the root "/" always
// does. It panics on a malformed path or a missing parent, which surfaces test
// fixture mistakes immediately.
func (s *Store) AddGroup(path string, attrs store.Attrs) *Store {
	s.addNode(path, &store.Group{GroupPath: path, GroupAttrs: attrs}, store.KindGroup)
	return s
}

// AddArray adds an array at path described by spec, with chunks keyed by their
// dotted coordinate string (e.g. "0.1" for chunk [0,1], "" for a scalar). The
// spec's ArrayPath is overwritten with path. Returns the store for chaining.
func (s *Store) AddArray(path string, spec store.Array, chunks map[string][]byte) *Store {
	spec.ArrayPath = path
	a := spec
	s.addNode(path, &a, store.KindArray)

	s.mu.Lock()
	for coordStr, data := range chunks {
		s.chunks[chunkKey(path, parseCoord(coordStr))] = data
	}
	s.mu.Unlock()
	return s
}

// parseCoord parses a dotted coordinate string ("0.1.2") into a coord slice.
// The empty string yields the empty (scalar) coord.
func parseCoord(s string) []int64 {
	if s == "" {
		return nil
	}
	parts := strings.Split(s, ".")
	coord := make([]int64, len(parts))
	for i, p := range parts {
		var v int64
		fmt.Sscanf(p, "%d", &v)
		coord[i] = v
	}
	return coord
}

// addNode registers a node and links it into its parent's child entry list.
// Group.Children is deliberately left empty in Meta results — child discovery
// is List's job (this mirrors drivers like zarr v2 whose group metadata holds
// no child listing); store.Tree fills Children on its own copies.
func (s *Store) addNode(path string, obj store.Object, kind store.ObjectKind) {
	if path == "" || path == "/" || !strings.HasPrefix(path, "/") {
		panic(fmt.Sprintf("memstore: invalid path %q", path))
	}
	parent := parentPath(path)

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.objects[parent].(*store.Group); !ok {
		panic(fmt.Sprintf("memstore: parent group %q of %q does not exist", parent, path))
	}
	if _, exists := s.objects[path]; exists {
		panic(fmt.Sprintf("memstore: node %q already exists", path))
	}

	s.objects[path] = obj
	s.children[parent] = append(s.children[parent], store.Entry{Name: lastSegment(path), Kind: kind})
}

// parentPath returns the parent of an absolute path; the parent of a top-level
// node is "/".
func parentPath(path string) string {
	path = strings.TrimRight(path, "/")
	i := strings.LastIndex(path, "/")
	if i <= 0 {
		return "/"
	}
	return path[:i]
}

// lastSegment returns the final path segment.
func lastSegment(path string) string {
	path = strings.TrimRight(path, "/")
	if i := strings.LastIndex(path, "/"); i >= 0 {
		return path[i+1:]
	}
	return path
}

// List implements store.Store.
func (s *Store) List(_ context.Context, path string) ([]store.Entry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if _, ok := s.objects[path].(*store.Group); !ok {
		if _, exists := s.objects[path]; !exists {
			return nil, fmt.Errorf("memstore: no such path %q", path)
		}
		return nil, fmt.Errorf("memstore: %q is not a group", path)
	}
	entries := s.children[path]
	out := make([]store.Entry, len(entries))
	copy(out, entries)
	return out, nil
}

// Meta implements store.Store.
func (s *Store) Meta(_ context.Context, path string) (store.Object, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	obj, ok := s.objects[path]
	if !ok {
		return nil, fmt.Errorf("memstore: no such path %q", path)
	}
	return obj, nil
}

// ReadChunk implements store.Store. A chunk that was never added returns
// store.ErrChunkMissing.
func (s *Store) ReadChunk(_ context.Context, key store.ChunkKey) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	data, ok := s.chunks[chunkKey(key.ArrayPath, key.Coord)]
	if !ok {
		return nil, fmt.Errorf("memstore: %s%v: %w", key.ArrayPath, key.Coord, store.ErrChunkMissing)
	}
	out := make([]byte, len(data))
	copy(out, data)
	return out, nil
}

// Close implements store.Store. It is a no-op for the in-memory store.
func (s *Store) Close() error { return nil }

// ResolveChunk implements store.ChunkResolver with a synthetic URL, exercising
// the physical-location seam. A chunk that was never added returns
// store.ErrChunkMissing.
func (s *Store) ResolveChunk(key store.ChunkKey) (store.ChunkRef, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	ck := chunkKey(key.ArrayPath, key.Coord)
	data, ok := s.chunks[ck]
	if !ok {
		return store.ChunkRef{}, fmt.Errorf("memstore: %s%v: %w", key.ArrayPath, key.Coord, store.ErrChunkMissing)
	}
	var codecs []store.CodecSpec
	if a, ok := s.objects[key.ArrayPath].(*store.Array); ok {
		codecs = a.Codecs
	}
	return store.ChunkRef{
		Key:    key,
		URL:    "mem://" + coordURL(ck),
		Offset: 0,
		Length: int64(len(data)),
		Codecs: codecs,
	}, nil
}

// coordURL renders an internal chunk map key as a stable URL path segment.
func coordURL(ck string) string {
	return strings.ReplaceAll(ck, "\x00", "/")
}

// SortedChunkKeys returns the dotted coordinate keys held for an array, sorted,
// for deterministic test assertions.
func (s *Store) SortedChunkKeys(arrayPath string) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	prefix := arrayPath + "\x00"
	var out []string
	for k := range s.chunks {
		if strings.HasPrefix(k, prefix) {
			out = append(out, strings.TrimPrefix(k, prefix))
		}
	}
	sort.Strings(out)
	return out
}

var _ store.Store = (*Store)(nil)
var _ store.ChunkResolver = (*Store)(nil)
