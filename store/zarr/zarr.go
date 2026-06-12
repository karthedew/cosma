// Package zarr is a local-filesystem Zarr driver for cosma/store. It reads
// Zarr v2 (.zgroup/.zarray/.zattrs, optionally consolidated via .zmetadata) and
// Zarr v3 (per-node zarr.json) stores, parsing all metadata into store types.
//
// It exposes raw, still-compressed chunk bytes only — decoding and fill-value
// materialization belong to higher layers. The driver implements
// store.ChunkResolver: chunk file paths are derivable from metadata without
// reading any chunk bytes.
package zarr

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/karthedew/cosma/store"
)

// version discriminates the two on-disk Zarr metadata layouts.
type version int

const (
	versionV2 version = iota + 2
	versionV3
)

// metaReader is the version-specific half of the driver: metadata walking and
// chunk-path resolution. The shared FS wrapper adds ReadChunk/ResolveChunk and
// Close on top of it.
type metaReader interface {
	Meta(ctx context.Context, path string) (store.Object, error)
	List(ctx context.Context, path string) ([]store.Entry, error)
	// chunkPath returns the absolute filesystem path of a chunk without reading
	// it; the file may or may not exist.
	chunkPath(arrayPath string, coord []int64) (string, error)
}

// fsStore is the public store.Store returned by OpenFS. It delegates metadata to
// a version-specific metaReader and serves chunk bytes / refs from the
// filesystem.
type fsStore struct {
	root    string
	version version
	reader  metaReader
}

var (
	_ store.Store         = (*fsStore)(nil)
	_ store.ChunkResolver = (*fsStore)(nil)
)

// OpenFS opens the Zarr store rooted at path on the local filesystem. The
// version is sniffed at the root: a zarr.json names v3; a .zgroup or .zarray
// names v2; finding both v2 and v3 markers is an ambiguous store and errors.
func OpenFS(path string) (store.Store, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("zarr: %q is not a directory", path)
	}

	hasV3 := fileExists(filepath.Join(path, "zarr.json"))
	hasV2 := fileExists(filepath.Join(path, ".zgroup")) || fileExists(filepath.Join(path, ".zarray"))

	switch {
	case hasV2 && hasV3:
		return nil, fmt.Errorf("zarr: %q has both v2 and v3 metadata (ambiguous store)", path)
	case hasV3:
		return &fsStore{root: path, version: versionV3, reader: openV3(path)}, nil
	case hasV2:
		r, err := openV2(path)
		if err != nil {
			return nil, err
		}
		return &fsStore{root: path, version: versionV2, reader: r}, nil
	default:
		return nil, fmt.Errorf("zarr: %q is not a Zarr store (no zarr.json, .zgroup, or .zarray)", path)
	}
}

// Meta implements store.Store.
func (s *fsStore) Meta(ctx context.Context, path string) (store.Object, error) {
	return s.reader.Meta(ctx, path)
}

// List implements store.Store.
func (s *fsStore) List(ctx context.Context, path string) ([]store.Entry, error) {
	return s.reader.List(ctx, path)
}

// ReadChunk implements store.Store. It returns the raw, still-compressed bytes
// of one chunk. A chunk file that does not exist returns store.ErrChunkMissing
// (wrapped, errors.Is-able).
func (s *fsStore) ReadChunk(_ context.Context, key store.ChunkKey) ([]byte, error) {
	fp, err := s.reader.chunkPath(key.ArrayPath, key.Coord)
	if err != nil {
		return nil, err
	}
	data, err := os.ReadFile(fp)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("zarr: %s %v: %w", key.ArrayPath, key.Coord, store.ErrChunkMissing)
		}
		return nil, err
	}
	return data, nil
}

// ResolveChunk implements store.ChunkResolver. The chunk's physical location is
// derivable from metadata alone, so this never reads chunk bytes. URL is the
// absolute filesystem path; Offset is 0 and Length is -1 ("to end of file"),
// which avoids a stat — a known chunk file is the whole object. A chunk file
// that does not exist returns store.ErrChunkMissing.
func (s *fsStore) ResolveChunk(key store.ChunkKey) (store.ChunkRef, error) {
	fp, err := s.reader.chunkPath(key.ArrayPath, key.Coord)
	if err != nil {
		return store.ChunkRef{}, err
	}
	if !fileExists(fp) {
		return store.ChunkRef{}, fmt.Errorf("zarr: %s %v: %w", key.ArrayPath, key.Coord, store.ErrChunkMissing)
	}

	var codecs []store.CodecSpec
	if obj, err := s.reader.Meta(context.Background(), key.ArrayPath); err == nil {
		if a, ok := obj.(*store.Array); ok {
			codecs = a.Codecs
		}
	}
	return store.ChunkRef{
		Key:    key,
		URL:    fp,
		Offset: 0,
		Length: -1, // whole file; documented choice to avoid statting
		Codecs: codecs,
	}, nil
}

// Close implements store.Store. The filesystem driver holds no resources.
func (s *fsStore) Close() error { return nil }

// fileExists reports whether path names an existing regular file.
func fileExists(path string) bool {
	info, err := os.Stat(path)
	return err == nil && !info.IsDir()
}

// joinStorePath joins a parent store path and a child segment into an absolute,
// slash-separated store path.
func joinStorePath(parent, child string) string {
	if parent == "/" || parent == "" {
		return "/" + child
	}
	return strings.TrimRight(parent, "/") + "/" + child
}

// jsonIntSlice converts a decoded JSON array of numbers into an []int64. A nil
// input yields an empty slice (the 0-d / scalar shape).
func jsonIntSlice(raw any) ([]int64, error) {
	if raw == nil {
		return []int64{}, nil
	}
	arr, ok := raw.([]any)
	if !ok {
		return nil, fmt.Errorf("expected JSON array, got %T", raw)
	}
	out := make([]int64, len(arr))
	for i, e := range arr {
		f, ok := e.(float64)
		if !ok {
			return nil, fmt.Errorf("expected number at index %d, got %T", i, e)
		}
		out[i] = int64(f)
	}
	return out, nil
}
