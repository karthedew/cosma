package zarr

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/karthedew/cosma/store"
)

// v2Store is the Zarr v2 driver. It reads .zgroup/.zarray/.zattrs metadata from
// the filesystem, or — when a root .zmetadata (consolidated metadata, v1) is
// present — serves all metadata from that single document.
type v2Store struct {
	root string

	// consolidated is the parsed .zmetadata "metadata" map (relative key ->
	// decoded JSON object), or nil for an unconsolidated store.
	consolidated map[string]map[string]any
}

// openV2 builds a v2 store rooted at dir, loading consolidated metadata if a
// .zmetadata file is present.
func openV2(dir string) (*v2Store, error) {
	s := &v2Store{root: dir}
	zmeta := filepath.Join(dir, ".zmetadata")
	if data, err := os.ReadFile(zmeta); err == nil {
		var doc struct {
			Metadata map[string]map[string]any `json:"metadata"`
		}
		if err := json.Unmarshal(data, &doc); err != nil {
			return nil, fmt.Errorf("zarr: parse .zmetadata: %w", err)
		}
		s.consolidated = doc.Metadata
	} else if !os.IsNotExist(err) {
		return nil, err
	}
	return s, nil
}

// metaKey turns an absolute store path and a metadata filename into the relative
// key used in .zmetadata (e.g. ("/sub/counts", ".zarray") -> "sub/counts/.zarray";
// ("/", ".zgroup") -> ".zgroup").
func metaKey(path, file string) string {
	rel := strings.Trim(path, "/")
	if rel == "" {
		return file
	}
	return rel + "/" + file
}

// readMeta returns the decoded JSON object for a metadata file at path, looking
// in consolidated metadata first, then on disk. found is false when the file is
// absent (e.g. an array has no .zattrs).
func (s *v2Store) readMeta(path, file string) (obj map[string]any, found bool, err error) {
	key := metaKey(path, file)
	if s.consolidated != nil {
		m, ok := s.consolidated[key]
		return m, ok, nil
	}
	fp := filepath.Join(s.root, filepath.FromSlash(strings.Trim(path, "/")), file)
	data, err := os.ReadFile(fp)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, false, nil
		}
		return nil, false, err
	}
	var m map[string]any
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, false, fmt.Errorf("zarr: parse %s: %w", key, err)
	}
	return m, true, nil
}

// Meta implements store.Store for v2: a path is a group if it has .zgroup, an
// array if it has .zarray.
func (s *v2Store) Meta(_ context.Context, path string) (store.Object, error) {
	if _, ok, err := s.readMeta(path, ".zgroup"); err != nil {
		return nil, err
	} else if ok {
		attrs, err := s.readAttrs(path)
		if err != nil {
			return nil, err
		}
		return &store.Group{GroupPath: path, GroupAttrs: attrs}, nil
	}

	zarray, ok, err := s.readMeta(path, ".zarray")
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("zarr: no group or array at %q", path)
	}
	attrs, err := s.readAttrs(path)
	if err != nil {
		return nil, err
	}
	return s.parseArray(path, zarray, attrs)
}

func (s *v2Store) readAttrs(path string) (store.Attrs, error) {
	m, ok, err := s.readMeta(path, ".zattrs")
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, nil
	}
	return store.Attrs(m), nil
}

// parseArray builds a store.Array from a decoded .zarray object and its attrs.
func (s *v2Store) parseArray(path string, z map[string]any, attrs store.Attrs) (*store.Array, error) {
	shape, err := jsonIntSlice(z["shape"])
	if err != nil {
		return nil, fmt.Errorf("zarr: %q shape: %w", path, err)
	}
	chunks, err := jsonIntSlice(z["chunks"])
	if err != nil {
		return nil, fmt.Errorf("zarr: %q chunks: %w", path, err)
	}

	dtypeRaw, ok := z["dtype"].(string)
	if !ok {
		return nil, fmt.Errorf("zarr: %q has non-string or structured dtype %v (unsupported)", path, z["dtype"])
	}
	dt, err := parseV2DType(dtypeRaw)
	if err != nil {
		return nil, fmt.Errorf("zarr: %q: %w", path, err)
	}

	if filters, ok := z["filters"].([]any); ok && len(filters) > 0 {
		return nil, fmt.Errorf("zarr: %q uses filters, which are unsupported", path)
	}

	order := store.OrderC
	if o, _ := z["order"].(string); o == "F" {
		order = store.OrderF
	}

	fill, err := parseV2FillValue(z["fill_value"], dt)
	if err != nil {
		return nil, fmt.Errorf("zarr: %q fill_value: %w", path, err)
	}

	codecs := v2Codecs(z["compressor"])

	var dimNames []string
	if attrs != nil {
		if dn, ok := attrs.Strings("_ARRAY_DIMENSIONS"); ok {
			dimNames = dn
		}
	}

	return &store.Array{
		ArrayPath:  path,
		ArrayAttrs: attrs,
		Shape:      shape,
		ChunkShape: chunks,
		DType:      dt.DType,
		Endianness: dt.Endianness,
		Codecs:     codecs,
		FillValue:  fill,
		Order:      order,
		DimNames:   dimNames,
	}, nil
}

// v2Codecs turns a v2 "compressor" object into a one-element codec list, or nil
// when the compressor is null (uncompressed). v2 has at most one compressor.
func v2Codecs(raw any) []store.CodecSpec {
	m, ok := raw.(map[string]any)
	if !ok {
		return nil
	}
	id, _ := m["id"].(string)
	cfg := make(map[string]any, len(m))
	for k, v := range m {
		if k == "id" {
			continue
		}
		cfg[k] = v
	}
	return []store.CodecSpec{{ID: id, Config: cfg}}
}

// dimSeparator returns the chunk dimension separator for the array at path,
// defaulting to ".".
func (s *v2Store) dimSeparator(path string) (string, error) {
	z, ok, err := s.readMeta(path, ".zarray")
	if err != nil {
		return "", err
	}
	if !ok {
		return "", fmt.Errorf("zarr: no array at %q", path)
	}
	if sep, ok := z["dimension_separator"].(string); ok && sep != "" {
		return sep, nil
	}
	return ".", nil
}

// List implements store.Store for v2. Children are discovered by directory
// listing (unconsolidated) or by scanning consolidated keys.
func (s *v2Store) List(_ context.Context, path string) ([]store.Entry, error) {
	// path must be a group
	if _, ok, err := s.readMeta(path, ".zgroup"); err != nil {
		return nil, err
	} else if !ok {
		if _, aok, _ := s.readMeta(path, ".zarray"); aok {
			return nil, fmt.Errorf("zarr: %q is an array, not a group", path)
		}
		return nil, fmt.Errorf("zarr: no group at %q", path)
	}

	if s.consolidated != nil {
		return s.listConsolidated(path), nil
	}
	return s.listDir(path)
}

// listDir lists direct child groups/arrays of a group by reading the directory.
func (s *v2Store) listDir(path string) ([]store.Entry, error) {
	dir := filepath.Join(s.root, filepath.FromSlash(strings.Trim(path, "/")))
	ents, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var out []store.Entry
	for _, e := range ents {
		if !e.IsDir() {
			continue
		}
		childPath := joinStorePath(path, e.Name())
		if _, ok, _ := s.readMeta(childPath, ".zgroup"); ok {
			out = append(out, store.Entry{Name: e.Name(), Kind: store.KindGroup})
		} else if _, ok, _ := s.readMeta(childPath, ".zarray"); ok {
			out = append(out, store.Entry{Name: e.Name(), Kind: store.KindArray})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

// listConsolidated lists direct children of a group from consolidated keys.
func (s *v2Store) listConsolidated(path string) []store.Entry {
	prefix := strings.Trim(path, "/")
	if prefix != "" {
		prefix += "/"
	}
	seen := map[string]store.ObjectKind{}
	for key := range s.consolidated {
		if !strings.HasSuffix(key, "/.zgroup") && !strings.HasSuffix(key, "/.zarray") &&
			key != ".zgroup" && key != ".zarray" {
			continue
		}
		dir := strings.TrimSuffix(strings.TrimSuffix(key, "/.zgroup"), "/.zarray")
		if key == ".zgroup" || key == ".zarray" {
			dir = ""
		}
		if dir == strings.TrimSuffix(prefix, "/") {
			continue // the group itself
		}
		if !strings.HasPrefix(dir, prefix) {
			continue
		}
		rest := strings.TrimPrefix(dir, prefix)
		if strings.Contains(rest, "/") {
			continue // not a direct child
		}
		if strings.HasSuffix(key, ".zgroup") {
			seen[rest] = store.KindGroup
		} else {
			seen[rest] = store.KindArray
		}
	}
	out := make([]store.Entry, 0, len(seen))
	for name, kind := range seen {
		out = append(out, store.Entry{Name: name, Kind: kind})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// chunkPath returns the absolute filesystem path of the chunk at coord for the
// v2 array at arrayPath, honoring its dimension_separator. A 0-d array uses the
// key "0".
func (s *v2Store) chunkPath(arrayPath string, coord []int64) (string, error) {
	sep, err := s.dimSeparator(arrayPath)
	if err != nil {
		return "", err
	}
	key := v2ChunkKey(coord, sep)
	base := filepath.Join(s.root, filepath.FromSlash(strings.Trim(arrayPath, "/")))
	return filepath.Join(base, filepath.FromSlash(key)), nil
}

// v2ChunkKey renders a chunk coordinate as a v2 chunk key with the given
// separator. The 0-d (empty coord) key is "0".
func v2ChunkKey(coord []int64, sep string) string {
	if len(coord) == 0 {
		return "0"
	}
	parts := make([]string, len(coord))
	for i, c := range coord {
		parts[i] = strconv.FormatInt(c, 10)
	}
	return strings.Join(parts, sep)
}
