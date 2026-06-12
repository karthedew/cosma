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

// v3Store is the Zarr v3 driver. Each node carries its own zarr.json; children
// are discovered by directory listing.
type v3Store struct {
	root string
}

func openV3(dir string) *v3Store { return &v3Store{root: dir} }

// nodeJSON returns the decoded zarr.json for the node at path; found is false
// when there is no zarr.json there.
func (s *v3Store) nodeJSON(path string) (m map[string]any, found bool, err error) {
	fp := filepath.Join(s.root, filepath.FromSlash(strings.Trim(path, "/")), "zarr.json")
	data, err := os.ReadFile(fp)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, false, nil
		}
		return nil, false, err
	}
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, false, fmt.Errorf("zarr: parse %s zarr.json: %w", path, err)
	}
	return m, true, nil
}

// Meta implements store.Store for v3.
func (s *v3Store) Meta(_ context.Context, path string) (store.Object, error) {
	m, ok, err := s.nodeJSON(path)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("zarr: no node at %q", path)
	}
	nodeType, _ := m["node_type"].(string)
	attrs := v3Attrs(m)
	switch nodeType {
	case "group":
		return &store.Group{GroupPath: path, GroupAttrs: attrs}, nil
	case "array":
		return s.parseArray(path, m, attrs)
	default:
		return nil, fmt.Errorf("zarr: %q has unknown node_type %q", path, nodeType)
	}
}

func v3Attrs(m map[string]any) store.Attrs {
	if a, ok := m["attributes"].(map[string]any); ok && len(a) > 0 {
		return store.Attrs(a)
	}
	return nil
}

// parseArray builds a store.Array from a v3 array zarr.json.
func (s *v3Store) parseArray(path string, m map[string]any, attrs store.Attrs) (*store.Array, error) {
	shape, err := jsonIntSlice(m["shape"])
	if err != nil {
		return nil, fmt.Errorf("zarr: %q shape: %w", path, err)
	}

	chunkShape, err := v3ChunkShape(m["chunk_grid"])
	if err != nil {
		return nil, fmt.Errorf("zarr: %q: %w", path, err)
	}

	dtName, ok := m["data_type"].(string)
	if !ok {
		return nil, fmt.Errorf("zarr: %q has non-string data_type %v (unsupported)", path, m["data_type"])
	}
	dt, err := parseV3DType(dtName)
	if err != nil {
		return nil, fmt.Errorf("zarr: %q: %w", path, err)
	}

	codecs, endian, err := v3Codecs(m["codecs"])
	if err != nil {
		return nil, fmt.Errorf("zarr: %q: %w", path, err)
	}

	fill, err := parseV3FillValue(m["fill_value"], dt)
	if err != nil {
		return nil, fmt.Errorf("zarr: %q fill_value: %w", path, err)
	}

	var dimNames []string
	if dn, ok := m["dimension_names"].([]any); ok {
		dimNames = make([]string, len(dn))
		for i, e := range dn {
			if s, ok := e.(string); ok {
				dimNames[i] = s
			}
		}
	}

	return &store.Array{
		ArrayPath:  path,
		ArrayAttrs: attrs,
		Shape:      shape,
		ChunkShape: chunkShape,
		DType:      dt,
		Endianness: endian,
		Codecs:     codecs,
		FillValue:  fill,
		Order:      store.OrderC, // v3 has no F order; layout is fixed C
		DimNames:   dimNames,
	}, nil
}

// v3ChunkShape extracts the chunk shape from a chunk_grid object. Only the
// "regular" grid is supported; anything else errors.
func v3ChunkShape(raw any) ([]int64, error) {
	grid, ok := raw.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("missing chunk_grid")
	}
	name, _ := grid["name"].(string)
	if name != "regular" {
		return nil, fmt.Errorf("unsupported chunk_grid %q (only \"regular\" is supported)", name)
	}
	cfg, _ := grid["configuration"].(map[string]any)
	return jsonIntSlice(cfg["chunk_shape"])
}

// v3Codecs passes the v3 codec list through verbatim into []store.CodecSpec and
// extracts the byte order from the "bytes" codec into an Endianness (default
// little).
func v3Codecs(raw any) ([]store.CodecSpec, store.Endianness, error) {
	endian := store.LittleEndian
	list, ok := raw.([]any)
	if !ok {
		return nil, endian, nil
	}
	out := make([]store.CodecSpec, 0, len(list))
	for _, c := range list {
		cm, ok := c.(map[string]any)
		if !ok {
			return nil, endian, fmt.Errorf("malformed codec entry %v", c)
		}
		id, _ := cm["name"].(string)
		cfg, _ := cm["configuration"].(map[string]any)
		if id == "bytes" {
			if e, _ := cfg["endian"].(string); e == "big" {
				endian = store.BigEndian
			}
		}
		out = append(out, store.CodecSpec{ID: id, Config: cfg})
	}
	return out, endian, nil
}

// List implements store.Store for v3 by directory listing.
func (s *v3Store) List(_ context.Context, path string) ([]store.Entry, error) {
	m, ok, err := s.nodeJSON(path)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("zarr: no node at %q", path)
	}
	if nt, _ := m["node_type"].(string); nt != "group" {
		return nil, fmt.Errorf("zarr: %q is not a group", path)
	}

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
		cm, ok, _ := s.nodeJSON(childPath)
		if !ok {
			continue
		}
		switch nt, _ := cm["node_type"].(string); nt {
		case "group":
			out = append(out, store.Entry{Name: e.Name(), Kind: store.KindGroup})
		case "array":
			out = append(out, store.Entry{Name: e.Name(), Kind: store.KindArray})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

// chunkPath returns the absolute filesystem path of the chunk at coord for the
// v3 array at arrayPath, honoring its chunk_key_encoding.
func (s *v3Store) chunkPath(arrayPath string, coord []int64) (string, error) {
	m, ok, err := s.nodeJSON(arrayPath)
	if err != nil {
		return "", err
	}
	if !ok {
		return "", fmt.Errorf("zarr: no array at %q", arrayPath)
	}
	key, err := v3ChunkKey(m["chunk_key_encoding"], coord)
	if err != nil {
		return "", err
	}
	base := filepath.Join(s.root, filepath.FromSlash(strings.Trim(arrayPath, "/")))
	return filepath.Join(base, filepath.FromSlash(key)), nil
}

// v3ChunkKey renders a chunk coordinate as a v3 chunk key per the array's
// chunk_key_encoding. Supported encodings:
//   - "default": "c" prefix then separator-joined coords ("c/0/1" or "c.0.1");
//     a 0-d array is just "c". Default separator is "/".
//   - "v2": separator-joined coords with no prefix ("0.1"); a 0-d array is "0".
//     Default separator is ".".
func v3ChunkKey(raw any, coord []int64) (string, error) {
	enc, _ := raw.(map[string]any)
	name, _ := enc["name"].(string)
	cfg, _ := enc["configuration"].(map[string]any)
	sep, _ := cfg["separator"].(string)

	switch name {
	case "", "default":
		if sep == "" {
			sep = "/"
		}
		if len(coord) == 0 {
			return "c", nil
		}
		parts := make([]string, 0, len(coord)+1)
		parts = append(parts, "c")
		for _, c := range coord {
			parts = append(parts, strconv.FormatInt(c, 10))
		}
		return strings.Join(parts, sep), nil
	case "v2":
		if sep == "" {
			sep = "."
		}
		if len(coord) == 0 {
			return "0", nil
		}
		parts := make([]string, len(coord))
		for i, c := range coord {
			parts[i] = strconv.FormatInt(c, 10)
		}
		return strings.Join(parts, sep), nil
	default:
		return "", fmt.Errorf("zarr: unsupported chunk_key_encoding %q", name)
	}
}
