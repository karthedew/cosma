package zarr

import (
	"context"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/karthedew/cosma/schema"
	"github.com/karthedew/cosma/store"
)

func TestParseV2DTypeBranches(t *testing.T) {
	for _, tc := range []struct {
		in     string
		dt     schema.DType
		endian store.Endianness
		width  int
	}{
		{"<i1", schema.Int8, store.LittleEndian, 1},
		{"<i2", schema.Int16, store.LittleEndian, 2},
		{">i2", schema.Int16, store.BigEndian, 2},
		{"=i4", schema.Int32, store.LittleEndian, 4},
		{"<i8", schema.Int64, store.LittleEndian, 8},
		{"|u1", schema.UInt8, store.LittleEndian, 1},
		{"<u2", schema.UInt16, store.LittleEndian, 2},
		{"<u4", schema.UInt32, store.LittleEndian, 4},
		{"<u8", schema.UInt64, store.LittleEndian, 8},
		{"<f2", schema.Float16, store.LittleEndian, 2},
		{"<f4", schema.Float32, store.LittleEndian, 4},
		{"<f8", schema.Float64, store.LittleEndian, 8},
		{"|b1", schema.Bool, store.LittleEndian, 1},
		{"|S3", schema.FixedSizeBinary, store.LittleEndian, 3},
		{"<M8[ns]", schema.Timestamp, store.LittleEndian, 0},
		{"<m8[ns]", schema.Duration, store.LittleEndian, 0},
	} {
		t.Run(tc.in, func(t *testing.T) {
			got, err := parseV2DType(tc.in)
			require.NoError(t, err)
			require.Equal(t, tc.dt, got.DType)
			require.Equal(t, tc.endian, got.Endianness)
			require.Equal(t, tc.width, got.ByteWidth)
		})
	}

	for _, in := range []string{"", "i4", "<", "<iX", "<i3", "|Sbad", "<U16", "|V8", "<c16", "<x4", "(i4,i4)"} {
		t.Run("err_"+in, func(t *testing.T) {
			_, err := parseV2DType(in)
			require.Error(t, err)
		})
	}
	_, ok := numericDType('x', 1)
	require.False(t, ok)
}

func TestV2MetadataBranches(t *testing.T) {
	s := &v2Store{}
	a, err := s.parseArray("/a", map[string]any{
		"shape":               []any{float64(2), float64(3)},
		"chunks":              []any{float64(1), float64(3)},
		"dtype":               "<i4",
		"order":               "F",
		"fill_value":          float64(0),
		"compressor":          map[string]any{"id": "gzip", "level": float64(1)},
		"dimension_separator": "/",
	}, store.Attrs{"_ARRAY_DIMENSIONS": []any{"y", "x"}})
	require.NoError(t, err)
	require.Equal(t, store.OrderF, a.Order)
	require.Equal(t, []string{"y", "x"}, a.DimNames)
	require.Equal(t, []store.CodecSpec{{ID: "gzip", Config: map[string]any{"level": float64(1)}}}, a.Codecs)

	_, err = s.parseArray("/bad", map[string]any{"shape": "bad", "chunks": []any{}, "dtype": "<i4"}, nil)
	require.ErrorContains(t, err, "shape")
	_, err = s.parseArray("/bad", map[string]any{"shape": []any{}, "chunks": "bad", "dtype": "<i4"}, nil)
	require.ErrorContains(t, err, "chunks")
	_, err = s.parseArray("/bad", map[string]any{"shape": []any{}, "chunks": []any{}, "dtype": []any{}}, nil)
	require.ErrorContains(t, err, "dtype")
	_, err = s.parseArray("/bad", map[string]any{"shape": []any{}, "chunks": []any{}, "dtype": "<i4", "filters": []any{"x"}}, nil)
	require.ErrorContains(t, err, "filters")

	root := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(root, ".zgroup"), []byte(`{"zarr_format":2}`), 0o600))
	v2, err := openV2(root)
	require.NoError(t, err)
	_, err = v2.Meta(context.Background(), "/missing")
	require.ErrorContains(t, err, "no group or array")
	_, err = v2.List(context.Background(), "/missing")
	require.ErrorContains(t, err, "no group")
	_, err = v2.dimSeparator("/missing")
	require.ErrorContains(t, err, "no array")
	require.NoError(t, os.WriteFile(filepath.Join(root, ".zmetadata"), []byte(`{bad`), 0o600))
	_, err = openV2(root)
	require.ErrorContains(t, err, "parse .zmetadata")
}

func TestV3MetadataBranches(t *testing.T) {
	s := &v3Store{}
	_, err := s.parseArray("/bad", map[string]any{"shape": "bad"}, nil)
	require.ErrorContains(t, err, "shape")
	_, err = s.parseArray("/bad", map[string]any{"shape": []any{}, "chunk_grid": "bad", "data_type": "int32"}, nil)
	require.ErrorContains(t, err, "chunk_grid")
	_, err = s.parseArray("/bad", map[string]any{"shape": []any{}, "chunk_grid": regularGrid(), "data_type": 1}, nil)
	require.ErrorContains(t, err, "data_type")
	_, err = s.parseArray("/bad", map[string]any{"shape": []any{}, "chunk_grid": regularGrid(), "data_type": "complex64"}, nil)
	require.ErrorContains(t, err, "complex")
	_, err = s.parseArray("/bad", map[string]any{"shape": []any{}, "chunk_grid": regularGrid(), "data_type": "int32", "codecs": []any{"bad"}}, nil)
	require.ErrorContains(t, err, "malformed codec")
	a, err := s.parseArray("/ok", map[string]any{
		"shape":              []any{float64(2)},
		"chunk_grid":         regularGrid(),
		"data_type":          "int32",
		"codecs":             []any{map[string]any{"name": "bytes", "configuration": map[string]any{"endian": "big"}}},
		"dimension_names":    []any{"x"},
		"chunk_key_encoding": map[string]any{"name": "v2"},
	}, store.Attrs{"a": "b"})
	require.NoError(t, err)
	require.Equal(t, store.BigEndian, a.Endianness)
	require.Equal(t, []string{"x"}, a.DimNames)
	require.Equal(t, store.Attrs{"a": "b"}, a.Attrs())

	_, err = v3ChunkShape(map[string]any{"name": "other"})
	require.ErrorContains(t, err, "unsupported chunk_grid")
	codecs, endian, err := v3Codecs("not-list")
	require.NoError(t, err)
	require.Nil(t, codecs)
	require.Equal(t, store.LittleEndian, endian)
	key, err := v3ChunkKey(nil, nil)
	require.NoError(t, err)
	require.Equal(t, "c", key)
	key, err = v3ChunkKey(map[string]any{"name": "default", "configuration": map[string]any{"separator": "."}}, []int64{1, 2})
	require.NoError(t, err)
	require.Equal(t, "c.1.2", key)
	key, err = v3ChunkKey(map[string]any{"name": "v2"}, nil)
	require.NoError(t, err)
	require.Equal(t, "0", key)
	_, err = v3ChunkKey(map[string]any{"name": "bad"}, []int64{1})
	require.ErrorContains(t, err, "unsupported")

	root := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(root, "zarr.json"), []byte(`{"node_type":"bad"}`), 0o600))
	v3 := openV3(root)
	_, err = v3.Meta(context.Background(), "/")
	require.ErrorContains(t, err, "unknown node_type")
	_, err = v3.List(context.Background(), "/")
	require.ErrorContains(t, err, "not a group")
	_, err = v3.chunkPath("/missing", []int64{0})
	require.ErrorContains(t, err, "no array")
	require.NoError(t, os.WriteFile(filepath.Join(root, "zarr.json"), []byte(`{bad`), 0o600))
	_, _, err = v3.nodeJSON("/")
	require.ErrorContains(t, err, "parse")
}

func regularGrid() map[string]any {
	return map[string]any{"name": "regular", "configuration": map[string]any{"chunk_shape": []any{float64(1)}}}
}

func TestOpenFSAndJSONIntSliceBranches(t *testing.T) {
	root := t.TempDir()
	file := filepath.Join(root, "file")
	require.NoError(t, os.WriteFile(file, []byte("x"), 0o600))
	_, err := OpenFS(file)
	require.ErrorContains(t, err, "not a directory")
	require.NoError(t, os.WriteFile(filepath.Join(root, ".zgroup"), []byte(`{}`), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(root, "zarr.json"), []byte(`{}`), 0o600))
	_, err = OpenFS(root)
	require.ErrorContains(t, err, "ambiguous")

	got, err := jsonIntSlice(nil)
	require.NoError(t, err)
	require.Empty(t, got)
	_, err = jsonIntSlice("bad")
	require.ErrorContains(t, err, "expected JSON array")
	_, err = jsonIntSlice([]any{"bad"})
	require.ErrorContains(t, err, "expected number")
}

func TestParseV3DTypeBranches(t *testing.T) {
	for _, tc := range []struct {
		in string
		dt schema.DType
	}{
		{"bool", schema.Bool}, {"int8", schema.Int8}, {"int16", schema.Int16}, {"int32", schema.Int32}, {"int64", schema.Int64},
		{"uint8", schema.UInt8}, {"uint16", schema.UInt16}, {"uint32", schema.UInt32}, {"uint64", schema.UInt64},
		{"float16", schema.Float16}, {"float32", schema.Float32}, {"float64", schema.Float64},
	} {
		dt, err := parseV3DType(tc.in)
		require.NoError(t, err)
		require.Equal(t, tc.dt, dt)
	}
	for _, in := range []string{"complex64", "complex128", "string", "bytes", "unknown"} {
		_, err := parseV3DType(in)
		require.Error(t, err)
	}
}

func TestFillValueBranches(t *testing.T) {
	for _, s := range []string{"NaN", "Infinity", "-Infinity", "unknown"} {
		got, err := parseV2FillValue(s, dtypeInfo{DType: schema.Float64})
		require.NoError(t, err)
		f := got.(float64)
		switch s {
		case "Infinity":
			require.True(t, math.IsInf(f, 1))
		case "-Infinity":
			require.True(t, math.IsInf(f, -1))
		default:
			require.True(t, math.IsNaN(f))
		}
	}
	got, err := parseV2FillValue("AQID", dtypeInfo{DType: schema.FixedSizeBinary})
	require.NoError(t, err)
	require.True(t, reflect.DeepEqual([]byte{1, 2, 3}, got))
	_, err = parseV2FillValue("not-base64", dtypeInfo{DType: schema.FixedSizeBinary})
	require.Error(t, err)
	got, err = parseV2FillValue([]byte{1}, dtypeInfo{DType: schema.FixedSizeBinary})
	require.NoError(t, err)
	require.Equal(t, []byte{1}, got)
	got, err = parseV2FillValue(true, dtypeInfo{DType: schema.Bool})
	require.NoError(t, err)
	require.Equal(t, true, got)
	got, err = parseV2FillValue(float64(7), dtypeInfo{DType: schema.Int32})
	require.NoError(t, err)
	require.Equal(t, float64(7), got)
	got, err = parseV2FillValue(nil, dtypeInfo{DType: schema.Int32})
	require.NoError(t, err)
	require.Nil(t, got)

	got, err = parseV3FillValue("Infinity", schema.Float32)
	require.NoError(t, err)
	require.True(t, math.IsInf(got.(float64), 1))
	got, err = parseV3FillValue(float64(1), schema.Float32)
	require.NoError(t, err)
	require.Equal(t, float64(1), got)
	got, err = parseV3FillValue([]any{float64(1), float64(255), "bad"}, schema.FixedSizeBinary)
	require.NoError(t, err)
	require.Equal(t, []byte{1, 255, 0}, got)
	got, err = parseV3FillValue("raw", schema.FixedSizeBinary)
	require.NoError(t, err)
	require.Equal(t, "raw", got)
	got, err = parseV3FillValue(nil, schema.Int64)
	require.NoError(t, err)
	require.Nil(t, got)
	got, err = parseV3FillValue(true, schema.Bool)
	require.NoError(t, err)
	require.Equal(t, true, got)
}
