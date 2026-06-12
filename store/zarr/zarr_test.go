package zarr_test

import (
	"context"
	"errors"
	"math"
	"reflect"
	"sort"
	"sync/atomic"
	"testing"

	"github.com/karthedew/cosma/schema"
	"github.com/karthedew/cosma/store"
	"github.com/karthedew/cosma/store/conformance"
	"github.com/karthedew/cosma/store/zarr"
)

func mustOpen(t *testing.T, dir string) store.Store {
	t.Helper()
	s, err := zarr.OpenFS("testdata/" + dir)
	if err != nil {
		t.Fatalf("OpenFS(%s): %v", dir, err)
	}
	return s
}

// --- Version sniffing -------------------------------------------------------

func TestOpenFS_NotAStore(t *testing.T) {
	if _, err := zarr.OpenFS("testdata"); err == nil {
		t.Fatal("expected error opening a non-Zarr directory")
	}
	if _, err := zarr.OpenFS("testdata/does-not-exist"); err == nil {
		t.Fatal("expected error opening a missing directory")
	}
}

// --- Conformance suite ------------------------------------------------------

func TestConformance_V2(t *testing.T) {
	conformance.RunConformance(t, conformance.Fixture{
		NewStore:     func() store.Store { return mustOpen(t, "v2_nested_slash") },
		Groups:       []string{"/", "/sub"},
		Arrays:       []string{"/temp", "/sub/counts"},
		PresentChunk: store.ChunkKey{ArrayPath: "/temp", Coord: []int64{0, 0}},
		MissingChunk: store.ChunkKey{ArrayPath: "/temp", Coord: []int64{9, 9}},
	})
}

func TestConformance_V2Consolidated(t *testing.T) {
	conformance.RunConformance(t, conformance.Fixture{
		NewStore:     func() store.Store { return mustOpen(t, "v2_consolidated") },
		Groups:       []string{"/", "/grp"},
		Arrays:       []string{"/a", "/grp/b"},
		PresentChunk: store.ChunkKey{ArrayPath: "/a", Coord: []int64{0}},
		MissingChunk: store.ChunkKey{ArrayPath: "/a", Coord: []int64{99}},
	})
}

func TestConformance_V3(t *testing.T) {
	conformance.RunConformance(t, conformance.Fixture{
		NewStore:     func() store.Store { return mustOpen(t, "v3_nested") },
		Groups:       []string{"/", "/sub"},
		Arrays:       []string{"/temp", "/sub/counts"},
		PresentChunk: store.ChunkKey{ArrayPath: "/temp", Coord: []int64{0, 0}},
		MissingChunk: store.ChunkKey{ArrayPath: "/temp", Coord: []int64{9, 9}},
	})
}

// --- Golden metadata --------------------------------------------------------

func arrayAt(t *testing.T, s store.Store, path string) *store.Array {
	t.Helper()
	obj, err := s.Meta(context.Background(), path)
	if err != nil {
		t.Fatalf("Meta(%s): %v", path, err)
	}
	a, ok := obj.(*store.Array)
	if !ok {
		t.Fatalf("Meta(%s): not an array", path)
	}
	return a
}

func TestGoldenMetadata_V2Nested(t *testing.T) {
	s := mustOpen(t, "v2_nested_slash")
	temp := arrayAt(t, s, "/temp")
	if !reflect.DeepEqual(temp.Shape, []int64{4, 4}) {
		t.Errorf("temp.Shape = %v", temp.Shape)
	}
	if !reflect.DeepEqual(temp.ChunkShape, []int64{2, 2}) {
		t.Errorf("temp.ChunkShape = %v", temp.ChunkShape)
	}
	if temp.DType != schema.Float64 {
		t.Errorf("temp.DType = %v", temp.DType)
	}
	if temp.Endianness != store.LittleEndian {
		t.Errorf("temp.Endianness = %v", temp.Endianness)
	}
	if !reflect.DeepEqual(temp.DimNames, []string{"y", "x"}) {
		t.Errorf("temp.DimNames = %v", temp.DimNames)
	}
	if len(temp.Codecs) != 0 {
		t.Errorf("temp.Codecs = %v, want empty (uncompressed)", temp.Codecs)
	}
	if u, ok := temp.Attrs().String("units"); !ok || u != "kelvin" {
		t.Errorf("temp units attr = %q,%v", u, ok)
	}
}

func TestGoldenMetadata_DTypeSpread(t *testing.T) {
	s := mustOpen(t, "v2_dtypes")
	want := map[string]struct {
		dt     schema.DType
		endian store.Endianness
	}{
		"/i4":    {schema.Int32, store.LittleEndian},
		"/i8":    {schema.Int64, store.LittleEndian},
		"/f4":    {schema.Float32, store.LittleEndian},
		"/f8":    {schema.Float64, store.LittleEndian},
		"/bool":  {schema.Bool, store.LittleEndian},
		"/u1":    {schema.UInt8, store.LittleEndian},
		"/be_f8": {schema.Float64, store.BigEndian},
	}
	for path, w := range want {
		a := arrayAt(t, s, path)
		if a.DType != w.dt {
			t.Errorf("%s DType = %v, want %v", path, a.DType, w.dt)
		}
		if a.Endianness != w.endian {
			t.Errorf("%s Endianness = %v, want %v", path, a.Endianness, w.endian)
		}
	}
}

func TestGoldenMetadata_V3DTypeSpread(t *testing.T) {
	s := mustOpen(t, "v3_dtypes")
	want := map[string]struct {
		dt     schema.DType
		endian store.Endianness
	}{
		"/i4":    {schema.Int32, store.LittleEndian},
		"/i8":    {schema.Int64, store.LittleEndian},
		"/f4":    {schema.Float32, store.LittleEndian},
		"/f8":    {schema.Float64, store.LittleEndian},
		"/bool":  {schema.Bool, store.LittleEndian},
		"/u1":    {schema.UInt8, store.LittleEndian},
		"/be_f8": {schema.Float64, store.BigEndian},
	}
	for path, w := range want {
		a := arrayAt(t, s, path)
		if a.DType != w.dt {
			t.Errorf("%s DType = %v, want %v", path, a.DType, w.dt)
		}
		if a.Endianness != w.endian {
			t.Errorf("%s Endianness = %v, want %v", path, a.Endianness, w.endian)
		}
		// v3 codec list passes through verbatim and includes the bytes codec.
		if len(a.Codecs) == 0 || a.Codecs[0].ID != "bytes" {
			t.Errorf("%s Codecs = %v, want a leading bytes codec", path, a.Codecs)
		}
	}
}

func TestGoldenMetadata_FillNaN(t *testing.T) {
	s := mustOpen(t, "v2_fill_nan")
	a := arrayAt(t, s, "/f")
	f, ok := a.FillValue.(float64)
	if !ok || !math.IsNaN(f) {
		t.Errorf("fill = %v (%T), want NaN", a.FillValue, a.FillValue)
	}
}

func TestGoldenMetadata_V3FillNaN(t *testing.T) {
	s := mustOpen(t, "v3_missing")
	a := arrayAt(t, s, "/sparse")
	f, ok := a.FillValue.(float64)
	if !ok || !math.IsNaN(f) {
		t.Errorf("fill = %v (%T), want NaN", a.FillValue, a.FillValue)
	}
}

func TestGoldenMetadata_Scalar(t *testing.T) {
	for _, dir := range []string{"v2_scalar", "v3_scalar"} {
		s := mustOpen(t, dir)
		a := arrayAt(t, s, "/sc")
		if a.Ndim() != 0 {
			t.Errorf("%s scalar Ndim = %d, want 0", dir, a.Ndim())
		}
		if a.DType != schema.Int32 {
			t.Errorf("%s scalar DType = %v", dir, a.DType)
		}
	}
}

func TestGoldenMetadata_Xarray(t *testing.T) {
	s := mustOpen(t, "v2_xarray")
	// 1-D coord arrays named after their own dim.
	for _, name := range []string{"x", "y"} {
		a := arrayAt(t, s, "/"+name)
		if !reflect.DeepEqual(a.DimNames, []string{name}) {
			t.Errorf("/%s DimNames = %v, want [%s]", name, a.DimNames, name)
		}
	}
	data := arrayAt(t, s, "/data")
	if !reflect.DeepEqual(data.DimNames, []string{"y", "x"}) {
		t.Errorf("/data DimNames = %v", data.DimNames)
	}
}

// --- Consolidated == unconsolidated ----------------------------------------

func TestConsolidatedEqualsUnconsolidated(t *testing.T) {
	ctx := context.Background()
	uncons, err := store.Open(ctx, mustOpen(t, "v2_unconsolidated"))
	if err != nil {
		t.Fatal(err)
	}
	cons, err := store.Open(ctx, mustOpen(t, "v2_consolidated"))
	if err != nil {
		t.Fatal(err)
	}

	var pathsU, pathsC []string
	collect := func(tree *store.Tree) []string {
		var paths []string
		_ = tree.Walk(func(o store.Object) error {
			paths = append(paths, o.Path()+":"+o.Kind().String())
			return nil
		})
		return paths
	}
	pathsU, pathsC = collect(uncons), collect(cons)
	if !reflect.DeepEqual(pathsU, pathsC) {
		t.Fatalf("tree walk differs:\n uncons: %v\n cons:   %v", pathsU, pathsC)
	}

	// Array metadata must match field-for-field.
	for _, au := range uncons.Arrays() {
		ac, ok := cons.Array(au.Path())
		if !ok {
			t.Errorf("array %q missing from consolidated tree", au.Path())
			continue
		}
		if !reflect.DeepEqual(au.Shape, ac.Shape) ||
			!reflect.DeepEqual(au.ChunkShape, ac.ChunkShape) ||
			au.DType != ac.DType || au.Endianness != ac.Endianness {
			t.Errorf("array %q metadata differs:\n u=%+v\n c=%+v", au.Path(), au, ac)
		}
	}
}

// --- dimension_separator variants -------------------------------------------

func TestDimensionSeparator_BothVariants(t *testing.T) {
	ctx := context.Background()
	for _, dir := range []string{"v2_nested_dot", "v2_nested_slash"} {
		s := mustOpen(t, dir)
		// temp is 4x4 with 2x2 chunks: chunk [1,1] holds cells [2..4, 2..4].
		b, err := s.ReadChunk(ctx, store.ChunkKey{ArrayPath: "/temp", Coord: []int64{1, 1}})
		if err != nil {
			t.Fatalf("%s: ReadChunk temp[1,1]: %v", dir, err)
		}
		if len(b) != 4*8 {
			t.Errorf("%s: temp[1,1] len = %d, want 32", dir, len(b))
		}
	}
}

// --- v3 chunk_key_encoding variants -----------------------------------------

func TestV3ChunkKeyEncodings(t *testing.T) {
	ctx := context.Background()
	s := mustOpen(t, "v3_key_encodings")
	for _, arr := range []string{"/default_slash", "/default_dot", "/v2_enc"} {
		b, err := s.ReadChunk(ctx, store.ChunkKey{ArrayPath: arr, Coord: []int64{0}})
		if err != nil {
			t.Fatalf("%s chunk[0]: %v", arr, err)
		}
		if len(b) != 2*4 { // two int32 cells
			t.Errorf("%s chunk[0] len = %d, want 8", arr, len(b))
		}
		// chunk[1] also present
		if _, err := s.ReadChunk(ctx, store.ChunkKey{ArrayPath: arr, Coord: []int64{1}}); err != nil {
			t.Errorf("%s chunk[1]: %v", arr, err)
		}
	}
}

// --- ReadChunk golden bytes -------------------------------------------------

func TestReadChunkGoldenBytes(t *testing.T) {
	ctx := context.Background()
	s := mustOpen(t, "v2_nested_slash")
	// temp is np.arange(16).reshape(4,4) float64 LE. Chunk [0,0] is the 2x2
	// top-left block: cells 0,1,4,5 in row-major chunk order.
	b, err := s.ReadChunk(ctx, store.ChunkKey{ArrayPath: "/temp", Coord: []int64{0, 0}})
	if err != nil {
		t.Fatal(err)
	}
	want := []float64{0, 1, 4, 5}
	if len(b) != len(want)*8 {
		t.Fatalf("len = %d, want %d", len(b), len(want)*8)
	}
	for i, w := range want {
		got := math.Float64frombits(leUint64(b[i*8 : i*8+8]))
		if got != w {
			t.Errorf("cell %d = %v, want %v", i, got, w)
		}
	}

	// scalar value is 42 (int32 LE).
	ss := mustOpen(t, "v2_scalar")
	sb, err := ss.ReadChunk(ctx, store.ChunkKey{ArrayPath: "/sc", Coord: nil})
	if err != nil {
		t.Fatal(err)
	}
	if got := leUint32(sb); got != 42 {
		t.Errorf("scalar = %d, want 42", got)
	}
}

func leUint64(b []byte) uint64 {
	var v uint64
	for i := 7; i >= 0; i-- {
		v = v<<8 | uint64(b[i])
	}
	return v
}

func leUint32(b []byte) uint32 {
	return uint32(b[0]) | uint32(b[1])<<8 | uint32(b[2])<<16 | uint32(b[3])<<24
}

// --- Missing chunk ----------------------------------------------------------

func TestMissingChunk(t *testing.T) {
	ctx := context.Background()
	s := mustOpen(t, "v2_missing")
	// chunk 1 was never written.
	_, err := s.ReadChunk(ctx, store.ChunkKey{ArrayPath: "/sparse", Coord: []int64{1}})
	if !errors.Is(err, store.ErrChunkMissing) {
		t.Errorf("missing chunk err = %v, want ErrChunkMissing", err)
	}
	// chunk 0 and 2 are present.
	for _, c := range []int64{0, 2} {
		if _, err := s.ReadChunk(ctx, store.ChunkKey{ArrayPath: "/sparse", Coord: []int64{c}}); err != nil {
			t.Errorf("chunk %d: %v", c, err)
		}
	}
}

// --- ChunkResolver ----------------------------------------------------------

func TestChunkResolver(t *testing.T) {
	s := mustOpen(t, "v2_nested_slash")
	cr, ok := s.(store.ChunkResolver)
	if !ok {
		t.Fatal("driver does not implement ChunkResolver")
	}
	ref, err := cr.ResolveChunk(store.ChunkKey{ArrayPath: "/temp", Coord: []int64{0, 0}})
	if err != nil {
		t.Fatal(err)
	}
	if ref.URL == "" || ref.Offset != 0 || ref.Length != -1 {
		t.Errorf("ref = %+v, want non-empty URL, offset 0, length -1", ref)
	}
	_, err = cr.ResolveChunk(store.ChunkKey{ArrayPath: "/temp", Coord: []int64{9, 9}})
	if !errors.Is(err, store.ErrChunkMissing) {
		t.Errorf("resolve missing err = %v, want ErrChunkMissing", err)
	}
}

// --- Zero chunk reads during Open -------------------------------------------

// countingStore wraps a store and counts ReadChunk calls.
type countingStore struct {
	store.Store
	reads int64
}

func (c *countingStore) ReadChunk(ctx context.Context, key store.ChunkKey) ([]byte, error) {
	atomic.AddInt64(&c.reads, 1)
	return c.Store.ReadChunk(ctx, key)
}

func TestZeroChunkReadsDuringOpen(t *testing.T) {
	ctx := context.Background()
	for _, dir := range []string{"v2_nested_slash", "v2_consolidated", "v3_nested"} {
		cs := &countingStore{Store: mustOpen(t, dir)}
		tree, err := store.Open(ctx, cs)
		if err != nil {
			t.Fatalf("%s: Open: %v", dir, err)
		}
		// also walk every array's metadata
		for _, a := range tree.Arrays() {
			_ = a.Shape
		}
		if n := atomic.LoadInt64(&cs.reads); n != 0 {
			t.Errorf("%s: %d chunk reads during Open, want 0", dir, n)
		}
	}
}

// --- Unsupported surfaces error loudly --------------------------------------

func TestUnsupportedSurfaces(t *testing.T) {
	cases := []struct {
		dtype string
	}{
		{"<U16"},     // unicode
		{"|V8"},      // void/structured
		{"<c16"},     // complex
		{"<weird99"}, // malformed
	}
	for _, c := range cases {
		if _, err := zarr.ParseV2DTypeForTest(c.dtype); err == nil {
			t.Errorf("dtype %q: expected error, got nil", c.dtype)
		}
	}
	if _, err := zarr.ParseV3DTypeForTest("complex64"); err == nil {
		t.Errorf("v3 complex64: expected error")
	}
	if _, err := zarr.ParseV3DTypeForTest("nonsense"); err == nil {
		t.Errorf("v3 nonsense: expected error")
	}
}

// --- Tree array set ---------------------------------------------------------

func TestTreeArraySets(t *testing.T) {
	ctx := context.Background()
	cases := map[string][]string{
		"v2_nested_slash":  {"/sub/counts", "/temp"},
		"v3_nested":        {"/sub/counts", "/temp"},
		"v2_xarray":        {"/data", "/x", "/y"},
		"v3_key_encodings": {"/default_dot", "/default_slash", "/v2_enc"},
	}
	for dir, want := range cases {
		tree, err := store.Open(ctx, mustOpen(t, dir))
		if err != nil {
			t.Fatalf("%s: %v", dir, err)
		}
		var got []string
		for _, a := range tree.Arrays() {
			got = append(got, a.Path())
		}
		sort.Strings(got)
		sort.Strings(want)
		if !reflect.DeepEqual(got, want) {
			t.Errorf("%s arrays = %v, want %v", dir, got, want)
		}
	}
}
