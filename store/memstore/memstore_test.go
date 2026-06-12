package memstore_test

import (
	"context"
	"testing"

	"github.com/karthedew/cosma/schema"
	"github.com/karthedew/cosma/store"
	"github.com/karthedew/cosma/store/conformance"
	"github.com/karthedew/cosma/store/memstore"
)

// newFixtureStore builds the canonical test hierarchy:
//
//	/
//	├── weather/            (group, attrs)
//	│   ├── temp            (2-D float64 array, chunks (0,0) and (0,1))
//	│   └── time            (1-D int64 array, chunk (0))
//	└── flags               (1-D bool array, no chunks)
func newFixtureStore() *memstore.Store {
	return memstore.New().
		AddGroup("/weather", store.Attrs{"title": "station data"}).
		AddArray("/weather/temp", store.Array{
			Shape:      []int64{4, 6},
			ChunkShape: []int64{4, 3},
			DType:      schema.Float64,
			Endianness: store.LittleEndian,
			Codecs:     []store.CodecSpec{{ID: "gzip", Config: map[string]any{"level": 5}}},
			FillValue:  0.0,
			Order:      store.OrderC,
			DimNames:   []string{"time", "station"},
		}, map[string][]byte{
			"0.0": {1, 2, 3, 4},
			"0.1": {5, 6, 7, 8},
		}).
		AddArray("/weather/time", store.Array{
			Shape:      []int64{4},
			ChunkShape: []int64{4},
			DType:      schema.Int64,
		}, map[string][]byte{
			"0": {0, 1, 2, 3},
		}).
		AddArray("/flags", store.Array{
			Shape:      []int64{8},
			ChunkShape: []int64{8},
			DType:      schema.Bool,
		}, nil)
}

func TestMemstoreConformance(t *testing.T) {
	conformance.RunConformance(t, conformance.Fixture{
		NewStore: func() store.Store { return newFixtureStore() },
		Groups:   []string{"/", "/weather"},
		Arrays:   []string{"/weather/temp", "/weather/time", "/flags"},
		PresentChunk: store.ChunkKey{
			ArrayPath: "/weather/temp", Coord: []int64{0, 1},
		},
		MissingChunk: store.ChunkKey{
			ArrayPath: "/weather/temp", Coord: []int64{1, 0},
		},
	})
}

func TestResolveChunkSyntheticURL(t *testing.T) {
	s := newFixtureStore()
	defer s.Close()

	ref, err := s.ResolveChunk(store.ChunkKey{ArrayPath: "/weather/temp", Coord: []int64{0, 1}})
	if err != nil {
		t.Fatalf("ResolveChunk: %v", err)
	}
	if want := "mem:///weather/temp/0.1"; ref.URL != want {
		t.Errorf("URL = %q, want %q", ref.URL, want)
	}
	if ref.Offset != 0 || ref.Length != 4 {
		t.Errorf("Offset/Length = %d/%d, want 0/4", ref.Offset, ref.Length)
	}
	if len(ref.Codecs) != 1 || ref.Codecs[0].ID != "gzip" {
		t.Errorf("Codecs = %+v, want the array's gzip spec", ref.Codecs)
	}
}

func TestReadChunkReturnsCopy(t *testing.T) {
	s := newFixtureStore()
	defer s.Close()

	key := store.ChunkKey{ArrayPath: "/weather/time", Coord: []int64{0}}
	a, err := s.ReadChunk(context.Background(), key)
	if err != nil {
		t.Fatalf("ReadChunk: %v", err)
	}
	a[0] = 99
	b, err := s.ReadChunk(context.Background(), key)
	if err != nil {
		t.Fatalf("ReadChunk: %v", err)
	}
	if b[0] == 99 {
		t.Error("mutating a returned chunk leaked into the store")
	}
}

func TestSortedChunkKeys(t *testing.T) {
	s := newFixtureStore()
	defer s.Close()

	got := s.SortedChunkKeys("/weather/temp")
	want := []string{"0.0", "0.1"}
	if len(got) != len(want) {
		t.Fatalf("SortedChunkKeys = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("SortedChunkKeys = %v, want %v", got, want)
		}
	}
}
