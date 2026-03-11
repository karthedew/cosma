package dataframe

import "testing"

func TestRechunkDefaults(t *testing.T) {
	s1, err := NewSeries("ids", []int64{1, 2})
	if err != nil {
		t.Fatalf("NewSeries ids: %v", err)
	}
	fn, err := New([]*Series{s1})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	if fn.ShouldRechunk() {
		t.Fatalf("ShouldRechunk = true, want false")
	}
	if err := fn.RechunkMut(); err != nil {
		t.Fatalf("RechunkMut: %v", err)
	}
}
