package dataframe

import (
	"strings"
	"testing"

	"github.com/karthedew/cosma/schema"
)

func TestNewDataFrameSuccess(t *testing.T) {
	s1, err := NewSeries("ids", []int64{1, 2, 3})
	if err != nil {
		t.Fatalf("NewSeries ids: %v", err)
	}
	s2, err := NewSeries("names", []string{"a", "b", "c"})
	if err != nil {
		t.Fatalf("NewSeries names: %v", err)
	}

	df, err := New([]*Series{s1, s2})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if df.Height() != 3 {
		t.Fatalf("Height = %d, want 3", df.Height())
	}
	if df.Width() != 2 {
		t.Fatalf("Width = %d, want 2", df.Width())
	}

	col, ok := df.Column("names")
	if !ok {
		t.Fatalf("Column names not found")
	}
	if col.Name() != "names" {
		t.Fatalf("Column name = %q, want names", col.Name())
	}

	fields := df.Schema().Fields()
	if fields[0].Name != "ids" || fields[0].Type != schema.Int64 {
		t.Fatalf("fields[0] = %+v, want ids/int64", fields[0])
	}
	if fields[1].Name != "names" || fields[1].Type != schema.Utf8 {
		t.Fatalf("fields[1] = %+v, want names/utf8", fields[1])
	}
}

func TestNewDataFrameErrors(t *testing.T) {
	if _, err := New([]*Series{nil}); err == nil {
		t.Fatalf("expected error for nil series")
	}

	s1 := &Series{name: "", col: NewChunkedColumn(chunkedFromInt64([]int64{1}))}
	if _, err := New([]*Series{s1}); err == nil {
		t.Fatalf("expected error for empty series name")
	}

	s2, err := NewSeries("dup", []int64{1})
	if err != nil {
		t.Fatalf("NewSeries dup: %v", err)
	}
	s3, err := NewSeries("dup", []int64{2})
	if err != nil {
		t.Fatalf("NewSeries dup: %v", err)
	}
	if _, err := New([]*Series{s2, s3}); err == nil || !strings.Contains(err.Error(), "duplicate series name") {
		t.Fatalf("expected duplicate name error, got %v", err)
	}

	s4, err := NewSeries("a", []int64{1, 2})
	if err != nil {
		t.Fatalf("NewSeries a: %v", err)
	}
	s5, err := NewSeries("b", []string{"x", "y", "z"})
	if err != nil {
		t.Fatalf("NewSeries b: %v", err)
	}
	if _, err := New([]*Series{s4, s5}); err == nil || !strings.Contains(err.Error(), "len=") {
		t.Fatalf("expected length mismatch error, got %v", err)
	}
}
