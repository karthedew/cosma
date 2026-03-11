package dataframe

import (
	"strings"
	"testing"

	"github.com/apache/arrow/go/v18/arrow"
)

func TestNewSeriesFromArrayErrors(t *testing.T) {
	if _, err := NewSeriesFromArray("", nil); err == nil {
		t.Fatalf("expected error for empty name")
	}
	if _, err := NewSeriesFromArray("values", nil); err == nil {
		t.Fatalf("expected error for nil array")
	}
}

func TestNewSeriesErrors(t *testing.T) {
	if _, err := NewSeries("", []int64{1}); err == nil {
		t.Fatalf("expected error for empty name")
	}
	if _, err := NewSeries("bad", []complex64{1}); err == nil || !strings.Contains(err.Error(), "unsupported series values type") {
		t.Fatalf("expected unsupported type error, got %v", err)
	}
	if _, err := NewSeriesNull("nulls", -1); err == nil {
		t.Fatalf("expected error for negative length")
	}
}

func TestNewSeriesData(t *testing.T) {
	s, err := NewSeries("ids", []int64{1, 2, 3})
	if err != nil {
		t.Fatalf("NewSeries ids: %v", err)
	}
	if s.Len() != 3 {
		t.Fatalf("Len = %d, want 3", s.Len())
	}
	if s.DataType() == nil || s.DataType().ID() != arrow.INT64 {
		t.Fatalf("DataType = %v, want INT64", s.DataType())
	}

	s2, err := NewSeries("names", []string{"a", "b"})
	if err != nil {
		t.Fatalf("NewSeries names: %v", err)
	}
	if s2.Len() != 2 {
		t.Fatalf("Len = %d, want 2", s2.Len())
	}
}
