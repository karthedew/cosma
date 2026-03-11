package dataframe

import (
	"strings"
	"testing"
)

func TestDataFrameStringBasic(t *testing.T) {
	s1, err := NewSeries("ids", []int32{1, 2, 3})
	if err != nil {
		t.Fatalf("NewSeries ids: %v", err)
	}
	s2, err := NewSeries("names", []string{"a", "b", "c"})
	if err != nil {
		t.Fatalf("NewSeries names: %v", err)
	}
	fn, err := New([]*Series{s1, s2})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	output := fn.String()
	checks := []string{"shape: (3, 2)", "ids", "names", "i32", "str"}
	for _, check := range checks {
		if !strings.Contains(output, check) {
			t.Fatalf("expected output to contain %q", check)
		}
	}
}

func TestDataFrameStringEllipsis(t *testing.T) {
	values := make([]int32, 25)
	for i := range values {
		values[i] = int32(i)
	}
	s1, err := NewSeries("values", values)
	if err != nil {
		t.Fatalf("NewSeries values: %v", err)
	}
	fn, err := New([]*Series{s1})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	output := fn.String()
	if !strings.Contains(output, "…") {
		t.Fatalf("expected ellipsis in output")
	}
}
