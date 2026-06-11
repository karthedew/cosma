package schema

import (
	"testing"

	"github.com/apache/arrow/go/v18/arrow"
)

func TestDTypeFromArrow(t *testing.T) {
	cases := []struct {
		dt   arrow.DataType
		want DType
	}{
		{arrow.PrimitiveTypes.Int64, Int64},
		{arrow.PrimitiveTypes.Float64, Float64},
		{arrow.BinaryTypes.String, Utf8},
		{arrow.FixedWidthTypes.Boolean, Bool},
	}
	for _, c := range cases {
		got, err := DTypeFromArrow(c.dt)
		if err != nil {
			t.Fatalf("DTypeFromArrow(%s): %v", c.dt, err)
		}
		if got != c.want {
			t.Fatalf("DTypeFromArrow(%s) = %q, want %q", c.dt, got, c.want)
		}
	}

	if _, err := DTypeFromArrow(nil); err == nil {
		t.Fatalf("expected error for nil type")
	}
}

func TestFromAndToArrowRoundTrip(t *testing.T) {
	arrSchema := arrow.NewSchema([]arrow.Field{
		{Name: "a", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "b", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)

	s, err := FromArrow(arrSchema)
	if err != nil {
		t.Fatalf("FromArrow: %v", err)
	}
	if s.Len() != 2 {
		t.Fatalf("Len = %d, want 2", s.Len())
	}
	fa, _ := s.Field("a")
	if fa.Type != Int64 || fa.ArrowType == nil {
		t.Fatalf("field a = %+v, want Int64 with ArrowType", fa)
	}
	fb, _ := s.Field("b")
	if fb.Type != Utf8 || !fb.Nullable {
		t.Fatalf("field b = %+v, want nullable Utf8", fb)
	}

	back, err := ToArrow(s)
	if err != nil {
		t.Fatalf("ToArrow: %v", err)
	}
	if !back.Equal(arrSchema) {
		t.Fatalf("round trip mismatch:\n got %s\nwant %s", back, arrSchema)
	}

	if _, err := FromArrow(nil); err == nil {
		t.Fatalf("expected error for nil arrow schema")
	}
	if _, err := ToArrow(nil); err == nil {
		t.Fatalf("expected error for nil schema")
	}
}

func TestFieldFromArrow(t *testing.T) {
	f, err := FieldFromArrow("x", arrow.PrimitiveTypes.Int32)
	if err != nil {
		t.Fatalf("FieldFromArrow: %v", err)
	}
	if f.Name != "x" || f.Type != Int32 || f.ArrowType == nil {
		t.Fatalf("field = %+v, want x Int32 with ArrowType", f)
	}
}
