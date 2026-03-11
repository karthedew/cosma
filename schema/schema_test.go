package schema

import "testing"

func TestSchemaFieldsCopy(t *testing.T) {
	fields := []Field{{Name: "a", Type: Int64}, {Name: "b", Type: Utf8}}
	s := New(fields...)

	copyFields := s.Fields()
	copyFields[0].Name = "changed"

	if field, ok := s.Field("a"); !ok || field.Name != "a" {
		t.Fatalf("expected field a to remain unchanged")
	}
}

func TestSchemaLookupAndString(t *testing.T) {
	s := New(Field{Name: "a", Type: Int32}, Field{Name: "b", Type: Bool})

	if _, ok := s.Field("missing"); ok {
		t.Fatalf("expected missing field lookup to fail")
	}

	if got := s.String(); got != "Schema(fields=2)" {
		t.Fatalf("String = %q, want Schema(fields=2)", got)
	}
}
