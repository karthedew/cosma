package schema

import "fmt"

// Schema is a minimal public schema with controlled mutability.
type Schema struct {
	fields []Field
	index  map[string]int
}

func New(fields ...Field) *Schema {
	idx := make(map[string]int, len(fields))
	for i, f := range fields {
		idx[f.Name] = i
	}
	return &Schema{fields: fields, index: idx}
}

func (s *Schema) Fields() []Field {
	// Return a copy to prevent external mutation.
	out := make([]Field, len(s.fields))
	copy(out, s.fields)
	return out
}

func (s *Schema) Len() int { return len(s.fields) }

func (s *Schema) Field(name string) (Field, bool) {
	i, ok := s.index[name]
	if !ok {
		return Field{}, false
	}
	return s.fields[i], true
}

func (s *Schema) FieldIndex(name string) (int, bool) {
	i, ok := s.index[name]
	return i, ok
}

func (s *Schema) String() string {
	return fmt.Sprintf("Schema(fields=%d)", len(s.fields))
}
