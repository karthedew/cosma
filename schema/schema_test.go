package schema

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewAndLen(t *testing.T) {
	t.Parallel()

	t.Run("empty", func(t *testing.T) {
		t.Parallel()
		s := New()
		assert.Equal(t, 0, s.Len())
		assert.Empty(t, s.Fields())
	})

	t.Run("multiple", func(t *testing.T) {
		t.Parallel()
		s := New(
			Field{Name: "a", Type: Int64},
			Field{Name: "b", Type: Utf8},
			Field{Name: "c", Type: Bool},
		)
		assert.Equal(t, 3, s.Len())
	})
}

func TestSchemaFieldsCopy(t *testing.T) {
	t.Parallel()
	s := New(Field{Name: "a", Type: Int64}, Field{Name: "b", Type: Utf8})

	copyFields := s.Fields()
	copyFields[0].Name = "changed"

	field, ok := s.Field("a")
	require.True(t, ok, "field a should still be present after mutating the copy")
	assert.Equal(t, "a", field.Name)

	// A fresh Fields() call must reflect the original, unmutated state.
	again := s.Fields()
	assert.Equal(t, "a", again[0].Name)
}

func TestSchemaFieldsPreservesOrder(t *testing.T) {
	t.Parallel()
	s := New(
		Field{Name: "z", Type: Int64},
		Field{Name: "y", Type: Utf8},
		Field{Name: "x", Type: Bool},
	)
	fields := s.Fields()
	require.Len(t, fields, 3)
	assert.Equal(t, "z", fields[0].Name)
	assert.Equal(t, "y", fields[1].Name)
	assert.Equal(t, "x", fields[2].Name)
}

func TestSchemaFieldLookup(t *testing.T) {
	t.Parallel()
	s := New(
		Field{Name: "a", Type: Int32, Nullable: true},
		Field{Name: "b", Type: Bool},
	)

	t.Run("hit", func(t *testing.T) {
		t.Parallel()
		f, ok := s.Field("a")
		require.True(t, ok)
		assert.Equal(t, Int32, f.Type)
		assert.True(t, f.Nullable)
	})

	t.Run("miss", func(t *testing.T) {
		t.Parallel()
		f, ok := s.Field("missing")
		assert.False(t, ok)
		assert.Equal(t, Field{}, f)
	})
}

func TestSchemaFieldIndex(t *testing.T) {
	t.Parallel()
	s := New(
		Field{Name: "a", Type: Int64},
		Field{Name: "b", Type: Utf8},
		Field{Name: "c", Type: Bool},
	)

	cases := []struct {
		name    string
		wantIdx int
		wantOK  bool
	}{
		{"a", 0, true},
		{"b", 1, true},
		{"c", 2, true},
		{"missing", 0, false},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			i, ok := s.FieldIndex(c.name)
			assert.Equal(t, c.wantOK, ok)
			assert.Equal(t, c.wantIdx, i)
		})
	}
}

func TestSchemaString(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name   string
		schema *Schema
		want   string
	}{
		{"empty", New(), "Schema(fields=0)"},
		{"one", New(Field{Name: "a", Type: Int64}), "Schema(fields=1)"},
		{"two", New(Field{Name: "a", Type: Int32}, Field{Name: "b", Type: Bool}), "Schema(fields=2)"},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, c.want, c.schema.String())
		})
	}
}
