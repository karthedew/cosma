package dataframe

import (
	"fmt"

	"github.com/karthedew/cosma/schema"
)

// Select returns a new DataFrame containing only the named columns, in the
// given order. Columns share Arrow buffers with the receiver (zero-copy); the
// receiver is unchanged. It is an error to name an unknown or duplicate column.
func (df *DataFrame) Select(names ...string) (*DataFrame, error) {
	fields := df.schema.Fields()
	cols := make([]Series, len(names))
	outFields := make([]schema.Field, len(names))
	seen := make(map[string]struct{}, len(names))

	for i, name := range names {
		if _, dup := seen[name]; dup {
			return nil, fmt.Errorf("select: duplicate column %q", name)
		}
		seen[name] = struct{}{}

		idx := df.columnIndex(name)
		if idx < 0 {
			return nil, fmt.Errorf("select: column %q not found", name)
		}
		cols[i] = df.cols[idx]
		outFields[i] = fields[idx]
	}

	return NewDataFrame(schema.New(outFields...), cols)
}

// Drop returns a new DataFrame without the named columns, preserving the order
// of the remaining columns. It is an error to drop an unknown column.
func (df *DataFrame) Drop(names ...string) (*DataFrame, error) {
	drop := make(map[string]struct{}, len(names))
	for _, name := range names {
		if df.columnIndex(name) < 0 {
			return nil, fmt.Errorf("drop: column %q not found", name)
		}
		drop[name] = struct{}{}
	}

	keep := make([]string, 0, len(df.cols))
	for i := range df.cols {
		name := df.cols[i].Name()
		if _, dropped := drop[name]; !dropped {
			keep = append(keep, name)
		}
	}
	return df.Select(keep...)
}

// Rename returns a new DataFrame with the column named old renamed to new,
// keeping column order and sharing buffers. It is an error if old is absent, if
// new is empty, or if new collides with another existing column.
func (df *DataFrame) Rename(old, new string) (*DataFrame, error) {
	idx := df.columnIndex(old)
	if idx < 0 {
		return nil, fmt.Errorf("rename: column %q not found", old)
	}
	if new == "" {
		return nil, fmt.Errorf("rename: new name is empty")
	}
	if old != new && df.columnIndex(new) >= 0 {
		return nil, fmt.Errorf("rename: column %q already exists", new)
	}

	outFields := make([]schema.Field, len(df.cols))
	copy(outFields, df.schema.Fields())
	outFields[idx].Name = new

	cols := make([]Series, len(df.cols))
	copy(cols, df.cols)
	cols[idx] = *NewSeriesFromChunked(new, df.cols[idx].Chunked())

	return NewDataFrame(schema.New(outFields...), cols)
}

// columnIndex returns the position of the named column, or -1 if absent.
func (df *DataFrame) columnIndex(name string) int {
	for i := range df.cols {
		if df.cols[i].Name() == name {
			return i
		}
	}
	return -1
}
