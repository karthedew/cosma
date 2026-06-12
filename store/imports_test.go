package store

import (
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// TestLayering enforces the Z3 acceptance criterion: store (and its
// subpackages) must not import dataframe or plan. Layer 1 is a dumb mirror of
// the store; query semantics live above it.
func TestLayering(t *testing.T) {
	forbidden := []string{
		"github.com/karthedew/cosma/dataframe",
		"github.com/karthedew/cosma/plan",
	}

	err := filepath.WalkDir(".", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || !strings.HasSuffix(path, ".go") {
			return nil
		}
		fset := token.NewFileSet()
		f, err := parser.ParseFile(fset, path, nil, parser.ImportsOnly)
		if err != nil {
			return err
		}
		for _, imp := range f.Imports {
			ipath, err := strconv.Unquote(imp.Path.Value)
			if err != nil {
				return err
			}
			for _, bad := range forbidden {
				if ipath == bad || strings.HasPrefix(ipath, bad+"/") {
					t.Errorf("%s imports %s: store must not depend on it", path, ipath)
				}
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walking store package: %v", err)
	}
}
