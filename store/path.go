package store

import "strings"

// baseName returns the final segment of an absolute store path. The root "/"
// has the empty base name.
func baseName(path string) string {
	if path == "/" || path == "" {
		return ""
	}
	path = strings.TrimRight(path, "/")
	if i := strings.LastIndex(path, "/"); i >= 0 {
		return path[i+1:]
	}
	return path
}

// joinPath appends a child segment to a parent path, normalizing slashes so the
// result is always absolute with a single separator. The root parent is "/".
func joinPath(parent, child string) string {
	if parent == "/" || parent == "" {
		return "/" + child
	}
	return strings.TrimRight(parent, "/") + "/" + child
}
