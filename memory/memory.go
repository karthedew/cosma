package memory

import "github.com/apache/arrow/go/v18/arrow/memory"

// DefaultAllocator returns the standard Go allocator used by Arrow.
func DefaultAllocator() memory.Allocator {
	return memory.NewGoAllocator()
}
