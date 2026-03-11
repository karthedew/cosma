package memory

import "testing"

func TestDefaultAllocator(t *testing.T) {
	alloc := DefaultAllocator()
	if alloc == nil {
		t.Fatalf("expected allocator")
	}
	buf := alloc.Allocate(8)
	if len(buf) != 8 {
		t.Fatalf("Allocate len = %d, want 8", len(buf))
	}
	alloc.Free(buf)
}
