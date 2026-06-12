package compute

import (
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/karthedew/cosma/expr"
)

// BinaryKernel evaluates a binary op over two same-length arrays of a custom
// Arrow type, returning a newly-allocated result the caller owns.
type BinaryKernel func(op expr.BinaryOp, left, right arrow.Array, mem memory.Allocator) (arrow.Array, error)

// UnaryKernel evaluates a unary op over one array of a custom Arrow type,
// returning a newly-allocated result the caller owns.
type UnaryKernel func(op expr.UnaryOp, input arrow.Array, mem memory.Allocator) (arrow.Array, error)

// kernelRegistry holds custom per-type kernels registered at init time (e.g. by
// the carray package). evalBinary / evalUnary consult it as a fallback after
// their built-in type switches, so a registered type ID can extend the engine
// without modifying compute.
var kernelRegistry = struct {
	mu     sync.RWMutex
	binary map[arrow.Type]BinaryKernel
	unary  map[arrow.Type]UnaryKernel
}{
	binary: make(map[arrow.Type]BinaryKernel),
	unary:  make(map[arrow.Type]UnaryKernel),
}

// RegisterBinaryKernel installs a binary kernel for the given Arrow type ID.
// A later registration for the same type ID replaces the earlier one.
func RegisterBinaryKernel(typeID arrow.Type, k BinaryKernel) {
	kernelRegistry.mu.Lock()
	defer kernelRegistry.mu.Unlock()
	kernelRegistry.binary[typeID] = k
}

// RegisterUnaryKernel installs a unary kernel for the given Arrow type ID.
// A later registration for the same type ID replaces the earlier one.
func RegisterUnaryKernel(typeID arrow.Type, k UnaryKernel) {
	kernelRegistry.mu.Lock()
	defer kernelRegistry.mu.Unlock()
	kernelRegistry.unary[typeID] = k
}

func lookupBinaryKernel(typeID arrow.Type) (BinaryKernel, bool) {
	kernelRegistry.mu.RLock()
	defer kernelRegistry.mu.RUnlock()
	k, ok := kernelRegistry.binary[typeID]
	return k, ok
}

func lookupUnaryKernel(typeID arrow.Type) (UnaryKernel, bool) {
	kernelRegistry.mu.RLock()
	defer kernelRegistry.mu.RUnlock()
	k, ok := kernelRegistry.unary[typeID]
	return k, ok
}
