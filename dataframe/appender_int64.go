package dataframe

import (
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

const defaultChunkSize = 1024

type Int64Appender struct {
	chunkSize  int
	allocator  memory.Allocator
	builder    *array.Int64Builder
	pendingLen int
}

func NewInt64Appender(chunkSize int) *Int64Appender {
	if chunkSize <= 0 {
		chunkSize = defaultChunkSize
	}
	alloc := memory.NewGoAllocator()
	return &Int64Appender{
		chunkSize: chunkSize,
		allocator: alloc,
		builder:   array.NewInt64Builder(alloc),
	}
}

func (a *Int64Appender) Append(value any) error {
	v, ok := value.(int64)
	if !ok {
		return fmt.Errorf("int64 appender got %T", value)
	}
	a.builder.Append(v)
	a.pendingLen++
	return nil
}

func (a *Int64Appender) AppendNull() {
	a.builder.AppendNull()
	a.pendingLen++
}

func (a *Int64Appender) ShouldFlush() bool {
	return a.pendingLen >= a.chunkSize
}

func (a *Int64Appender) PendingLen() int {
	return a.pendingLen
}

func (a *Int64Appender) Flush() (arrow.Array, bool) {
	if a.pendingLen == 0 {
		return nil, false
	}

	arr := a.builder.NewArray()
	a.builder.Release()
	a.builder = array.NewInt64Builder(a.allocator)
	a.pendingLen = 0
	return arr, true
}

func (a *Int64Appender) Release() {
	if a.builder != nil {
		a.builder.Release()
		a.builder = nil
	}
}
