package main

import (
	"fmt"
	// "log"

	"github.com/apache/arrow/go/v14/arrow"
	// "github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/memory"
)

type DataFrame struct {
	Schema *arrow.Schema
	Columns []*Column
}

type Column struct {
	Name string
	DType arrow.DataType
	Chunks *arrow.Chunked		// immutable chunks
	Appender ColumnAppender		// builder + flush logic
}

type ColumnAppender interface {
	AppendBatch(any) error			// e.g., []int64, []string, +validity
	Flush() (arrow.Array, bool)		// returns a chunk if ready
	Finish() (arrow.Array, bool)	// flush remainder at end
}

func main() {
	fmt.Println("Hello World!")

	pool := memory.NewGoAllocator()

	fmt.Println(pool)

}
