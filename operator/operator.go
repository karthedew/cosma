package operator

import "github.com/apache/arrow/go/v18/arrow"

type Operator interface {
	Schema() *arrow.Schema
	// Process consumes the input record and returns a new record.
	// The executor will Release the input record after Process returns.
	// Operators that pass through input must Retain before returning it.
	Process(arrow.Record) (arrow.Record, error)
	Release()
}
