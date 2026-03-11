package schema

import "github.com/apache/arrow/go/v18/arrow"

type Field struct {
	Name      string
	Type      DType
	Nullable  bool
	ArrowType arrow.DataType
}
