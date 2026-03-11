package compute

import (
	"fmt"

	"github.com/karthedew/cosma/dataframe"
	"github.com/karthedew/cosma/expr"
)

func Filter(df *dataframe.DataFrame, predicate expr.Expr) (*dataframe.DataFrame, error) {
	// TODO: compile expr -> physical predicate; apply to columns via Arrow kernels.
	return nil, fmt.Errorf("compute.Filter not implemented")
}
