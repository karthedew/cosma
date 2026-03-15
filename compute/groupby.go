package compute

import (
	"fmt"

	"github.com/karthedew/cosma/dataframe"
	"github.com/karthedew/cosma/internal/expr"
)

func GroupBy(df *dataframe.DataFrame, keys []expr.Expr, aggs []expr.Expr) (*dataframe.DataFrame, error) {
	// TODO: build hash groups; aggregate via Arrow kernels where possible.
	return nil, fmt.Errorf("compute.GroupBy not implemented")
}
