package compute

import (
	"fmt"

	"github.com/karthedew/cosma/dataframe"
	"github.com/karthedew/cosma/internal/expr"
)

func Project(df *dataframe.DataFrame, projections []expr.Expr) (*dataframe.DataFrame, error) {
	// TODO: evaluate expressions into new columns; support aliasing.
	return nil, fmt.Errorf("compute.Project not implemented")
}
