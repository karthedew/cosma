package compute

import (
	"fmt"

	"github.com/karthedew/cosma/dataframe"
)

type JoinType string

const (
	Inner JoinType = "inner"
	Left  JoinType = "left"
	Right JoinType = "right"
	Outer JoinType = "outer"
)

func Join(left, right *dataframe.DataFrame, on []string, how JoinType) (*dataframe.DataFrame, error) {
	// TODO: hash join on key columns; produce merged schema.
	return nil, fmt.Errorf("compute.Join not implemented")
}
