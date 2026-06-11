package dataframe

import (
	"fmt"

	"github.com/karthedew/cosma/internal/compute"
)

// reduce folds the named column into its aggregates, or errors if absent.
func (df *DataFrame) reduce(name string) (compute.Aggregates, error) {
	idx := df.columnIndex(name)
	if idx < 0 {
		return compute.Aggregates{}, fmt.Errorf("aggregate: column %q not found", name)
	}
	return compute.Reduce(df.cols[idx].Chunked())
}

// Sum returns the sum of the named column's non-null values, boxed in the
// column's element type. It is nil when the column is empty or all-null.
func (df *DataFrame) Sum(name string) (any, error) {
	ag, err := df.reduce(name)
	if err != nil {
		return nil, err
	}
	return ag.Sum, nil
}

// Count returns the number of non-null values in the named column.
func (df *DataFrame) Count(name string) (int64, error) {
	ag, err := df.reduce(name)
	if err != nil {
		return 0, err
	}
	return ag.Count, nil
}

// Mean returns the arithmetic mean of the named column's non-null values. It is
// an error to take the mean of an empty or all-null column.
func (df *DataFrame) Mean(name string) (float64, error) {
	ag, err := df.reduce(name)
	if err != nil {
		return 0, err
	}
	if ag.Count == 0 {
		return 0, fmt.Errorf("mean: column %q has no non-null values", name)
	}
	return ag.Mean, nil
}

// Min returns the minimum non-null value of the named column, boxed in the
// column's element type. It is nil when the column is empty or all-null.
func (df *DataFrame) Min(name string) (any, error) {
	ag, err := df.reduce(name)
	if err != nil {
		return nil, err
	}
	return ag.Min, nil
}

// Max returns the maximum non-null value of the named column, boxed in the
// column's element type. It is nil when the column is empty or all-null.
func (df *DataFrame) Max(name string) (any, error) {
	ag, err := df.reduce(name)
	if err != nil {
		return nil, err
	}
	return ag.Max, nil
}
