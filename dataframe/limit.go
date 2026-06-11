package dataframe

// Limit returns a new DataFrame with at most the first n rows, in row order
// across chunks. It is the eager-operation spelling of Head: n is clamped to
// [0, NumRows] and the result shares Arrow buffers with the receiver.
func (df *DataFrame) Limit(n int) *DataFrame {
	return df.Head(n)
}
