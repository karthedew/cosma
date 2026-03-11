package dataframe

// ShouldRechunk is a heuristic that decides if we should coalesce chunks.
// TODO: implement based on chunk counts, sizes, and operator needs.
func (df *DataFrame) ShouldRechunk() bool {
	return false
}

// RechunkMut coalesces chunked columns to fewer/larger chunks.
// TODO: implement using Arrow concat kernels (or manual buffer concat).
func (df *DataFrame) RechunkMut() error {
	return nil
}
