package expr

// SortKey describes one column of a multi-column sort: which column, ascending
// vs descending, and where nulls land. Build one with By and refine it with the
// fluent Desc / WithNullsFirst methods.
type SortKey struct {
	Column     string
	Descending bool
	NullsFirst bool
}

// By starts an ascending, nulls-last sort key over the named column.
func By(column string) SortKey {
	return SortKey{Column: column}
}

// Desc flips the key to descending order.
func (k SortKey) Desc() SortKey {
	k.Descending = true
	return k
}

// WithNullsFirst orders nulls before non-null values for this key.
func (k SortKey) WithNullsFirst() SortKey {
	k.NullsFirst = true
	return k
}
