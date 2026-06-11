package expr_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/karthedew/cosma/expr"
)

func TestSortKeyWithNullsFirst(t *testing.T) {
	t.Parallel()

	sk := expr.By("age").WithNullsFirst()
	require.Equal(t, "age", sk.Column)
	require.False(t, sk.Descending)
	require.True(t, sk.NullsFirst)
}
