package expr_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/karthedew/cosma/expr"
)

func TestColAndLitConstruct(t *testing.T) {
	t.Parallel()

	col := expr.Col("age")
	require.NotNil(t, col.Node)

	lit := expr.Lit(int64(30))
	require.NotNil(t, lit.Node)
}
