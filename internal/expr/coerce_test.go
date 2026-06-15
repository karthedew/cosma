package expr

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/stretchr/testify/require"

	pubexpr "github.com/karthedew/cosma/expr"
	"github.com/karthedew/cosma/schema"
)

func coercionSchema() *schema.Schema {
	return schema.New(
		schema.Field{Name: "i8", ArrowType: arrow.PrimitiveTypes.Int8},
		schema.Field{Name: "i16", ArrowType: arrow.PrimitiveTypes.Int16},
		schema.Field{Name: "i32", ArrowType: arrow.PrimitiveTypes.Int32},
		schema.Field{Name: "i64", ArrowType: arrow.PrimitiveTypes.Int64},
		schema.Field{Name: "u8", ArrowType: arrow.PrimitiveTypes.Uint8},
		schema.Field{Name: "u16", ArrowType: arrow.PrimitiveTypes.Uint16},
		schema.Field{Name: "u32", ArrowType: arrow.PrimitiveTypes.Uint32},
		schema.Field{Name: "u64", ArrowType: arrow.PrimitiveTypes.Uint64},
		schema.Field{Name: "f32", ArrowType: arrow.PrimitiveTypes.Float32},
		schema.Field{Name: "f64", ArrowType: arrow.PrimitiveTypes.Float64},
		schema.Field{Name: "name", ArrowType: arrow.BinaryTypes.String},
	)
}

func TestPromoteLiterals(t *testing.T) {
	t.Parallel()

	s := coercionSchema()
	for _, tc := range []struct {
		name      string
		expr      pubexpr.Expr
		wantLeft  any
		wantRight any
		wantType  arrow.Type
	}{
		{"right literal", pubexpr.Col("i32").Gt(pubexpr.Lit(2)), nil, int32(2), arrow.INT32},
		{"left literal", pubexpr.Lit(2).Lt(pubexpr.Col("f32")), float32(2), nil, arrow.FLOAT32},
	} {
		t.Run(tc.name, func(t *testing.T) {
			out, err := PromoteLiterals(tc.expr, s)
			require.NoError(t, err)
			bn := out.Node.(pubexpr.BinaryNode)
			if tc.wantLeft != nil {
				lit := bn.Left.(pubexpr.LiteralNode)
				require.Equal(t, tc.wantLeft, lit.Value)
				require.Equal(t, tc.wantType, lit.Type.ID())
			}
			if tc.wantRight != nil {
				lit := bn.Right.(pubexpr.LiteralNode)
				require.Equal(t, tc.wantRight, lit.Value)
				require.Equal(t, tc.wantType, lit.Type.ID())
			}
		})
	}

	nilExpr := pubexpr.Expr{}
	out, err := PromoteLiterals(nilExpr, s)
	require.NoError(t, err)
	require.Equal(t, nilExpr, out)

	litVsLit := pubexpr.Lit(1).Eq(pubexpr.Lit(2))
	out, err = PromoteLiterals(litVsLit, s)
	require.NoError(t, err)
	require.Equal(t, litVsLit.String(), out.String())

	colVsCol := pubexpr.Col("i32").Eq(pubexpr.Col("i64"))
	out, err = PromoteLiterals(colVsCol, s)
	require.NoError(t, err)
	require.Equal(t, colVsCol.String(), out.String())

	missingColumn := pubexpr.Col("missing").Eq(pubexpr.Lit(2))
	out, err = PromoteLiterals(missingColumn, s)
	require.NoError(t, err)
	require.Equal(t, missingColumn.String(), out.String())

	stringColumn := pubexpr.Col("name").Eq(pubexpr.Lit(2))
	out, err = PromoteLiterals(stringColumn, s)
	require.NoError(t, err)
	require.Equal(t, stringColumn.String(), out.String())

	_, err = PromoteLiterals(pubexpr.Col("u8").Eq(pubexpr.Lit(-1)), s)
	require.ErrorContains(t, err, "negative literal cannot be unsigned")
}

func TestPromoteLiteralToAllNumericTargets(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name  string
		lit   pubexpr.LiteralNode
		type_ arrow.DataType
		want  any
		id    arrow.Type
	}{
		{"int8", pubexpr.LiteralNode{Value: int64(1)}, arrow.PrimitiveTypes.Int8, int8(1), arrow.INT8},
		{"int16", pubexpr.LiteralNode{Value: int64(1)}, arrow.PrimitiveTypes.Int16, int16(1), arrow.INT16},
		{"int32", pubexpr.LiteralNode{Value: int64(1)}, arrow.PrimitiveTypes.Int32, int32(1), arrow.INT32},
		{"int64", pubexpr.LiteralNode{Value: int(1)}, arrow.PrimitiveTypes.Int64, int64(1), arrow.INT64},
		{"uint8", pubexpr.LiteralNode{Value: uint64(1)}, arrow.PrimitiveTypes.Uint8, uint8(1), arrow.UINT8},
		{"uint16", pubexpr.LiteralNode{Value: uint64(1)}, arrow.PrimitiveTypes.Uint16, uint16(1), arrow.UINT16},
		{"uint32", pubexpr.LiteralNode{Value: uint64(1)}, arrow.PrimitiveTypes.Uint32, uint32(1), arrow.UINT32},
		{"uint64", pubexpr.LiteralNode{Value: uint(1)}, arrow.PrimitiveTypes.Uint64, uint64(1), arrow.UINT64},
		{"float32", pubexpr.LiteralNode{Value: int64(1)}, arrow.PrimitiveTypes.Float32, float32(1), arrow.FLOAT32},
		{"float64", pubexpr.LiteralNode{Value: float32(1)}, arrow.PrimitiveTypes.Float64, float64(1), arrow.FLOAT64},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, ok, err := promoteLiteralTo(tc.lit, tc.type_)
			require.NoError(t, err)
			require.True(t, ok)
			require.Equal(t, tc.want, got.Value)
			require.Equal(t, tc.id, got.Type.ID())
		})
	}

	lit := pubexpr.LiteralNode{Value: int32(1), Type: arrow.PrimitiveTypes.Int32}
	got, ok, err := promoteLiteralTo(lit, arrow.PrimitiveTypes.Int32)
	require.NoError(t, err)
	require.False(t, ok)
	require.Equal(t, lit, got)

	got, ok, err = promoteLiteralTo(lit, nil)
	require.NoError(t, err)
	require.False(t, ok)
	require.Equal(t, lit, got)

	got, ok, err = promoteLiteralTo(lit, arrow.BinaryTypes.String)
	require.NoError(t, err)
	require.False(t, ok)
	require.Equal(t, lit, got)

	got, ok, err = promoteLiteralTo(pubexpr.LiteralNode{Value: "x"}, arrow.PrimitiveTypes.Int32)
	require.NoError(t, err)
	require.False(t, ok)
	require.Equal(t, "x", got.Value)

	got, ok, err = promoteLiteralTo(pubexpr.LiteralNode{Value: "x"}, arrow.PrimitiveTypes.Float32)
	require.NoError(t, err)
	require.False(t, ok)
	require.Equal(t, "x", got.Value)

	_, ok, err = promoteLiteralTo(pubexpr.LiteralNode{Value: "x"}, arrow.PrimitiveTypes.Uint32)
	require.ErrorContains(t, err, "cannot convert string to uint64")
	require.False(t, ok)
}

func TestConversionHelpers(t *testing.T) {
	t.Parallel()

	for _, v := range []any{int(1), int8(1), int16(1), int32(1), int64(1), uint(1), uint8(1), uint16(1), uint32(1), uint64(1), float32(1), float64(1)} {
		got, err := toInt64(v)
		require.NoError(t, err)
		require.Equal(t, int64(1), got)
	}
	_, err := toInt64("x")
	require.ErrorContains(t, err, "cannot convert string to int64")

	for _, v := range []any{int(1), int8(1), int16(1), int32(1), int64(1), uint(1), uint8(1), uint16(1), uint32(1), uint64(1)} {
		got, err := toUint64(v)
		require.NoError(t, err)
		require.Equal(t, uint64(1), got)
	}
	for _, v := range []any{int(-1), int8(-1), int16(-1), int32(-1), int64(-1)} {
		_, err := toUint64(v)
		require.ErrorContains(t, err, "negative literal cannot be unsigned")
	}
	_, err = toUint64(float64(1))
	require.ErrorContains(t, err, "cannot convert float64 to uint64")

	for _, v := range []any{int(1), int8(1), int16(1), int32(1), int64(1), uint(1), uint8(1), uint16(1), uint32(1), uint64(1), float32(1), float64(1)} {
		got, err := toFloat64(v)
		require.NoError(t, err)
		require.Equal(t, float64(1), got)
	}
	_, err = toFloat64("x")
	require.ErrorContains(t, err, "cannot convert string to float64")

	require.True(t, isNumeric(arrow.PrimitiveTypes.Int8))
	require.True(t, isNumeric(arrow.PrimitiveTypes.Uint64))
	require.True(t, isNumeric(arrow.PrimitiveTypes.Float64))
	require.False(t, isNumeric(nil))
	require.False(t, isNumeric(arrow.BinaryTypes.String))
}
