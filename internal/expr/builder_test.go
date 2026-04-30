package expr

import (
	"testing"

	"github.com/apache/arrow/go/v18/arrow"

	"github.com/karthedew/cosma/schema"
)

func sampleSchema() *schema.Schema {
	return schema.New(
		schema.Field{Name: "price", Type: schema.Float64, ArrowType: arrow.PrimitiveTypes.Float64},
		schema.Field{Name: "quantity", Type: schema.Int64, ArrowType: arrow.PrimitiveTypes.Int64},
		schema.Field{Name: "region", Type: schema.Utf8, ArrowType: arrow.BinaryTypes.String},
		schema.Field{Name: "order_id", Type: schema.Int64, ArrowType: arrow.PrimitiveTypes.Int64},
	)
}

func TestBuilderFilterChain(t *testing.T) {
	got := Col("price").Gt(100).And(Col("region").Eq("West")).Build()
	want := "((price > 100) and (region == \"West\"))"
	if got.String() != want {
		t.Fatalf("filter chain = %q, want %q", got.String(), want)
	}
	dt, err := got.DataType(sampleSchema())
	if err != nil {
		t.Fatalf("DataType: %v", err)
	}
	if dt.ID() != arrow.BOOL {
		t.Fatalf("filter result type = %s, want bool", dt)
	}
}

func TestBuilderComputedColumn(t *testing.T) {
	got := Col("price").Mul(1.10).Alias("price_with_tax").Build()
	want := "(price * 1.1) as price_with_tax"
	if got.String() != want {
		t.Fatalf("computed column = %q, want %q", got.String(), want)
	}
	dt, err := got.DataType(sampleSchema())
	if err != nil {
		t.Fatalf("DataType: %v", err)
	}
	// price (float64) * 1.10 (float64) → float64
	if dt.ID() != arrow.FLOAT64 {
		t.Fatalf("price * 1.10 type = %s, want float64", dt)
	}
}

func TestBuilderColumnOnColumn(t *testing.T) {
	got := Col("price").Mul(Col("quantity")).Alias("revenue").Build()
	want := "(price * quantity) as revenue"
	if got.String() != want {
		t.Fatalf("col*col = %q, want %q", got.String(), want)
	}
	// price (float64) * quantity (int64) → float64
	dt, err := got.DataType(sampleSchema())
	if err != nil {
		t.Fatalf("DataType: %v", err)
	}
	if dt.ID() != arrow.FLOAT64 {
		t.Fatalf("price * quantity type = %s, want float64", dt)
	}
}

func TestBuilderAggregations(t *testing.T) {
	cases := []struct {
		name    string
		expr    Expr
		wantStr string
		wantID  arrow.Type
	}{
		{"sum",   Col("quantity").Sum().Alias("total").Build(),   "sum(quantity) as total",   arrow.INT64},
		{"mean",  Col("quantity").Mean().Alias("avg").Build(),    "mean(quantity) as avg",    arrow.FLOAT64},
		{"min",   Col("price").Min().Build(),                     "min(price)",               arrow.FLOAT64},
		{"max",   Col("price").Max().Build(),                     "max(price)",               arrow.FLOAT64},
		{"count", Col("order_id").Count().Alias("orders").Build(), "count(order_id) as orders", arrow.INT64},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.expr.String() != tc.wantStr {
				t.Fatalf("String() = %q, want %q", tc.expr.String(), tc.wantStr)
			}
			dt, err := tc.expr.DataType(sampleSchema())
			if err != nil {
				t.Fatalf("DataType: %v", err)
			}
			if dt.ID() != tc.wantID {
				t.Fatalf("type = %s, want %s", dt, tc.wantID)
			}
		})
	}
}

func TestBuilderTypedLiteralEscapeHatch(t *testing.T) {
	// Default Lit(5) infers int64; Int32(5) opts into int32 explicitly.
	def := Lit(5).Build()
	if dt, _ := def.DataType(nil); dt.ID() != arrow.INT64 {
		t.Fatalf("Lit(5) inferred %s, want int64", dt)
	}
	typed := Int32(5).Build()
	if dt, _ := typed.DataType(nil); dt.ID() != arrow.INT32 {
		t.Fatalf("Int32(5) = %s, want int32", dt)
	}
	if got := typed.String(); got != "5" {
		t.Fatalf("Int32(5).String() = %q, want %q", got, "5")
	}
}

func TestBuilderRhsAcceptsExprBuilderAndScalar(t *testing.T) {
	// rhs: ExprBuilder
	a := Col("price").Eq(Col("quantity")).Build()
	if got, want := a.String(), "(price == quantity)"; got != want {
		t.Fatalf("ExprBuilder rhs: %q, want %q", got, want)
	}
	// rhs: scalar (auto-Lit)
	b := Col("price").Eq(42).Build()
	if got, want := b.String(), "(price == 42)"; got != want {
		t.Fatalf("scalar rhs: %q, want %q", got, want)
	}
	// rhs: raw Expr value
	c := Col("price").Eq(ColumnNode{Name: "quantity"}).Build()
	if got, want := c.String(), "(price == quantity)"; got != want {
		t.Fatalf("Expr rhs: %q, want %q", got, want)
	}
}

func TestBuilderUnaryAndCast(t *testing.T) {
	// Not on a comparison
	notExpr := Col("price").Gt(100).Not().Build()
	if got, want := notExpr.String(), "not((price > 100))"; got != want {
		t.Fatalf("Not chain: %q, want %q", got, want)
	}
	// Cast int64 column to float64
	castExpr := Col("quantity").Cast(arrow.PrimitiveTypes.Float64).Build()
	dt, err := castExpr.DataType(sampleSchema())
	if err != nil {
		t.Fatalf("Cast DataType: %v", err)
	}
	if dt.ID() != arrow.FLOAT64 {
		t.Fatalf("cast result = %s, want float64", dt)
	}
	// IsNull
	isNull := Col("region").IsNull().Build()
	if got, want := isNull.String(), "is_null(region)"; got != want {
		t.Fatalf("IsNull: %q, want %q", got, want)
	}
}

func TestBuilderBindFailsOnTypeMismatch(t *testing.T) {
	// region is string, comparing with numeric should fail at DataType().
	bad := Col("region").Gt(100).Build()
	if _, err := bad.DataType(sampleSchema()); err == nil {
		t.Fatalf("expected type-mismatch error on string > int")
	}
}
