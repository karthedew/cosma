package expr

import "testing"

func TestLiteralString(t *testing.T) {
	if got := Lit(5).String(); got != "lit(5)" {
		t.Fatalf("Literal String = %q, want lit(5)", got)
	}
}

func TestBinaryString(t *testing.T) {
	expr := Bin(OpAdd, Lit(1), Lit(2))
	if got := expr.String(); got != "(lit(1) + lit(2))" {
		t.Fatalf("Binary String = %q, want (lit(1) + lit(2))", got)
	}
}

func TestAggString(t *testing.T) {
	if got := Sum(Lit(1)).String(); got != "sum(lit(1))" {
		t.Fatalf("Sum String = %q, want sum(lit(1))", got)
	}
	if got := Count(Lit("x")).String(); got != "count(lit(x))" {
		t.Fatalf("Count String = %q, want count(lit(x))", got)
	}
}
