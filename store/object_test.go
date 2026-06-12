package store

import "testing"

func TestAttrsString(t *testing.T) {
	a := Attrs{"s": "hello", "n": 3}

	if v, ok := a.String("s"); !ok || v != "hello" {
		t.Errorf("String(s) = %q, %v", v, ok)
	}
	if _, ok := a.String("n"); ok {
		t.Error("String(n) should fail for an int value")
	}
	if _, ok := a.String("absent"); ok {
		t.Error("String(absent) should fail")
	}
}

func TestAttrsStrings(t *testing.T) {
	a := Attrs{
		"typed": []string{"x", "y"},
		"json":  []any{"x", "y"}, // shape produced by encoding/json
		"mixed": []any{"x", 1},
		"n":     7,
	}

	if v, ok := a.Strings("typed"); !ok || len(v) != 2 || v[1] != "y" {
		t.Errorf("Strings(typed) = %v, %v", v, ok)
	}
	if v, ok := a.Strings("json"); !ok || len(v) != 2 || v[0] != "x" {
		t.Errorf("Strings(json) = %v, %v", v, ok)
	}
	if _, ok := a.Strings("mixed"); ok {
		t.Error("Strings(mixed) should fail when an element is not a string")
	}
	if _, ok := a.Strings("n"); ok {
		t.Error("Strings(n) should fail for a scalar")
	}
	if _, ok := a.Strings("absent"); ok {
		t.Error("Strings(absent) should fail")
	}
}

func TestAttrsInt(t *testing.T) {
	a := Attrs{
		"i64":   int64(42),
		"i":     7,
		"i32":   int32(5),
		"whole": 12.0, // JSON numbers decode as float64
		"frac":  12.5,
		"s":     "12",
	}

	if v, ok := a.Int("i64"); !ok || v != 42 {
		t.Errorf("Int(i64) = %d, %v", v, ok)
	}
	if v, ok := a.Int("i"); !ok || v != 7 {
		t.Errorf("Int(i) = %d, %v", v, ok)
	}
	if v, ok := a.Int("i32"); !ok || v != 5 {
		t.Errorf("Int(i32) = %d, %v", v, ok)
	}
	if v, ok := a.Int("whole"); !ok || v != 12 {
		t.Errorf("Int(whole) = %d, %v — whole floats must round-trip", v, ok)
	}
	if _, ok := a.Int("frac"); ok {
		t.Error("Int(frac) should fail for a fractional float")
	}
	if _, ok := a.Int("s"); ok {
		t.Error("Int(s) should fail for a string")
	}
	if _, ok := a.Int("absent"); ok {
		t.Error("Int(absent) should fail")
	}
}

func TestAttrsFloat(t *testing.T) {
	a := Attrs{
		"f64": 2.5,
		"f32": float32(1.5),
		"i64": int64(3),
		"i":   4,
		"s":   "2.5",
	}

	if v, ok := a.Float("f64"); !ok || v != 2.5 {
		t.Errorf("Float(f64) = %v, %v", v, ok)
	}
	if v, ok := a.Float("f32"); !ok || v != 1.5 {
		t.Errorf("Float(f32) = %v, %v", v, ok)
	}
	if v, ok := a.Float("i64"); !ok || v != 3 {
		t.Errorf("Float(i64) = %v, %v — ints widen", v, ok)
	}
	if v, ok := a.Float("i"); !ok || v != 4 {
		t.Errorf("Float(i) = %v, %v — ints widen", v, ok)
	}
	if _, ok := a.Float("s"); ok {
		t.Error("Float(s) should fail for a string")
	}
	if _, ok := a.Float("absent"); ok {
		t.Error("Float(absent) should fail")
	}
}

func TestAttrsBool(t *testing.T) {
	a := Attrs{"b": true, "n": 1}

	if v, ok := a.Bool("b"); !ok || !v {
		t.Errorf("Bool(b) = %v, %v", v, ok)
	}
	if _, ok := a.Bool("n"); ok {
		t.Error("Bool(n) should fail for an int")
	}
	if _, ok := a.Bool("absent"); ok {
		t.Error("Bool(absent) should fail")
	}
}

func TestNilAttrsAccessorsSafe(t *testing.T) {
	var a Attrs
	if _, ok := a.String("k"); ok {
		t.Error("nil Attrs String should be false")
	}
	if _, ok := a.Int("k"); ok {
		t.Error("nil Attrs Int should be false")
	}
}

func TestEnumStrings(t *testing.T) {
	if KindGroup.String() != "group" || KindArray.String() != "array" {
		t.Error("ObjectKind String mismatch")
	}
	if OrderC.String() != "C" || OrderF.String() != "F" {
		t.Error("MemoryOrder String mismatch")
	}
	if LittleEndian.String() != "little" || BigEndian.String() != "big" {
		t.Error("Endianness String mismatch")
	}
}
