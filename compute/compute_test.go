package compute

import "testing"

func TestNotImplementedOperations(t *testing.T) {
	if _, err := Filter(nil, nil); err == nil {
		t.Fatalf("expected Filter error")
	}
	if _, err := Project(nil, nil); err == nil {
		t.Fatalf("expected Project error")
	}
	if _, err := GroupBy(nil, nil, nil); err == nil {
		t.Fatalf("expected GroupBy error")
	}
	if _, err := Join(nil, nil, nil, Inner); err == nil {
		t.Fatalf("expected Join error")
	}
}
