package event

import "testing"

func TestInitEventReturnsInstanceScopedWorkers(t *testing.T) {
	first := InitEvent()
	second := InitEvent()

	if len(first) != 1 {
		t.Fatalf("unexpected first bean count: %d", len(first))
	}
	if len(second) != 1 {
		t.Fatalf("unexpected second bean count: %d", len(second))
	}
	if first[0] == second[0] {
		t.Fatal("expected InitEvent to return a distinct worker for each server instance")
	}
}
