package main

import (
	"encoding/json"
	"reflect"
	"sort"
	"testing"
)

var header = []string{"step", "type", "amount", "nameOrig", "oldbalanceOrg", "newbalanceOrig",
	"nameDest", "oldbalanceDest", "newbalanceDest", "isFraud", "isFlaggedFraud"}

func row(step, typ, amount string) []string {
	return []string{step, typ, amount, "C1", "500.0", "0.0", "C9", "0.0", "0.0", "1", "0"}
}

// The Spark job parses this exact schema; it must match producer/replay.py field for field.
func TestEventJSONMatchesPythonSchema(t *testing.T) {
	ev, err := buildEvent(row("1", "TRANSFER", "500.0"), index(header), 1, 0)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := json.Marshal(ev)
	var m map[string]any
	_ = json.Unmarshal(b, &m)
	got := make([]string, 0, len(m))
	for k := range m {
		got = append(got, k)
	}
	sort.Strings(got)
	want := []string{"account_id", "amount", "dest_id", "event_ts", "is_fraud", "new_balance_dest",
		"new_balance_orig", "old_balance_dest", "old_balance_orig", "txn_id", "txn_type"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("fields = %v, want %v", got, want)
	}
	if ev.EventTS != "2026-01-01T01:00:00.000000" || ev.Amount != 500 || ev.IsFraud != 1 {
		t.Fatalf("unexpected event: %+v", ev)
	}
}

func TestEventTimeIsMonotonicWithinAnHour(t *testing.T) {
	col := index(header)
	a, _ := buildEvent(row("5", "PAYMENT", "1"), col, 1, 0)
	b, _ := buildEvent(row("5", "PAYMENT", "1"), col, 2, 51_351) // busiest PaySim hour
	if !(a.EventTS < b.EventTS) || b.EventTS >= "2026-01-01T06:00:00" {
		t.Fatalf("timestamps not monotonic inside hour 5: %s, %s", a.EventTS, b.EventTS)
	}
}

func TestBadNumberIsAnError(t *testing.T) {
	if _, err := buildEvent(row("1", "PAYMENT", "abc"), index(header), 1, 0); err == nil {
		t.Fatal("expected error for non-numeric amount")
	}
}
