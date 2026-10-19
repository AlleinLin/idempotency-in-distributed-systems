package main

import (
	"bytes"
	"encoding/json"
	"io"
	"os"
	"testing"
)

func fixture() State {
	b := newBucket("b1", 2)
	s := Obj{"account_id": 1, "action": "purchase.create.external.v2", "session_id": "E1", "idempotency_key": "K1"}
	b.Sessions[sid(s)] = Session{1, "purchase.create.external.v2", 1000, false}
	b.Operations[canonical(s)] = &Operation{S: s, Input: Obj{"product_id": "paper-001", "quantity": 1, "quote_id": "Q7", "currency": "CNY", "amount_minor": 3000, "payment_method": "saved-card-7"}, Version: "external-v2",
		P: "P1", C: "C1", T: "T1", Scope: "simulator:merchant-7:test:charge.v1", Key: "pay-C1",
		Order: "O1", EventID: "EV1", ReservationReady: true, WorkState: "ready",
		PaymentInput: Obj{"account_id": 1, "charge_id": "C1", "currency": "CNY", "amount_minor": 3000},
		Acceptance:   Obj{"purchase_id": "P1", "charge_id": "C1", "operation": s, "product_id": "paper-001", "quantity": 1, "quote_id": "Q7", "currency": "CNY", "amount_minor": 3000}}
	return State{Buckets: map[string]*Bucket{"b1": b}, Routes: map[string]Route{}, Applied: 42}
}
func TestLegacyFixtureExport(t *testing.T) {
	path := os.Getenv("CH17_LEGACY_FIXTURE")
	if path == "" {
		t.Skip("fixture export not requested")
	}
	b := fixture().Buckets["b1"]
	b.Epoch = 1
	b.Mode = "frozen"
	b.Migration = "legacy-C1-import"
	b.Next = 1
	raw, _ := json.Marshal(Obj{"image": b, "sha256": digest(b)})
	if e := os.WriteFile(path, raw, 0600); e != nil {
		t.Fatal(e)
	}
}
func TestCompleteSnapshotAndCorruptImages(t *testing.T) {
	s := fixture()
	raw, e := encodeSnapshot(s)
	if e != nil {
		t.Fatal(e)
	}
	decoded, e := decodeSnapshot(raw)
	if e != nil || canonical(decoded) != canonical(s) {
		t.Fatal("full image mismatch", e)
	}
	op := decoded.Buckets["b1"].Operations
	for _, o := range op {
		if o.C != "C1" || o.Key != "pay-C1" {
			t.Fatal("legacy identity changed")
		}
	}
	variants := map[string]func(*SnapshotEnvelope){
		"missing operations":   func(e *SnapshotEnvelope) { e.State.Buckets["b1"].Operations = map[string]*Operation{} },
		"missing sessions":     func(e *SnapshotEnvelope) { e.State.Buckets["b1"].Sessions = nil },
		"missing refunds":      func(e *SnapshotEnvelope) { e.State.Buckets["b1"].Refunds = nil },
		"missing pending maps": func(e *SnapshotEnvelope) { e.State.Buckets["b1"].Prepared = nil },
		"rewritten old C": func(e *SnapshotEnvelope) {
			for _, o := range e.State.Buckets["b1"].Operations {
				o.C = "C:b1:1"
			}
		},
		"wrong counts":        func(e *SnapshotEnvelope) { e.Counts["operations"] = 0 },
		"unsupported version": func(e *SnapshotEnvelope) { e.Format = 99 },
	}
	for name, change := range variants {
		t.Run(name, func(t *testing.T) {
			var v SnapshotEnvelope
			json.Unmarshal(raw, &v)
			change(&v)
			bad, _ := json.Marshal(v)
			f := &FSM{state: fixture(), emit: func(Obj) {}}
			before := canonical(f.state)
			if err := f.Restore(io.NopCloser(bytes.NewReader(bad))); err == nil {
				t.Fatal("incomplete image accepted")
			}
			if canonical(f.state) != before {
				t.Fatal("failed restore mutated live state")
			}
		})
	}
	f := &FSM{state: empty(), emit: func(Obj) {}}
	if e = f.Restore(io.NopCloser(bytes.NewReader(raw))); e != nil || canonical(f.state) != canonical(s) {
		t.Fatal("restore", e)
	}
}
