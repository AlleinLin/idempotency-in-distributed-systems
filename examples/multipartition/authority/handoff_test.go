package main

import (
	"bytes"
	"io"
	"testing"
)

func TestHandoffAtomicEvidenceAndOriginalResult(t *testing.T) {
	f := &FSM{state: fixture(), emit: func(Obj) {}}
	var key string
	var o *Operation
	for k, operation := range f.state.Buckets["b1"].Operations {
		key, o = k, operation
	}
	o.Event = Obj{"event_id": "EV1", "event_type": "purchase.paid", "event_version": 1,
		"occurred_at": "2026-10-18T01:00:00Z", "payload": Obj{"order_id": "O1", "amount_minor": 3000}}
	o.Result = Obj{"acceptance": clone(o.Acceptance),
		"progress": Obj{"business_state": "paid", "fulfillment_state": "unfulfilled"}}
	original := canonical(o.Result)
	receipt := Obj{"event_id": "EV1", "order_id": "O1", "fulfillment_id": 501, "status": "fulfillment_accepted"}
	source := Obj{"domain": "fulfillment-primary", "record_id": "EV1", "event": clone(o.Event),
		"business_guard": "initial:O1", "dispatch_key": "dispatch:initial:O1",
		"dispatch_input": Obj{"fulfillment_id": 501, "order_id": "O1", "kind": "initial"}}
	command := Obj{"kind": "consumer_observed", "bucket": "b1", "epoch": 2, "s_key": key,
		"event": clone(o.Event), "receipt": receipt, "source": source}
	for name, mutate := range map[string]func(Obj){
		"event payload": func(c Obj) { obj(obj(obj(c, "source"), "event"), "payload")["amount_minor"] = 1 },
		"order":         func(c Obj) { obj(c, "receipt")["order_id"] = "O2" },
		"fulfillment":   func(c Obj) { obj(c, "receipt")["fulfillment_id"] = 502 },
		"guard":         func(c Obj) { obj(c, "source")["business_guard"] = "initial:O2" },
		"dispatch":      func(c Obj) { obj(c, "source")["dispatch_key"] = "dispatch:initial:O2" },
		"source domain": func(c Obj) { obj(c, "source")["domain"] = "other-domain" },
		"source record": func(c Obj) { obj(c, "source")["record_id"] = "EV2" },
	} {
		t.Run(name, func(t *testing.T) {
			bad := clone(command)
			mutate(bad)
			before := canonical(f.state)
			if canonical(f.apply(bad, 43)) != canonical(code("receipt_conflict")) || canonical(f.state) != before {
				t.Fatal("damaged evidence accepted or mutated state")
			}
		})
	}
	if canonical(f.apply(command, 44)) != canonical(code("publication_not_recorded")) {
		t.Fatal("receipt bypassed publication state")
	}
	f.apply(Obj{"kind": "published", "bucket": "b1", "epoch": 2, "s_key": key, "event": o.Event, "retry_after": 100}, 45)
	if canonical(f.apply(command, 46)) != canonical(code("consumer_observed")) ||
		!o.HandoffConfirmed || !o.Published || o.ConsumerSource == nil ||
		str(o.CurrentProgress, "fulfillment_state") != "fulfillment_accepted" {
		t.Fatal("handoff did not atomically save all facts")
	}
	if canonical(o.Result) != original {
		t.Fatal("current fulfillment progress overwrote the saved original response")
	}
	before := canonical(o)
	f.apply(Obj{"kind": "published", "bucket": "b1", "epoch": 2, "s_key": key, "event": o.Event, "retry_after": 999}, 47)
	f.apply(command, 48)
	if canonical(o) != before {
		t.Fatal("late publication or repeated handoff changed the terminal facts")
	}
	raw, err := encodeSnapshot(f.state)
	if err != nil {
		t.Fatal(err)
	}
	recovered := &FSM{state: empty(), emit: func(Obj) {}}
	if err := recovered.Restore(io.NopCloser(bytes.NewReader(raw))); err != nil || canonical(recovered.state) != canonical(f.state) {
		t.Fatal("snapshot lost handoff evidence/current progress/original response", err)
	}
}
