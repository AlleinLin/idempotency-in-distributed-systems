package main

import (
	"encoding/json"
	"fmt"
)

// This lab trusts the recovery service's authenticated read of the consumer.
// The source object records that read's immutable business relationships, not a
// cryptographic proof supplied by an untrusted caller.
func validHandoffEvidence(o *Operation, r, source Obj) bool {
	if o == nil || o.Event == nil || o.Result == nil || obj(o.Result, "progress") == nil ||
		!exactFields(r, "event_id", "order_id", "fulfillment_id", "status") ||
		str(r, "event_id") != o.EventID || str(r, "order_id") != o.Order ||
		!positive(r, "fulfillment_id", safeInteger) || str(r, "status") != "fulfillment_accepted" {
		return false
	}
	if !exactFields(source, "domain", "record_id", "event", "business_guard", "dispatch_key", "dispatch_input") ||
		str(source, "domain") != "fulfillment-primary" || str(source, "record_id") != o.EventID ||
		canonical(source["event"]) != canonical(o.Event) || str(source, "business_guard") != "initial:"+o.Order ||
		str(source, "dispatch_key") != "dispatch:initial:"+o.Order {
		return false
	}
	return canonical(source["dispatch_input"]) == canonical(Obj{"fulfillment_id": num(r, "fulfillment_id"), "order_id": o.Order, "kind": "initial"})
}

// Integrity detects incomplete/corrupt images in this trusted fail-stop lab.
// It is not an authenticity signature and not a disaster rollback watermark.
type SnapshotEnvelope struct {
	Format int            `json:"format_version"`
	State  State          `json:"state"`
	SHA    string         `json:"state_sha256"`
	Counts map[string]int `json:"counts"`
}

func stateCounts(s State) map[string]int {
	c := map[string]int{"buckets": len(s.Buckets), "routes": len(s.Routes), "sessions": 0, "operations": 0, "refunds": 0, "stock": 0, "prepared": 0, "transactions": 0}
	for _, b := range s.Buckets {
		c["sessions"] += len(b.Sessions)
		c["operations"] += len(b.Operations)
		c["refunds"] += len(b.Refunds)
		c["stock"] += len(b.Stock)
		c["prepared"] += len(b.Prepared)
		c["transactions"] += len(b.Transactions)
	}
	return c
}
func validateState(s State) error {
	if s.Buckets == nil || s.Routes == nil {
		return fmt.Errorf("missing top-level maps")
	}
	charges := map[string]bool{}
	for id, b := range s.Buckets {
		if b == nil || b.ID != id || b.Epoch <= 0 {
			return fmt.Errorf("invalid bucket identity")
		}
		if b.Mode != "active" && b.Mode != "inactive" && b.Mode != "frozen" {
			return fmt.Errorf("invalid bucket mode")
		}
		if b.Sessions == nil || b.Operations == nil || b.Refunds == nil || b.Stock == nil || b.Prepared == nil || b.Transactions == nil {
			return fmt.Errorf("incomplete bucket maps")
		}
		for key, o := range b.Operations {
			if o == nil || !validIdentity(o.S) || canonical(o.S) != key || o.C == "" || o.P == "" || o.T == "" || o.Key != "pay-"+o.C || o.Scope == "" || o.Acceptance == nil || o.PaymentInput == nil {
				return fmt.Errorf("incomplete operation")
			}
			if charges[o.C] {
				return fmt.Errorf("duplicate charge")
			}
			charges[o.C] = true
			if _, ok := b.Sessions[sid(o.S)]; !ok {
				return fmt.Errorf("missing operation session")
			}
			if str(o.Acceptance, "charge_id") != o.C || str(o.Acceptance, "purchase_id") != o.P || str(o.PaymentInput, "charge_id") != o.C {
				return fmt.Errorf("broken operation linkage")
			}
			if o.Published && o.Event == nil {
				return fmt.Errorf("published without original event")
			}
			if o.ConsumerReceipt != nil {
				r := o.ConsumerReceipt
				if o.Event == nil || str(r, "event_id") != o.EventID || str(r, "order_id") != o.Order ||
					num(r, "fulfillment_id") <= 0 || str(r, "status") != "fulfillment_accepted" {
					return fmt.Errorf("invalid consumer handoff")
				}
			}
			if o.HandoffConfirmed {
				if !o.Published || !validHandoffEvidence(o, o.ConsumerReceipt, o.ConsumerSource) ||
					canonical(o.CurrentProgress) != canonical(Obj{"business_state": "paid", "fulfillment_state": "fulfillment_accepted"}) {
					return fmt.Errorf("incomplete confirmed handoff")
				}
			} else if o.ConsumerSource != nil {
				return fmt.Errorf("source without confirmed handoff")
			}
			if o.ProviderResult != nil && (o.Result == nil || o.PaymentResult == nil || o.Event == nil) {
				return fmt.Errorf("incomplete observed result")
			}
		}
		for key, r := range b.Refunds {
			if r == nil || !validIdentity(r.S) || canonical(r.S) != key || r.RF == "" || r.Key != "refund-"+r.RF || r.ProviderInput == nil {
				return fmt.Errorf("incomplete refund")
			}
			found := false
			for _, o := range b.Operations {
				if o.C == r.SourceC {
					found = true
				}
			}
			if !found {
				return fmt.Errorf("orphan refund")
			}
		}
		for _, n := range b.Stock {
			if n < 0 {
				return fmt.Errorf("negative stock")
			}
		}
		for key, p := range b.Prepared {
			if p == nil || p.ID != key || p.Input == nil || p.Coordinator == "" {
				return fmt.Errorf("incomplete prepared")
			}
		}
		for key, t := range b.Transactions {
			if t == nil || t.ID != key || b.Operations[t.SKey] == nil || t.Votes == nil {
				return fmt.Errorf("incomplete transaction")
			}
			if t.DeadlineMS < 0 || t.DeadlineMS > safeInteger {
				return fmt.Errorf("invalid transaction deadline")
			}
			if t.Decision != "" && t.Decision != "commit" && t.Decision != "abort" {
				return fmt.Errorf("invalid transaction decision")
			}
			if t.Done && t.Decision == "" {
				return fmt.Errorf("completed transaction without decision")
			}
		}
	}
	return nil
}
func encodeSnapshot(s State) ([]byte, error) {
	if e := validateState(s); e != nil {
		return nil, e
	}
	return json.Marshal(SnapshotEnvelope{1, s, digest(s), stateCounts(s)})
}
func decodeSnapshot(data []byte) (State, error) {
	var e SnapshotEnvelope
	if err := json.Unmarshal(data, &e); err != nil {
		return State{}, err
	}
	if e.Format != 1 {
		return State{}, fmt.Errorf("unsupported snapshot format")
	}
	if err := validateState(e.State); err != nil {
		return State{}, err
	}
	if digest(e.State) != e.SHA || canonical(stateCounts(e.State)) != canonical(e.Counts) {
		return State{}, fmt.Errorf("snapshot integrity mismatch")
	}
	return e.State, nil
}
