// Complete business-state-machine prototype; Raft is the pinned upstream library.
// Trusted fail-stop lab API: authentication, production access control and clocks
// are intentionally outside its claim. External effects NEVER execute in Apply.
package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
)

type Obj = map[string]interface{}

func str(o Obj, k string) string { v, _ := o[k].(string); return v }
func num(o Obj, k string) int64 {
	v, ok := integerValue(o[k])
	if !ok {
		return 0
	}
	return v
}
func obj(o Obj, k string) Obj        { v, _ := o[k].(map[string]interface{}); return v }
func boolean(o Obj, k string) bool   { v, _ := o[k].(bool); return v }
func canonical(v interface{}) string { b, _ := json.Marshal(v); return string(b) }
func digest(v interface{}) string {
	h := sha256.Sum256([]byte(canonical(v)))
	return hex.EncodeToString(h[:])
}
func clone[T any](v T) T { var c T; json.Unmarshal([]byte(canonical(v)), &c); return c }
func code(c string) Obj  { return Obj{"code": c} }
func sid(s Obj) string {
	return fmt.Sprintf("%d/%s/%s", num(s, "account_id"), str(s, "action"), str(s, "session_id"))
}

type Session struct {
	Account  int64  `json:"account_id"`
	Action   string `json:"action"`
	Deadline int64  `json:"deadline"`
	Closed   bool   `json:"closed"`
}
type Operation struct {
	S                   Obj    `json:"s"`
	Input               Obj    `json:"input"`
	Version             string `json:"input_version"`
	P                   string `json:"p"`
	C                   string `json:"c"`
	T                   string `json:"t"`
	Scope               string `json:"scope"`
	Key                 string `json:"key"`
	PaymentInput        Obj    `json:"payment_input"`
	Acceptance          Obj    `json:"acceptance"`
	Result              Obj    `json:"result"`
	ProviderResult      Obj    `json:"provider_result"`
	PaymentResult       Obj    `json:"payment_result"`
	Order               string `json:"order_id"`
	EventID             string `json:"event_id"`
	Event               Obj    `json:"event"`
	Owner               string `json:"owner"`
	WorkEpoch           int64  `json:"work_epoch"`
	WorkState           string `json:"work_state"`
	Published           bool   `json:"published"`
	Bundle              bool   `json:"bundle"`
	ReservationReady    bool   `json:"reservation_ready"`
	ConsumerReceipt     Obj    `json:"consumer_receipt,omitempty"`
	NextDeliveryAttempt int64  `json:"next_delivery_attempt,omitempty"`
	ConsumerSource      Obj    `json:"consumer_source,omitempty"`
	HandoffConfirmed    bool   `json:"handoff_confirmed,omitempty"`
	CurrentProgress     Obj    `json:"current_progress,omitempty"`
}
type Refund struct {
	S              Obj    `json:"s"`
	Input          Obj    `json:"input"`
	RF             string `json:"rf"`
	SourceC        string `json:"source_c"`
	Scope          string `json:"scope"`
	Key            string `json:"key"`
	ProviderInput  Obj    `json:"provider_input"`
	Status         string `json:"status"`
	ProviderResult Obj    `json:"provider_result"`
	Result         Obj    `json:"result"`
}
type Participant struct {
	Bucket string `json:"bucket"`
	SKU    string `json:"sku"`
	Qty    int64  `json:"qty"`
}
type Tx struct {
	ID           string            `json:"id"`
	SKey         string            `json:"s_key"`
	Participants []Participant     `json:"participants"`
	Votes        map[string]string `json:"votes"`
	Decision     string            `json:"decision"`
	Done         bool              `json:"done"`
	DeadlineMS   int64             `json:"deadline_ms,omitempty"`
}
type Prepared struct {
	ID          string `json:"id"`
	Coordinator string `json:"coordinator"`
	Input       Obj    `json:"input"`
	Vote        string `json:"vote"`
	State       string `json:"state"`
}
type Bucket struct {
	ID           string                `json:"id"`
	Mode         string                `json:"mode"`
	Epoch        int64                 `json:"epoch"`
	Next         int64                 `json:"next"`
	Migration    string                `json:"migration"`
	ImportedSHA  string                `json:"imported_sha256,omitempty"`
	Sessions     map[string]Session    `json:"sessions"`
	Operations   map[string]*Operation `json:"operations"`
	Refunds      map[string]*Refund    `json:"refunds"`
	Stock        map[string]int64      `json:"stock"`
	Prepared     map[string]*Prepared  `json:"prepared"`
	Transactions map[string]*Tx        `json:"transactions"`
}
type Route struct {
	Owner     string `json:"owner"`
	Epoch     int64  `json:"epoch"`
	Migration string `json:"migration"`
	SHA       string `json:"sha256"`
}
type State struct {
	Buckets map[string]*Bucket `json:"buckets"`
	Routes  map[string]Route   `json:"routes"`
	Applied uint64             `json:"applied_index"`
}
type FSM struct {
	mu    sync.Mutex
	state State
	emit  func(Obj)
}

func empty() State { return State{Buckets: map[string]*Bucket{}, Routes: map[string]Route{}} }
func newBucket(id string, epoch int64) *Bucket {
	return &Bucket{ID: id, Mode: "active", Epoch: epoch, Sessions: map[string]Session{}, Operations: map[string]*Operation{}, Refunds: map[string]*Refund{}, Stock: map[string]int64{}, Prepared: map[string]*Prepared{}, Transactions: map[string]*Tx{}}
}

func (f *FSM) apply(c Obj, index uint64) interface{} {
	kind := str(c, "kind")
	bid := str(c, "bucket")
	if kind == "init" {
		if b := f.state.Buckets[bid]; b != nil {
			return code("exists")
		}
		b := newBucket(bid, num(c, "epoch"))
		f.state.Buckets[bid] = b
		if st := obj(c, "stock"); st != nil {
			for k, v := range st {
				n, ok := integerValue(v)
				if !ok || n < 0 {
					return code("invalid_input")
				}
				b.Stock[k] = n
			}
		}
		return code("created")
	}
	if kind == "route" {
		old, ok := f.state.Routes[bid]
		route := obj(c, "route")
		if ok && (old.Owner != str(c, "old_owner") || old.Epoch != num(c, "old_epoch")) {
			if canonical(old) == canonical(route) {
				return old
			}
			return code("route_conflict")
		}
		r := Route{str(route, "owner"), num(route, "epoch"), str(route, "migration"), str(route, "sha256")}
		if ok && r.Epoch <= old.Epoch {
			return code("epoch_conflict")
		}
		f.state.Routes[bid] = r
		return r
	}
	if kind == "install" {
		var incoming Bucket
		if e := json.Unmarshal([]byte(canonical(c["image"])), &incoming); e != nil {
			return code("invalid_image")
		}
		if digest(&incoming) != str(c, "sha256") || incoming.Mode != "frozen" || incoming.ID != bid ||
			incoming.Migration == "" || num(c, "new_epoch") <= incoming.Epoch {
			return code("invalid_image")
		}
		if e := validateState(State{Buckets: map[string]*Bucket{bid: &incoming}, Routes: map[string]Route{}}); e != nil {
			return code("invalid_image")
		}
		if old := f.state.Buckets[bid]; old != nil {
			if old.Migration == incoming.Migration && old.ImportedSHA == str(c, "sha256") {
				return code("installed")
			}
			return code("destination_exists")
		}
		incoming.Mode = "inactive"
		incoming.Epoch = num(c, "new_epoch")
		incoming.ImportedSHA = str(c, "sha256")
		f.state.Buckets[bid] = &incoming
		return code("installed")
	}
	b := f.state.Buckets[bid]
	if b == nil {
		return code("not_owner")
	}
	if kind == "activate" {
		if b.Migration != str(c, "migration") || b.Epoch != num(c, "epoch") {
			return code("activation_conflict")
		}
		if b.Mode != "inactive" && b.Mode != "active" {
			return code("activation_conflict")
		}
		// The trusted migration service checks the committed control-plane route.
		b.Mode = "active"
		return code("active")
	}
	if kind == "freeze" {
		for _, p := range b.Prepared {
			if p.State == "prepared" {
				return code("prepared_blocks_migration")
			}
		}
		for _, t := range b.Transactions {
			if !t.Done {
				return code("transaction_blocks_migration")
			}
		}
		if b.Mode == "frozen" && b.Migration == str(c, "migration") {
			return Obj{"code": "frozen", "image": clone(b), "sha256": digest(b)}
		}
		if b.Mode != "active" || b.Epoch != num(c, "epoch") {
			return code("freeze_conflict")
		}
		b.Mode = "frozen"
		b.Migration = str(c, "migration")
		return Obj{"code": "frozen", "image": clone(b), "sha256": digest(b), "freeze_index": index}
	}
	if b.Mode != "active" {
		return Obj{"code": "not_active", "bucket": bid, "epoch": b.Epoch, "mode": b.Mode}
	}
	if num(c, "epoch") != b.Epoch {
		return Obj{"code": "stale_route", "epoch": b.Epoch, "bucket": bid}
	}
	switch kind {
	case "session":
		s := obj(c, "s")
		key := sid(s)
		n := Session{num(s, "account_id"), str(s, "action"), num(c, "deadline"), false}
		if old, ok := b.Sessions[key]; ok {
			if canonical(old) != canonical(n) {
				return code("session_conflict")
			}
			return old
		}
		b.Sessions[key] = n
		return n
	case "close":
		key := sid(obj(c, "s"))
		ss, ok := b.Sessions[key]
		if !ok {
			return code("missing_session")
		}
		ss.Closed = true
		b.Sessions[key] = ss
		return ss
	case "accept":
		if !validPurchase(c) {
			return code("invalid_input")
		}
		s := obj(c, "s")
		inp := obj(c, "input")
		sk := canonical(s)
		ss, ok := b.Sessions[sid(s)]
		if !ok || ss.Closed || ss.Deadline <= num(c, "now_upper") {
			return code("operation_expired")
		}
		if old := b.Operations[sk]; old != nil {
			if old.Version != str(c, "input_version") || canonical(old.Input) != canonical(inp) {
				return code("conflict")
			}
			if old.Result != nil {
				return old.Result
			}
			return old.Acceptance
		}
		if str(s, "action") != "purchase.create.external.v2" || str(c, "input_version") != "external-v2" ||
			str(inp, "product_id") != "paper-001" || num(inp, "quantity") != 1 || str(inp, "quote_id") != "Q7" ||
			str(inp, "currency") != "CNY" || num(inp, "amount_minor") != 3000 || str(inp, "payment_method") != "saved-card-7" {
			return code("invalid_input")
		}
		if len(inp) != 6 && !(len(inp) == 7 && boolean(inp, "bundle")) {
			return code("invalid_input")
		}
		b.Next++
		suffix := fmt.Sprintf("%s:%d", bid, b.Next)
		p, cc, t := "P:"+suffix, "C:"+suffix, "T:"+suffix
		acc := Obj{"purchase_id": p, "charge_id": cc, "operation": clone(s), "product_id": "paper-001", "quantity": 1, "quote_id": "Q7", "currency": "CNY", "amount_minor": 3000}
		b.Operations[sk] = &Operation{S: clone(s), Input: clone(inp), Version: "external-v2", P: p, C: cc, T: t,
			Scope: "simulator:merchant-7:test:charge.v1", Key: "pay-" + cc,
			PaymentInput: Obj{"account_id": num(s, "account_id"), "charge_id": cc, "currency": "CNY", "amount_minor": 3000},
			Acceptance:   acc, WorkState: "ready", Bundle: boolean(inp, "bundle"), ReservationReady: !boolean(inp, "bundle")}
		return acc
	case "claim":
		o := b.Operations[str(c, "s_key")]
		if o == nil {
			return code("missing")
		}
		if o.Result != nil {
			return code("done")
		}
		if !o.ReservationReady {
			return code("reservation_not_ready")
		}
		o.WorkEpoch++
		o.Owner = str(c, "owner")
		o.WorkState = "running"
		return clone(o)
	case "observe":
		o := b.Operations[str(c, "s_key")]
		if o == nil {
			return code("missing")
		}
		pr := obj(c, "provider_result")
		if o.Result != nil {
			if canonical(o.ProviderResult) != canonical(pr) {
				return code("evidence_conflict")
			}
			return o.Result
		}
		if o.Owner != str(c, "owner") || o.WorkEpoch != num(c, "work_epoch") {
			return code("stale_worker")
		}
		if str(pr, "scope") != o.Scope || str(pr, "key") != o.Key || str(pr, "charge_id") != o.C ||
			num(pr, "account_id") != num(o.S, "account_id") || str(pr, "currency") != "CNY" || num(pr, "amount_minor") != 3000 {
			return code("evidence_conflict")
		}
		if str(pr, "status") != "succeeded" {
			return code("pending")
		}
		o.ProviderResult = clone(pr)
		// Imported legacy associations are retained verbatim.
		if o.Order == "" {
			o.Order = "O:" + o.C[2:]
		}
		if o.EventID == "" {
			o.EventID = "EV:" + o.C[2:]
		}
		o.PaymentResult = Obj{"purchase_id": o.P, "charge_id": o.C, "order_id": o.Order, "status": "succeeded", "provider_scope": o.Scope,
			"provider_charge_id": str(pr, "provider_charge_id"), "currency": "CNY", "amount_minor": 3000}
		payload := Obj{"purchase_id": o.P, "charge_id": o.C, "order_id": o.Order, "provider_charge_id": str(pr, "provider_charge_id"), "currency": "CNY", "amount_minor": 3000}
		o.Event = Obj{"event_id": o.EventID, "event_type": "purchase.paid", "event_version": 1, "occurred_at": "2026-10-18T01:00:00Z", "payload": payload}
		o.Result = Obj{"result_version": "purchase-result-external-v2", "acceptance": clone(o.Acceptance), "progress": Obj{"business_state": "paid", "fulfillment_state": "unfulfilled"}, "payment_result": clone(o.PaymentResult)}
		o.WorkState = "done"
		return o.Result
	case "published":
		o := b.Operations[str(c, "s_key")]
		if o == nil || o.Event == nil {
			return code("missing_event")
		}
		if canonical(o.Event) != canonical(c["event"]) {
			return code("event_conflict")
		}
		o.Published = true
		if !o.HandoffConfirmed {
			o.NextDeliveryAttempt = num(c, "retry_after")
		}
		return code("published")
	case "consumer_observed":
		// Trusted recovery service obtains this receipt from the committed Inbox.
		o := b.Operations[str(c, "s_key")]
		if o == nil || o.Event == nil {
			return code("missing_event")
		}
		r, source := obj(c, "receipt"), obj(c, "source")
		if canonical(o.Event) != canonical(c["event"]) || !validHandoffEvidence(o, r, source) {
			return code("receipt_conflict")
		}
		if o.ConsumerReceipt != nil && canonical(o.ConsumerReceipt) != canonical(r) {
			return code("receipt_conflict")
		}
		if o.ConsumerSource != nil && canonical(o.ConsumerSource) != canonical(source) {
			return code("receipt_conflict")
		}
		if !o.Published {
			return code("publication_not_recorded")
		}
		o.ConsumerReceipt = clone(r)
		o.ConsumerSource = clone(source)
		o.HandoffConfirmed = true
		// Result is the immutable original payment response. Queries expose the
		// newer fulfillment projection separately from that saved response.
		o.CurrentProgress = Obj{"business_state": "paid", "fulfillment_state": "fulfillment_accepted"}
		return code("consumer_observed")
	case "refund_accept":
		s := obj(c, "s")
		input := obj(c, "input")
		key := canonical(s)
		ss, ok := b.Sessions[sid(s)]
		if !ok || ss.Closed || ss.Deadline <= num(c, "now_upper") {
			return code("operation_expired")
		}
		if old := b.Refunds[key]; old != nil {
			if canonical(old.Input) != canonical(input) {
				return code("conflict")
			}
			return old
		}
		if str(s, "action") != "refund.create.v1" {
			return code("invalid_action")
		}
		if len(input) != 3 || str(input, "currency") != "CNY" {
			return code("invalid_input")
		}
		var source *Operation
		for _, o := range b.Operations {
			if o.C == str(input, "source_c") {
				source = o
			}
		}
		if source == nil || source.ProviderResult == nil || num(s, "account_id") != num(source.S, "account_id") {
			return code("missing_original_charge")
		}
		amount := num(input, "amount_minor")
		used := int64(0)
		for _, r := range b.Refunds {
			if r.SourceC == source.C && r.Status != "rejected" {
				used += num(r.Input, "amount_minor")
			}
		}
		if amount <= 0 || amount+used > 3000 {
			return code("refund_capacity_exceeded")
		}
		b.Next++
		rf := fmt.Sprintf("RF:%s:%d", bid, b.Next)
		r := &Refund{S: clone(s), Input: clone(input), RF: rf, SourceC: source.C, Scope: "simulator:merchant-7:test:refund.v1", Key: "refund-" + rf, Status: "pending",
			ProviderInput: Obj{"account_id": num(s, "account_id"), "refund_id": rf, "source_c": source.C, "source_x": str(source.ProviderResult, "provider_charge_id"), "currency": "CNY", "amount_minor": amount}}
		b.Refunds[key] = r
		return r
	case "refund_observe":
		r := b.Refunds[str(c, "s_key")]
		if r == nil {
			return code("missing")
		}
		pr := obj(c, "provider_result")
		if str(pr, "scope") != r.Scope || str(pr, "key") != r.Key || canonical(c["provider_input"]) != canonical(r.ProviderInput) {
			return code("evidence_conflict")
		}
		if r.ProviderResult != nil {
			if canonical(pr) != canonical(r.ProviderResult) {
				return code("evidence_conflict")
			}
			return r.Result
		}
		if str(pr, "status") != "succeeded" && str(pr, "status") != "rejected" {
			return code("pending")
		}
		if str(pr, "refund_id") != r.RF || str(pr, "source_charge_id") != r.SourceC ||
			str(pr, "source_provider_charge_id") != str(r.ProviderInput, "source_x") || str(pr, "currency") != "CNY" ||
			num(pr, "amount_minor") != num(r.Input, "amount_minor") {
			return code("evidence_conflict")
		}
		if str(pr, "status") == "succeeded" && str(pr, "provider_refund_id") == "" {
			return code("evidence_conflict")
		}
		r.ProviderResult = clone(pr)
		r.Status = str(pr, "status")
		r.Result = Obj{"refund_id": r.RF, "status": r.Status, "source_charge_id": r.SourceC, "provider_result": clone(pr)}
		return r.Result
	case "tx_begin":
		key := str(c, "s_key")
		o := b.Operations[key]
		if o == nil || !o.Bundle {
			return code("not_bundle")
		}
		id := "TX:" + o.P
		// A missing deadline preserves legacy, explicitly unbounded lab histories.
		// A configured deadline is fixed by the first committed begin, never renewed.
		deadline := int64(0)
		if _, ok := c["deadline_ms"]; ok {
			if !positive(c, "deadline_ms", safeInteger) {
				return code("invalid_deadline")
			}
			deadline = num(c, "deadline_ms")
		}
		if t := b.Transactions[id]; t != nil {
			if canonical(c["participants"]) != canonical(t.Participants) {
				return code("transaction_conflict")
			}
			if _, ok := c["deadline_ms"]; ok && deadline != t.DeadlineMS {
				return code("transaction_conflict")
			}
			return t
		}
		var ps []Participant
		json.Unmarshal([]byte(canonical(c["participants"])), &ps)
		if len(ps) != 2 || ps[0].Bucket == ps[1].Bucket || ps[0].Bucket == bid || ps[1].Bucket == bid {
			return code("invalid_participants")
		}
		t := &Tx{ID: id, SKey: key, Participants: ps, Votes: map[string]string{}, DeadlineMS: deadline}
		b.Transactions[id] = t
		return t
	case "prepare":
		id := str(c, "tx_id") + ":" + bid
		input := obj(c, "input")
		if old := b.Prepared[id]; old != nil {
			if canonical(old.Input) != canonical(input) || old.Coordinator != str(c, "coordinator") {
				return code("prepare_conflict")
			}
			return old
		}
		sku := str(input, "sku")
		qty := num(input, "qty")
		vote := "no"
		state := "rejected"
		if qty > 0 && b.Stock[sku] >= qty {
			vote = "yes"
			state = "prepared"
			b.Stock[sku] -= qty
		}
		p := &Prepared{ID: id, Coordinator: str(c, "coordinator"), Input: clone(input), Vote: vote, State: state}
		b.Prepared[id] = p
		return p
	case "tx_vote":
		t := b.Transactions[str(c, "tx_id")]
		if t == nil {
			return code("missing_tx")
		}
		part := str(c, "participant")
		valid := false
		for _, p := range t.Participants {
			if p.Bucket == part {
				valid = true
			}
		}
		if !valid || (str(c, "vote") != "yes" && str(c, "vote") != "no") {
			return code("invalid_vote")
		}
		// A delayed vote observes the terminal decision without mutating its evidence.
		if t.Decision != "" {
			return t
		}
		if old, ok := t.Votes[part]; ok && old != str(c, "vote") {
			return code("vote_conflict")
		}
		t.Votes[part] = str(c, "vote")
		return t
	case "tx_timeout":
		t := b.Transactions[str(c, "tx_id")]
		if t == nil {
			return code("missing_tx")
		}
		// The trusted recovery service authenticates before this lab API.
		// Apply compares committed inputs; it never reads a replica's wall clock.
		if str(c, "coordinator") != bid || t.DeadlineMS <= 0 {
			return code("invalid_timeout_source")
		}
		if !positive(c, "deadline_ms", safeInteger) || num(c, "deadline_ms") != t.DeadlineMS ||
			!positive(c, "observed_ms", safeInteger) {
			return code("timeout_conflict")
		}
		if t.Decision != "" {
			return t
		}
		if num(c, "observed_ms") < t.DeadlineMS {
			return code("deadline_not_reached")
		}
		t.Decision = "abort"
		return t
	case "tx_decide":
		t := b.Transactions[str(c, "tx_id")]
		if t == nil {
			return code("missing_tx")
		}
		if t.Decision != "" {
			return t
		}
		all := true
		no := false
		for _, p := range t.Participants {
			v := t.Votes[p.Bucket]
			if v == "" {
				all = false
			}
			if v == "no" {
				no = true
			}
		}
		if no {
			t.Decision = "abort"
		} else if all {
			t.Decision = "commit"
		} else {
			return code("undecided")
		}
		return t
	case "finish":
		d := str(c, "decision")
		if d != "commit" && d != "abort" {
			return code("invalid_decision")
		}
		id := str(c, "tx_id") + ":" + bid
		p := b.Prepared[id]
		input := obj(c, "input")
		coordinator := str(c, "coordinator")
		if str(c, "tx_id") == "" || coordinator == "" || str(input, "sku") == "" || num(input, "qty") <= 0 {
			return code("invalid_input")
		}
		if p == nil {
			if d == "commit" {
				return code("missing_prepare")
			}
			// Abort may arrive before the first prepare. Remember its immutable
			// source and input so a delayed prepare cannot create an orphan hold.
			p = &Prepared{ID: id, Coordinator: coordinator, Input: clone(input), Vote: "no", State: "aborted"}
			b.Prepared[id] = p
			return p
		}
		if p.Coordinator != coordinator || canonical(p.Input) != canonical(input) {
			return code("prepare_conflict")
		}
		if p.State == "committed" {
			if d == "commit" {
				return p
			}
			return code("decision_conflict")
		}
		if p.State == "aborted" {
			if d == "abort" {
				return p
			}
			return code("decision_conflict")
		}
		if d == "commit" {
			if p.Vote != "yes" {
				return code("invalid_commit")
			}
			p.State = "committed"
		} else {
			if p.State == "prepared" {
				b.Stock[str(p.Input, "sku")] += num(p.Input, "qty")
			}
			p.State = "aborted"
		}
		return p
	case "tx_done":
		t := b.Transactions[str(c, "tx_id")]
		if t == nil || t.Decision == "" {
			return code("missing_decision")
		}
		var completed []*Prepared
		json.Unmarshal([]byte(canonical(c["completions"])), &completed)
		if len(completed) != len(t.Participants) {
			return code("incomplete_participants")
		}
		seen := map[string]bool{}
		want := "aborted"
		if t.Decision == "commit" {
			want = "committed"
		}
		for _, p := range t.Participants {
			found := false
			for _, done := range completed {
				if done != nil && done.ID == t.ID+":"+p.Bucket && !seen[done.ID] && done.Coordinator == bid &&
					done.State == want && canonical(done.Input) == canonical(Obj{"sku": p.SKU, "qty": p.Qty}) {
					seen[done.ID] = true
					found = true
					break
				}
			}
			if !found {
				return code("incomplete_participants")
			}
		}
		t.Done = true
		o := b.Operations[t.SKey]
		if t.Decision == "commit" {
			o.ReservationReady = true
		} else {
			o.WorkState = "done"
			o.Result = Obj{"result_version": "purchase-result-external-v2", "acceptance": clone(o.Acceptance), "progress": Obj{"business_state": "rejected", "reason": "inventory_unavailable"}}
		}
		return t
	}
	return code("unknown_command")
}
func (f *FSM) Apply(l *raft.Log) interface{} {
	var c Obj
	if err := json.Unmarshal(l.Data, &c); err != nil {
		panic(err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.state.Applied = l.Index
	return clone(f.apply(c, l.Index))
}

type Snapshot struct {
	Data []byte
	Emit func(Obj)
}

func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	data, e := encodeSnapshot(f.state)
	if e != nil {
		return nil, e
	}
	return &Snapshot{data, f.emit}, nil
}
func (s *Snapshot) Persist(sink raft.SnapshotSink) error {
	if _, e := sink.Write(s.Data); e != nil {
		sink.Cancel()
		return e
	}
	if e := sink.Close(); e != nil {
		return e
	}
	h := sha256.Sum256(s.Data)
	s.Emit(Obj{"event": "snapshot_persisted", "bytes": len(s.Data), "sha256": hex.EncodeToString(h[:])})
	return nil
}
func (s *Snapshot) Release() {}
func (f *FSM) Restore(r io.ReadCloser) error {
	defer r.Close()
	data, e := io.ReadAll(r)
	if e != nil {
		return e
	}
	s, e := decodeSnapshot(data)
	if e != nil {
		return e
	}
	f.mu.Lock()
	f.state = s
	f.mu.Unlock()
	h := sha256.Sum256(data)
	f.emit(Obj{"event": "fsm_restored", "applied_index": s.Applied, "bytes": len(data), "sha256": hex.EncodeToString(h[:])})
	return nil
}

// Transport faults affect actual TCP streams owned by this process. A node's
// isolation closes existing inbound/outbound streams and refuses new streams.
type faultStream struct {
	net.Listener
	mu       sync.Mutex
	isolated bool
	delay    time.Duration
	conns    map[net.Conn]bool
}
type faultConn struct {
	net.Conn
	stream *faultStream
}

func (c *faultConn) Write(b []byte) (int, error) {
	c.stream.mu.Lock()
	iso, d := c.stream.isolated, c.stream.delay
	c.stream.mu.Unlock()
	if iso {
		return 0, errors.New("lab network isolated")
	}
	if d > 0 {
		time.Sleep(d)
	}
	return c.Conn.Write(b)
}
func (s *faultStream) track(c net.Conn) net.Conn {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.isolated {
		c.Close()
		return nil
	}
	s.conns[c] = true
	return &faultConn{c, s}
}
func (s *faultStream) Dial(addr raft.ServerAddress, t time.Duration) (net.Conn, error) {
	s.mu.Lock()
	iso := s.isolated
	s.mu.Unlock()
	if iso {
		return nil, errors.New("lab network isolated")
	}
	c, e := net.DialTimeout("tcp", string(addr), t)
	if e != nil {
		return nil, e
	}
	c = s.track(c)
	if c == nil {
		return nil, errors.New("isolated")
	}
	return c, nil
}
func (s *faultStream) Accept() (net.Conn, error) {
	for {
		c, e := s.Listener.Accept()
		if e != nil {
			return nil, e
		}
		if c = s.track(c); c != nil {
			return c, nil
		}
	}
}
func (s *faultStream) fault(iso bool, delay int64) {
	s.mu.Lock()
	s.isolated = iso
	s.delay = time.Duration(delay) * time.Millisecond
	if iso {
		for c := range s.conns {
			c.Close()
		}
		s.conns = map[net.Conn]bool{}
	}
	s.mu.Unlock()
}

type Peer struct {
	ID   string `json:"id"`
	Raft string `json:"raft"`
	HTTP string `json:"http"`
}

func main() {
	id := flag.String("id", "", "node")
	group := flag.String("group", "", "group")
	address := flag.String("raft", "", "TCP address")
	httpAddr := flag.String("http", "", "HTTP")
	dir := flag.String("dir", "", "own persistence")
	peersJSON := flag.String("peers", "", "initial full member list")
	bootstrap := flag.Bool("bootstrap", false, "bootstrap once")
	flag.Parse()
	os.MkdirAll(*dir, 0700)
	logFile, e := os.OpenFile(filepath.Join(*dir, "events.jsonl"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
	if e != nil {
		panic(e)
	}
	var eventMu sync.Mutex
	emit := func(o Obj) {
		eventMu.Lock()
		defer eventMu.Unlock()
		o["node"] = *id
		o["group"] = *group
		o["pid"] = os.Getpid()
		json.NewEncoder(logFile).Encode(o)
		logFile.Sync()
	}
	var peers []Peer
	if e = json.Unmarshal([]byte(*peersJSON), &peers); e != nil {
		panic(e)
	}
	l, e := net.Listen("tcp", *address)
	if e != nil {
		panic(e)
	}
	stream := &faultStream{Listener: l, conns: map[net.Conn]bool{}}
	transport := raft.NewNetworkTransport(stream, 5, 2*time.Second, os.Stderr)
	store, e := raftboltdb.NewBoltStore(filepath.Join(*dir, "raft.db"))
	if e != nil {
		panic(e)
	}
	snaps, e := raft.NewFileSnapshotStore(*dir, 3, os.Stderr)
	if e != nil {
		panic(e)
	}
	cfg := raft.DefaultConfig()
	cfg.LocalID = raft.ServerID(*id)
	cfg.HeartbeatTimeout = 650 * time.Millisecond
	cfg.ElectionTimeout = 650 * time.Millisecond
	cfg.LeaderLeaseTimeout = 300 * time.Millisecond
	cfg.CommitTimeout = 20 * time.Millisecond
	cfg.SnapshotInterval = 24 * time.Hour
	cfg.SnapshotThreshold = 128
	cfg.TrailingLogs = 16
	f := &FSM{state: empty(), emit: emit}
	r, e := raft.NewRaft(cfg, f, store, store, snaps, transport)
	if e != nil {
		panic(e)
	}
	if *bootstrap {
		var servers []raft.Server
		for _, p := range peers {
			servers = append(servers, raft.Server{ID: raft.ServerID(p.ID), Address: raft.ServerAddress(p.Raft), Suffrage: raft.Voter})
		}
		if e := r.BootstrapCluster(raft.Configuration{Servers: servers}).Error(); e != nil {
			log.Print(e)
		}
	}
	reply := func(w http.ResponseWriter, v interface{}) {
		w.Header().Set("Content-Type", "application/json")
		if e := json.NewEncoder(w).Encode(v); e != nil {
			emit(Obj{"event": "response_write_error", "error": e.Error()})
		}
	}
	leader := func() string {
		a, _ := r.LeaderWithID()
		for _, p := range peers {
			if p.Raft == string(a) {
				return p.HTTP
			}
		}
		return ""
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/status", func(w http.ResponseWriter, q *http.Request) {
		reply(w, Obj{"id": *id, "group": *group, "state": r.State().String(), "leader": leader(), "stats": r.Stats(), "pid": os.Getpid()})
	})
	mux.HandleFunc("/fault", func(w http.ResponseWriter, q *http.Request) {
		var c Obj
		json.NewDecoder(q.Body).Decode(&c)
		stream.fault(boolean(c, "isolated"), num(c, "delay_ms"))
		emit(Obj{"event": "transport_fault", "config": c})
		reply(w, code("configured"))
	})
	mux.HandleFunc("/snapshot", func(w http.ResponseWriter, q *http.Request) {
		// Consume the administrative POST body before waiting for Raft work.
		io.Copy(io.Discard, q.Body)
		q.Body.Close()
		emit(Obj{"event": "snapshot_request", "connection": q.Header.Get("Connection")})
		if e := r.Snapshot().Error(); e != nil {
			emit(Obj{"event": "snapshot_request_error", "error": e.Error()})
			reply(w, Obj{"code": "snapshot_error", "error": e.Error()})
			return
		}
		emit(Obj{"event": "snapshot_request_complete"})
		reply(w, code("snapshot_complete"))
	})
	mux.HandleFunc("/state", func(w http.ResponseWriter, q *http.Request) {
		if r.State() != raft.Leader {
			reply(w, Obj{"code": "not_leader", "leader": leader()})
			return
		}
		if e := r.VerifyLeader().Error(); e != nil {
			reply(w, Obj{"code": "unavailable", "error": e.Error()})
			return
		}
		if e := r.Barrier(2 * time.Second).Error(); e != nil {
			reply(w, Obj{"code": "unavailable", "error": e.Error()})
			return
		}
		f.mu.Lock()
		s := clone(f.state)
		f.mu.Unlock()
		reply(w, s)
	})
	mux.HandleFunc("/command", func(w http.ResponseWriter, q *http.Request) {
		raw, e := strictJSON(io.LimitReader(q.Body, 32<<20))
		if e != nil {
			reply(w, code("invalid_json"))
			return
		}
		c, ok := raw.(map[string]interface{})
		if !ok {
			reply(w, code("invalid_json"))
			return
		}
		if str(c, "kind") == "accept" && !validPurchase(c) {
			reply(w, code("invalid_input"))
			return
		}
		if str(c, "kind") == "refund_accept" && (!validIdentity(obj(c, "s")) || !positive(obj(c, "input"), "amount_minor", safeInteger)) {
			reply(w, code("invalid_input"))
			return
		}
		if r.State() != raft.Leader {
			reply(w, Obj{"code": "not_leader", "leader": leader()})
			return
		}
		future := r.Apply([]byte(canonical(c)), 2*time.Second)
		if e := future.Error(); e != nil {
			emit(Obj{"event": "apply_unknown", "command": c, "error": e.Error()})
			reply(w, Obj{"code": "unknown", "error": e.Error(), "leader": leader()})
			return
		}
		ans := future.Response()
		emit(Obj{"event": "committed_response", "command": c, "result": ans, "index": future.Index(), "current_term": r.Stats()["term"]})
		reply(w, ans)
	})
	emit(Obj{"event": "process_start", "raft": *address, "http": *httpAddr})
	log.Fatal(http.ListenAndServe(*httpAddr, mux))
}
