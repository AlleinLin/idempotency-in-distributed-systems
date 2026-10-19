package main

import (
	"bytes"
	"io"
	"testing"
)

func timedFixture(t *testing.T) (*FSM, string, string) {
	t.Helper()
	state := fixture()
	b := state.Buckets["b1"]
	var key string
	for k, o := range b.Operations {
		key = k
		o.Bundle = true
		o.ReservationReady = false
	}
	f := &FSM{state: state, emit: func(Obj) {}}
	command := Obj{"kind": "tx_begin", "bucket": "b1", "epoch": 2, "s_key": key,
		"participants": []Participant{{"b2", "SKU-A", 1}, {"b3", "SKU-B", 1}}, "deadline_ms": 100}
	answer, ok := f.apply(command, 43).(*Tx)
	if !ok || answer.DeadlineMS != 100 {
		t.Fatal("timed begin", answer)
	}
	command["deadline_ms"] = 200
	if canonical(f.apply(command, 44)) != canonical(code("transaction_conflict")) {
		t.Fatal("deadline renewal accepted")
	}
	return f, answer.ID, key
}

func TestTimeoutDecisionOrdersAndSnapshot(t *testing.T) {
	// Enumerate semantically different log orders through the actual FSM.
	for _, order := range [][]string{
		{"last_yes", "decide", "timeout"}, {"last_yes", "timeout", "decide"},
		{"timeout", "last_yes", "decide"},
	} {
		t.Run(canonical(order), func(t *testing.T) {
			f, id, key := timedFixture(t)
			call := func(kind string, fields Obj) interface{} {
				c := Obj{"kind": kind, "bucket": "b1", "epoch": 2, "tx_id": id}
				for k, v := range fields {
					c[k] = v
				}
				return f.apply(c, 45)
			}
			call("tx_vote", Obj{"participant": "b2", "vote": "yes"})
			timeout := Obj{"coordinator": "b1", "deadline_ms": 100, "observed_ms": 100}
			premature := clone(timeout)
			premature["observed_ms"] = 99
			if canonical(call("tx_timeout", premature)) != canonical(code("deadline_not_reached")) {
				t.Fatal("early timeout accepted")
			}
			for _, event := range order {
				switch event {
				case "last_yes":
					call("tx_vote", Obj{"participant": "b3", "vote": "yes"})
				case "decide":
					call("tx_decide", Obj{})
				case "timeout":
					call("tx_timeout", timeout)
				}
			}
			want := "abort"
			if order[1] == "decide" {
				want = "commit"
			}
			tx := f.state.Buckets["b1"].Transactions[id]
			if tx.Decision != want || tx.Done || f.state.Buckets["b1"].Operations[key].ReservationReady {
				t.Fatal("decision or payment gate", tx)
			}
			before := canonical(tx)
			call("tx_vote", Obj{"participant": "b3", "vote": "no"})
			call("tx_decide", Obj{})
			call("tx_timeout", timeout)
			if canonical(tx) != before {
				t.Fatal("terminal transaction changed")
			}
			raw, err := encodeSnapshot(f.state)
			if err != nil {
				t.Fatal(err)
			}
			restored := &FSM{state: empty(), emit: func(Obj) {}}
			if err := restored.Restore(io.NopCloser(bytes.NewReader(raw))); err != nil {
				t.Fatal(err)
			}
			if canonical(restored.state) != canonical(f.state) {
				t.Fatal("snapshot lost deadline or decision")
			}
		})
	}
}
