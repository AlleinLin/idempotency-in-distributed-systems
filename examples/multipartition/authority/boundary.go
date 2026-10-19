package main

import (
	"encoding/json"
	"fmt"
	"io"
	"math"
	"regexp"
	"strconv"
	"strings"
)

const safeInteger int64 = 9007199254740991

var tokenPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`)
var currencyPattern = regexp.MustCompile(`^[A-Z]{3}$`)

func integerValue(v interface{}) (int64, bool) {
	switch n := v.(type) {
	case json.Number:
		if strings.ContainsAny(n.String(), ".eE") {
			return 0, false
		}
		x, e := strconv.ParseInt(n.String(), 10, 64)
		return x, e == nil && x >= -safeInteger && x <= safeInteger
	case int64:
		return n, n >= -safeInteger && n <= safeInteger
	case int:
		x := int64(n)
		return x, x >= -safeInteger && x <= safeInteger
	case float64:
		// Compatibility for decoded legacy images. Wire numbers use json.Number.
		if math.IsNaN(n) || math.IsInf(n, 0) || math.Trunc(n) != n || n > float64(safeInteger) || n < float64(-safeInteger) {
			return 0, false
		}
		return int64(n), true
	}
	return 0, false
}

// Token decoding rejects duplicate object members before a map can erase them.
func strictJSON(r io.Reader) (interface{}, error) {
	d := json.NewDecoder(r)
	d.UseNumber()
	var value func() (interface{}, error)
	value = func() (interface{}, error) {
		t, e := d.Token()
		if e != nil {
			return nil, e
		}
		switch z := t.(type) {
		case json.Delim:
			if z == '{' {
				m := Obj{}
				for d.More() {
					k, e := d.Token()
					if e != nil {
						return nil, e
					}
					key, ok := k.(string)
					if !ok {
						return nil, fmt.Errorf("invalid object key")
					}
					if _, ok := m[key]; ok {
						return nil, fmt.Errorf("duplicate JSON member")
					}
					v, e := value()
					if e != nil {
						return nil, e
					}
					m[key] = v
				}
				_, e := d.Token()
				return m, e
			}
			if z == '[' {
				a := []interface{}{}
				for d.More() {
					v, e := value()
					if e != nil {
						return nil, e
					}
					a = append(a, v)
				}
				_, e := d.Token()
				return a, e
			}
			return nil, fmt.Errorf("unexpected delimiter")
		default:
			return t, nil
		}
	}
	v, e := value()
	if e != nil {
		return nil, e
	}
	if _, e = d.Token(); e != io.EOF {
		return nil, fmt.Errorf("trailing JSON input")
	}
	return v, nil
}

func exactFields(o Obj, fields ...string) bool {
	if len(o) != len(fields) {
		return false
	}
	for _, k := range fields {
		if _, ok := o[k]; !ok {
			return false
		}
	}
	return true
}
func positive(o Obj, k string, limit int64) bool {
	v, ok := integerValue(o[k])
	return ok && v > 0 && v <= limit
}
func validIdentity(s Obj) bool {
	if !exactFields(s, "account_id", "action", "session_id", "idempotency_key") || !positive(s, "account_id", safeInteger) {
		return false
	}
	for _, k := range []string{"action", "session_id", "idempotency_key"} {
		v, ok := s[k].(string)
		if !ok || !tokenPattern.MatchString(v) {
			return false
		}
	}
	return true
}
func validPurchase(c Obj) bool {
	s, inp := obj(c, "s"), obj(c, "input")
	if !validIdentity(s) {
		return false
	}
	fields := []string{"product_id", "quantity", "quote_id", "currency", "amount_minor", "payment_method"}
	if _, ok := inp["bundle"]; ok {
		fields = append(fields, "bundle")
		if _, ok := inp["bundle"].(bool); !ok {
			return false
		}
	}
	if !exactFields(inp, fields...) || !positive(inp, "amount_minor", safeInteger) || !positive(inp, "quantity", 1000000) {
		return false
	}
	for _, k := range []string{"product_id", "quote_id", "payment_method"} {
		v, ok := inp[k].(string)
		if !ok || !tokenPattern.MatchString(v) {
			return false
		}
	}
	return currencyPattern.MatchString(str(inp, "currency"))
}
