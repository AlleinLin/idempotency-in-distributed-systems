"""Versioned JSON boundary for the public purchase request, not authentication."""

import json
import re

MAX_INTEGER = 9007199254740991
TOKEN = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}\Z")


class InputError(ValueError):
    pass


def strict_loads(raw):
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise InputError("duplicate JSON member: " + key)
            result[key] = value
        return result

    def bad_constant(value):
        raise InputError("non-finite JSON number")

    try:
        return json.loads(raw, object_pairs_hook=pairs, parse_constant=bad_constant)
    except (ValueError, UnicodeError) as exc:
        raise InputError(str(exc)) from exc


def integer(value, name, upper=MAX_INTEGER):
    if type(value) is not int or not 1 <= value <= upper:
        raise InputError(name + " must be an integer in the documented range")


def identity(s):
    if type(s) is not dict or set(s) != {"account_id", "action", "session_id", "idempotency_key"}:
        raise InputError("S must contain exactly four members")
    integer(s["account_id"], "account_id")
    for name in ("action", "session_id", "idempotency_key"):
        if type(s[name]) is not str or not TOKEN.fullmatch(s[name]):
            raise InputError(name + " must be an ASCII token of 1 to 128 bytes")


def purchase(request):
    if type(request) is not dict or set(request) != {"s", "input"}:
        raise InputError("request must contain exactly s and input")
    identity(request["s"])
    inp = request["input"]
    required = {"product_id", "quantity", "quote_id", "currency", "amount_minor", "payment_method"}
    if type(inp) is not dict or set(inp) not in (required, required | {"bundle"}):
        raise InputError("unknown or missing input member")
    for name in ("product_id", "quote_id", "payment_method"):
        if type(inp[name]) is not str or not TOKEN.fullmatch(inp[name]):
            raise InputError(name + " must be an ASCII token of 1 to 128 bytes")
    if type(inp["currency"]) is not str or not re.fullmatch("[A-Z]{3}", inp["currency"]):
        raise InputError("currency must contain three ASCII uppercase letters")
    integer(inp["amount_minor"], "amount_minor")
    integer(inp["quantity"], "quantity", 1000000)
    if "bundle" in inp and type(inp["bundle"]) is not bool:
        raise InputError("bundle must be boolean")
    return request
