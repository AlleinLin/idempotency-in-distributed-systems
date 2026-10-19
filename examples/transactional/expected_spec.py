"""Independent full baseline oracle. Does not import protocol or read actual data.

Numbers, envelopes, IDs and terminal results here are specified before execution.
Expected dictionaries include every column and every nested field of all tables.
"""

import hashlib
import json
from copy import deepcopy

S = {
    "account_id": 1,
    "action": "purchase.create.external.v2",
    "session_id": "E1",
    "idempotency_key": "K1",
}
INPUT = {
    "product_id": "paper-001",
    "quantity": 1,
    "quote_id": "Q7",
    "currency": "CNY",
    "amount_minor": 3000,
    "payment_method": "saved-card-7",
}
SCOPE = "simulator:merchant-7:test:charge.v1"
PI = {"account_id": 1, "charge_id": "C1", "currency": "CNY", "amount_minor": 3000}
ACC = {
    "purchase_id": "P1",
    "charge_id": "C1",
    "operation": S,
    "product_id": "paper-001",
    "quantity": 1,
    "quote_id": "Q7",
    "currency": "CNY",
    "amount_minor": 3000,
}
PROVIDER = {
    "status": "succeeded",
    "scope": SCOPE,
    "key": "pay-C1",
    "account_id": 1,
    "charge_id": "C1",
    "provider_charge_id": "X1",
    "currency": "CNY",
    "amount_minor": 3000,
}
PAYMENT = {
    "purchase_id": "P1",
    "charge_id": "C1",
    "order_id": "O1",
    "status": "succeeded",
    "provider_scope": SCOPE,
    "provider_charge_id": "X1",
    "currency": "CNY",
    "amount_minor": 3000,
}
PAYLOAD = {
    "purchase_id": "P1",
    "charge_id": "C1",
    "order_id": "O1",
    "provider_charge_id": "X1",
    "currency": "CNY",
    "amount_minor": 3000,
}
EVENT = {
    "event_id": "EV1",
    "event_type": "purchase.paid",
    "event_version": 1,
    "occurred_at": "2026-10-18T01:00:00Z",
    "payload": PAYLOAD,
}
RESULT = {
    "result_version": "purchase-result-external-v2",
    "acceptance": ACC,
    "progress": {"business_state": "paid", "fulfillment_state": "unfulfilled"},
    "payment_result": PAYMENT,
}
RECEIPT = {
    "event_id": "EV1",
    "order_id": "O1",
    "fulfillment_id": 501,
    "status": "fulfillment_accepted",
}


def baseline():
    pb = {"acceptance": ACC, "version": "external-v2", "input": INPUT, "business_state": "paid"}
    cb = {
        "purchase_id": "P1",
        "task_id": "T1",
        "provider_scope": SCOPE,
        "provider_key": "pay-C1",
        "payment_input": PI,
        "status": "succeeded",
        "provider_result": PROVIDER,
        "payment_result": PAYMENT,
    }
    state = deepcopy(
        {
            "authority": {
                "directory": [{"id": 1, "epoch": 8, "mode": "open"}],
                "sessions": [
                    {
                        "id": "E1",
                        "account_id": 1,
                        "action": "purchase.create.external.v2",
                        "deadline": 1000,
                        "status": "active",
                    }
                ],
                "counters": [{"id": 1, "p": 1, "c": 1, "o": 1, "ev": 1, "rf": 0}],
                "wallets": [{"account_id": 1, "balance": 10000}],
                "debits": [],
                "operations": [
                    {
                        "s": S,
                        "version": "external-v2",
                        "input": INPUT,
                        "p": "P1",
                        "c": "C1",
                        "t": "T1",
                        "acceptance": ACC,
                        "result": RESULT,
                    }
                ],
                "charges": [
                    {
                        "c": "C1",
                        "p": "P1",
                        "t": "T1",
                        "scope": SCOPE,
                        "key": "pay-C1",
                        "input": PI,
                        "status": "succeeded",
                        "provider_result": PROVIDER,
                        "payment_result": PAYMENT,
                        "o": "O1",
                        "ev": "EV1",
                        "event": EVENT,
                    }
                ],
                "workflow": [{"t": "T1", "c": "C1", "epoch": 2, "owner": "W2", "status": "done"}],
                "refunds": [],
            },
            "local": {
                "gate": [{"id": 1, "epoch": 8, "mode": "open"}],
                "purchases": [{"id": "P1", "s": S, "body": pb}],
                "payments": [{"id": "C1", "p": "P1", "scope": SCOPE, "key": "pay-C1", "body": cb}],
                "tasks": [{"id": "T1", "c": "C1", "body": {"charge_id": "C1", "state": "done"}}],
                "orders": [
                    {
                        "id": "O1",
                        "p": "P1",
                        "c": "C1",
                        "x": "X1",
                        "body": {
                            "purchase_id": "P1",
                            "charge_id": "C1",
                            "provider_scope": SCOPE,
                            "provider_charge_id": "X1",
                            "currency": "CNY",
                            "amount_minor": 3000,
                            "fulfillment_state": "unfulfilled",
                        },
                    }
                ],
                "outbox": [{"id": "EV1", "o": "O1", "body": EVENT}],
                "refund_projection": [],
                "projection_cursor": [
                    {
                        "id": 1,
                        "body": {
                            "operations": 1,
                            "charges": 1,
                            "refunds": 0,
                            "image": "chapter15-frozen-authority-image-v1",
                        },
                    }
                ],
            },
            "platform": {
                "accounts": [{"id": 1, "balance": 7000}],
                "platform_operations": [
                    {
                        "scope": SCOPE,
                        "key": "pay-C1",
                        "kind": "charge",
                        "input": PI,
                        "status": "succeeded",
                        "result": PROVIDER,
                    }
                ],
            },
            "consumer": {
                "fulfillments": [{"id": 501, "order_id": "O1", "body": PAYLOAD}],
                "business_guard": [{"b": "initial:O1", "f": 501}],
                "inbox": [{"event_id": "EV1", "event": EVENT, "f": 501, "receipt": RECEIPT}],
                "dispatch": [
                    {
                        "key": "dispatch:initial:O1",
                        "f": 501,
                        "body": {"fulfillment_id": 501, "order_id": "O1", "kind": "initial"},
                        "result": {
                            "status": "accepted",
                            "dispatch_key": "dispatch:initial:O1",
                            "fulfillment_id": 501,
                            "provider_dispatch_id": "DSP-501",
                        },
                    }
                ],
            },
        }
    )
    image = {k: state["authority"][k] for k in ("operations", "charges", "refunds")}
    state["local"]["projection_cursor"][0]["body"]["snapshot_sha256"] = hashlib.sha256(
        json.dumps(image, sort_keys=True, ensure_ascii=False, separators=(",", ":")).encode()
    ).hexdigest()
    return state


def normalized(state):
    return {
        d: {
            t: sorted(rows, key=lambda r: json.dumps(r, sort_keys=True, ensure_ascii=False))
            for t, rows in tables.items()
        }
        for d, tables in state.items()
    }


def assert_financial_and_relational(state):
    """Independent interpretation, not protocol-produced expected values."""
    a, p, c = state["authority"], state["platform"], state["consumer"]
    assert len({json.dumps(r["s"], sort_keys=True) for r in a["operations"]}) == len(
        a["operations"]
    )
    assert len({(r["scope"], r["key"]) for r in a["charges"]}) == len(a["charges"])
    for q in a["charges"]:
        op = [r for r in a["operations"] if r["p"] == q["p"]]
        assert len(op) == 1 and op[0]["c"] == q["c"] and op[0]["t"] == q["t"]
        if q["status"] == "succeeded":
            assert q["payment_result"]["order_id"] == q["o"]
            assert q["event"]["event_id"] == q["ev"]
            assert (
                q["event"]["payload"]["provider_charge_id"]
                == q["provider_result"]["provider_charge_id"]
            )
        rs = [r for r in a["refunds"] if r["source_c"] == q["c"]]
        assert (
            sum(r["amount"] for r in rs if r["status"] in ("pending", "succeeded"))
            <= q["input"]["amount_minor"]
        )
        for r in rs:
            assert r["source_x"] == q["provider_result"]["provider_charge_id"]
    payments = sum(
        r["input"]["amount_minor"]
        for r in p["platform_operations"]
        if r["kind"] == "charge" and r["status"] == "succeeded"
    )
    refunds = sum(
        r["input"]["amount_minor"]
        for r in p["platform_operations"]
        if r["kind"] == "refund" and r["status"] == "succeeded"
    )
    assert p["accounts"] == [{"id": 1, "balance": 10000 - payments + refunds}]
    assert a["wallets"] == [
        {"account_id": 1, "balance": 10000 - sum(r["amount"] for r in a["debits"])}
    ]
    assert len({r["b"] for r in c["business_guard"]}) == len(c["business_guard"])
    for row in c["inbox"]:
        assert any(
            f["id"] == row["f"] and f["order_id"] == row["event"]["payload"]["order_id"]
            for f in c["fulfillments"]
        )
