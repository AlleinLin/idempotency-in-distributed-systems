"""Independent, explicit scenario specification. Imports no application modules.

The rows below describe the requested history rather than being discovered from
the system under test. A separate row is required when that history changes.
"""

import json

ACTION = "purchase.create.external.v2"
SCOPE = "simulator:merchant-7:test:charge.v1"
INPUT = {
    "product_id": "paper-001",
    "quantity": 1,
    "quote_id": "Q7",
    "currency": "CNY",
    "amount_minor": 3000,
    "payment_method": "saved-card-7",
}
# account, client key, stable bucket, local allocation, provider effect, fulfillment
ROWS = [
    (1, "K1", "b1", 1, 1, 501),
    (2, "K1", "b2", 1, 2, 502),
    (3, "K1", "b3", 1, 3, 503),
    (1, "K2", "b1", 2, 4, 504),
    (7, "failover", "b1", 3, 5, 505),
    (8, "failover", "b2", 2, 6, 506),
    (9, "failover", "b3", 2, 7, 507),
    (13, "held-platform", "b1", 4, 8, 508),
    (14, "relay-crash", "b2", 3, 9, 509),
    (15, "ack-crash", "b3", 3, 10, 510),
    (10, "bundle", "b1", 5, 11, 511),
    (16, "during-migration", "b1", 6, 12, 512),
    (22, "regional", "b1", 7, 13, 513),
]
STAGES = [
    ("01-baseline", 3),
    ("02-identity", 4),
    ("03-failover-all-groups", 7),
    ("04-platform-inflight", 8),
    ("05-real-message-crashes", 10),
    ("07-cross-partition-complete", 11),
    ("08-migration-complete", 12),
    ("09-logical-region-healed", 13),
    ("final-state", 13),
]


def encode(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def expected(row, handoff=False):
    account, key, bucket, local, effect, f = row
    suffix = f"{bucket}:{local}"
    s = {"account_id": account, "action": ACTION, "session_id": "E1", "idempotency_key": key}
    p, c, t, o, ev = [prefix + suffix for prefix in ("P:", "C:", "T:", "O:", "EV:")]
    inp = dict(INPUT, **({"bundle": True} if key == "bundle" else {}))
    acceptance = {
        "purchase_id": p,
        "charge_id": c,
        "operation": s,
        "product_id": "paper-001",
        "quantity": 1,
        "quote_id": "Q7",
        "currency": "CNY",
        "amount_minor": 3000,
    }
    pi = {"account_id": account, "charge_id": c, "currency": "CNY", "amount_minor": 3000}
    provider = {
        "status": "succeeded",
        "scope": SCOPE,
        "key": "pay-" + c,
        "currency": "CNY",
        "amount_minor": 3000,
        "account_id": account,
        "charge_id": c,
        "provider_charge_id": f"X{effect}",
    }
    payment = {
        "purchase_id": p,
        "charge_id": c,
        "order_id": o,
        "status": "succeeded",
        "provider_scope": SCOPE,
        "provider_charge_id": f"X{effect}",
        "currency": "CNY",
        "amount_minor": 3000,
    }
    payload = {
        "purchase_id": p,
        "charge_id": c,
        "order_id": o,
        "provider_charge_id": f"X{effect}",
        "currency": "CNY",
        "amount_minor": 3000,
    }
    event = {
        "event_id": ev,
        "event_type": "purchase.paid",
        "event_version": 1,
        "occurred_at": "2026-10-18T01:00:00Z",
        "payload": payload,
    }
    result = {
        "result_version": "purchase-result-external-v2",
        "acceptance": acceptance,
        "progress": {"business_state": "paid", "fulfillment_state": "unfulfilled"},
        "payment_result": payment,
    }
    operation = {
        "s": s,
        "input": inp,
        "input_version": "external-v2",
        "p": p,
        "c": c,
        "t": t,
        "scope": SCOPE,
        "key": "pay-" + c,
        "payment_input": pi,
        "acceptance": acceptance,
        "provider_result": provider,
        "payment_result": payment,
        "order_id": o,
        "event_id": ev,
        "event": event,
        "result": result,
        "bundle": key == "bundle",
        "reservation_ready": True,
        "work_state": "done",
    }
    if handoff:
        operation["current_progress"] = {
            "business_state": "paid",
            "fulfillment_state": "fulfillment_accepted",
        }
        operation["consumer_receipt"] = {
            "event_id": ev,
            "order_id": o,
            "fulfillment_id": f,
            "status": "fulfillment_accepted",
        }
        operation["consumer_source"] = {
            "domain": "fulfillment-primary",
            "record_id": ev,
            "event": event,
            "business_guard": "initial:" + o,
            "dispatch_key": "dispatch:initial:" + o,
            "dispatch_input": {"fulfillment_id": f, "order_id": o, "kind": "initial"},
        }
        operation["handoff_confirmed"] = True
    return {
        "bucket": bucket,
        "s_key": encode(s),
        "operation": operation,
        "effect": effect,
        "f": f,
        "platform_operation": {
            "scope": SCOPE,
            "key": "pay-" + c,
            "kind": "charge",
            "input": encode(pi),
            "status": "succeeded",
            "result": encode(provider),
        },
        "platform_effect": {
            "effect_id": effect,
            "scope": SCOPE,
            "key": "pay-" + c,
            "account_id": account,
            "kind": "charge",
            "amount": 3000,
            "result": encode(provider),
        },
        "fulfillment": {"f": f, "order_id": o, "payload": encode(payload)},
        "guard": {"b": "initial:" + o, "f": f},
        "inbox": {
            "event_id": ev,
            "event": encode(event),
            "f": f,
            "receipt": encode(
                {
                    "event_id": ev,
                    "order_id": o,
                    "fulfillment_id": f,
                    "status": "fulfillment_accepted",
                }
            ),
        },
        "dispatch": {
            "key": "dispatch:initial:" + o,
            "f": f,
            "body": encode({"fulfillment_id": f, "order_id": o, "kind": "initial"}),
            "result": encode(
                {
                    "status": "accepted",
                    "dispatch_key": "dispatch:initial:" + o,
                    "fulfillment_id": f,
                    "provider_dispatch_id": "DSP-" + str(f),
                }
            ),
        },
    }


# Serialization field order of the documented wire snapshot; map keys are sorted.
# This is needed because the Go checksum covers typed JSON, not sorted-key JSON.
WIRE_FIELDS = {
    "bucket": "id mode epoch next migration sessions operations refunds stock prepared transactions".split(),
    "session": "account_id action deadline closed".split(),
    "operation": (
        "s input input_version p c t scope key payment_input acceptance result provider_result payment_result "
        "order_id event_id event owner work_epoch work_state published bundle reservation_ready "
        "consumer_receipt next_delivery_attempt consumer_source handoff_confirmed current_progress"
    ).split(),
    "refund": "s input rf source_c scope key provider_input status provider_result result".split(),
    "prepared": "id coordinator input vote state".split(),
    "transaction": "id s_key participants votes decision done deadline_ms".split(),
    "participant": "bucket sku qty".split(),
}


def wire(value, kind=None):
    if not isinstance(value, (dict, list)):
        return value
    if isinstance(value, list):
        return [wire(v, "participant" if kind == "participants" else None) for v in value]
    fields = WIRE_FIELDS[kind] if kind in WIRE_FIELDS else sorted(value)
    child_kinds = {
        "sessions": "session",
        "operations": "operation",
        "refunds": "refund",
        "prepared": "prepared",
        "transactions": "transaction",
    }
    result = {}
    for key in fields:
        if (
            kind == "operation"
            and key
            in (
                "consumer_receipt",
                "next_delivery_attempt",
                "consumer_source",
                "handoff_confirmed",
                "current_progress",
            )
            and key not in value
        ):
            continue
        if kind == "transaction" and key == "deadline_ms" and key not in value:
            continue
        val = value[key]
        if kind == "bucket" and key in child_kinds:
            result[key] = {k: wire(v, child_kinds[key]) for k, v in sorted(val.items())}
        else:
            result[key] = wire(val, "participants" if key == "participants" else None)
    return result
