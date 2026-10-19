"""DBOS 3.3.0 entry: complete binding plus an independently durable HTTP effect."""

import argparse
import hashlib
import json
import time
from pathlib import Path

from common import Trace, canon, http
from services import database

ap = argparse.ArgumentParser()
ap.add_argument("--run", required=True)
ap.add_argument("--mode", choices=["start", "recover"], required=True)
args = ap.parse_args()
R = Path(args.run)
cfg = json.loads((R / "config.json").read_text(encoding="utf-8"))
trace = Trace(R / ("dbos-" + args.mode + ".jsonl"))
from dbos import DBOS, SetWorkflowID

S = {
    "account_id": 1,
    "action": "purchase.create.external.v2",
    "session_id": "E1",
    "idempotency_key": "durable-001",
}
INPUT = {
    "product_id": "paper-001",
    "quantity": 1,
    "quote_id": "Q7",
    "currency": "CNY",
    "amount_minor": 3000,
    "payment_method": "saved-card-7",
}
WF = "purchase:" + hashlib.sha256(canon(S).encode()).hexdigest()


def connect():
    c = database(R / "business.sqlite")
    c.execute(
        "CREATE TABLE IF NOT EXISTS operations(s TEXT PRIMARY KEY,input TEXT NOT NULL,binding TEXT NOT NULL,result TEXT)"
    )
    return c


def ensure_binding(s, inp):
    c = connect()
    c.execute("BEGIN IMMEDIATE")
    old = c.execute("SELECT * FROM operations WHERE s=?", (canon(s),)).fetchone()
    if old:
        c.execute("COMMIT")
        c.close()
        if old["input"] != canon(inp):
            raise ValueError("business_input_conflict")
        return json.loads(old["binding"])
    b = {
        "s": s,
        "input": inp,
        "input_version": "external-v2",
        "p": "b1-P1",
        "c": "b1-C1",
        "t": "b1-T1",
        "scope": "purchase-v2",
        "key": "pay-b1-C1",
        "payment_input": {
            "account_id": s["account_id"],
            "charge_id": "b1-C1",
            "amount_minor": inp["amount_minor"],
            "currency": inp["currency"],
            "payment_method": inp["payment_method"],
        },
    }
    c.execute("INSERT INTO operations VALUES(?,?,?,NULL)", (canon(s), canon(inp), canon(b)))
    c.execute("COMMIT")
    c.close()
    return b


@DBOS.step()
def bind_step(s, inp):
    trace("step_body", name="bind", s=s, input=inp)
    return ensure_binding(s, inp)


@DBOS.step(retries_allowed=True, interval_seconds=0.1, max_attempts=3)
def payment_step(binding):
    trace("step_body", name="payment", binding=binding)
    result = http(
        cfg["platform"] + "/apply",
        {
            "scope": binding["scope"],
            "key": binding["key"],
            "kind": "charge",
            "input": binding["payment_input"],
        },
    )
    trace("platform_reply", result=result)
    if args.mode == "start":
        (R / "effect-before-step-checkpoint.json").write_text(
            canon({"binding": binding, "platform_result": result, "workflow_id": WF}),
            encoding="utf-8",
        )
        # Parent kills this actual OS process while the DBOS step has not returned.
        while True:
            time.sleep(0.1)
    return result


@DBOS.step()
def finish_step(binding, platform_result):
    trace("step_body", name="finish")
    expected = {
        "status": "succeeded",
        "scope": binding["scope"],
        "key": binding["key"],
        "currency": "CNY",
        "amount_minor": 3000,
        "account_id": 1,
        "charge_id": "b1-C1",
        "provider_charge_id": "X1",
    }
    if platform_result != expected:
        raise ValueError("full_platform_result_mismatch")
    result = {
        "status": "paid",
        "purchase_id": binding["p"],
        "charge_id": binding["c"],
        "order_id": "b1-O1",
        "currency": "CNY",
        "amount_minor": 3000,
        "payment": {"provider": "labpay", "provider_charge_id": "X1", "status": "succeeded"},
    }
    c = connect()
    c.execute("BEGIN IMMEDIATE")
    old = c.execute("SELECT result FROM operations WHERE s=?", (canon(binding["s"]),)).fetchone()
    if old["result"] and old["result"] != canon(result):
        raise ValueError("immutable_result_conflict")
    c.execute("UPDATE operations SET result=? WHERE s=?", (canon(result), canon(binding["s"])))
    c.execute("COMMIT")
    c.close()
    return {
        "s": binding["s"],
        "input": binding["input"],
        "p": binding["p"],
        "c": binding["c"],
        "t": binding["t"],
        "scope": binding["scope"],
        "key": binding["key"],
        "platform_result": platform_result,
        "result": result,
    }


@DBOS.workflow()
def purchase_workflow(s, inp):
    b = bind_step(s, inp)
    p = payment_step(b)
    return finish_step(b, p)


DBOS(
    config={
        "name": "chapter21-durable",
        "application_version": "ch21-1",
        "system_database_url": "sqlite:///"
        + str((R / "dbos-system.sqlite").resolve()).replace("\\", "/"),
        "enable_otlp": False,
        "run_admin_server": False,
    }
)
DBOS.launch()
if args.mode == "start":
    ensure_binding(S, INPUT)
    with SetWorkflowID(WF):
        handle = DBOS.start_workflow(purchase_workflow, S, INPUT)
    handle.get_result()
else:
    result = DBOS.retrieve_workflow(WF).get_result(polling_interval_sec=0.1)
    with SetWorkflowID(WF):
        repeated = purchase_workflow(S, INPUT)
    changed = {**INPUT, "amount_minor": 3500}
    conflict = None
    try:
        ensure_binding(S, changed)
    except ValueError as e:
        conflict = str(e)
    # Diagnostic only: DBOS ID reuse returns the existing execution. Business
    # argument comparison belongs in the application's binding boundary.
    with SetWorkflowID(WF):
        raw_reuse = purchase_workflow(S, changed)
    (R / "result.json").write_text(
        canon(
            {
                "workflow_id": WF,
                "result": result,
                "repeat": repeated,
                "input_conflict": conflict,
                "raw_id_reuse_returns_existing": raw_reuse,
            }
        ),
        encoding="utf-8",
    )
    DBOS.destroy()
