"""Native DBOS recovery orchestrating the unchanged complete Raft authority."""

import argparse
import hashlib
import json
import time
from pathlib import Path

from common import Cluster, Trace, canon, http, identity, purchase_input
from dbos import DBOS, SetWorkflowID

ap = argparse.ArgumentParser()
ap.add_argument("--run", required=True)
ap.add_argument("--mode", choices=["start", "recover"], required=True)
args = ap.parse_args()
R = Path(args.run)
cfg = json.loads((R / "config.json").read_text(encoding="utf-8"))
trace = Trace(R / ("dbos-" + args.mode + ".jsonl"))
cl = Cluster(cfg)
cl.refresh()
S = identity(1, "dbos-full-authority")
INPUT = purchase_input()
WF = "purchase:" + hashlib.sha256(canon(S).encode()).hexdigest()
OWNER = "dbos:" + args.mode
FIELDS = (
    "s",
    "input",
    "input_version",
    "p",
    "c",
    "t",
    "scope",
    "key",
    "payment_input",
    "acceptance",
)


def bind(s, inp):
    ans = cl.command("b1", "accept", s=s, input=inp, input_version="external-v2", now_upper=1)
    if ans.get("code") == "conflict":
        raise ValueError("business_input_conflict")
    op = cl.bucket("b1")["operations"][canon(s)]
    if op["input"] != inp:
        raise ValueError("business_input_conflict")
    return {k: op[k] for k in FIELDS}


@DBOS.step()
def bind_step(s, inp):
    trace("step_body", name="bind", s=s, input=inp)
    return bind(s, inp)


@DBOS.step(retries_allowed=True, interval_seconds=0.1, max_attempts=3)
def payment_step(binding):
    trace("step_body", name="payment", binding=binding)
    claim = cl.command("b1", "claim", s_key=canon(binding["s"]), owner=OWNER)
    if "c" not in claim:
        raise ValueError("claim failed")
    pr = http(
        cfg["platform"] + "/apply",
        {
            "scope": binding["scope"],
            "key": binding["key"],
            "kind": "charge",
            "input": binding["payment_input"],
        },
    )
    trace("platform_reply", result=pr, claim=claim)
    if args.mode == "start":
        (R / "effect-before-step-checkpoint.json").write_text(
            canon({"workflow_id": WF, "binding": binding, "provider_result": pr, "claim": claim}),
            encoding="utf-8",
        )
        while True:
            time.sleep(0.1)
    return {"provider_result": pr, "owner": OWNER, "work_epoch": claim["work_epoch"]}


@DBOS.step()
def finish_step(binding, payment):
    trace("step_body", name="finish")
    ans = cl.command(
        "b1",
        "observe",
        s_key=canon(binding["s"]),
        owner=payment["owner"],
        work_epoch=payment["work_epoch"],
        provider_result=payment["provider_result"],
    )
    op = cl.bucket("b1")["operations"][canon(binding["s"])]
    if not op["result"]:
        raise ValueError("result not committed: " + canon(ans))
    if {k: op[k] for k in FIELDS} != binding:
        raise ValueError("binding changed")
    return {
        "binding": binding,
        "provider_result": op["provider_result"],
        "result": op["result"],
        "event": op["event"],
    }


@DBOS.workflow()
def purchase_workflow(s, inp):
    binding = bind_step(s, inp)
    payment = payment_step(binding)
    return finish_step(binding, payment)


DBOS(
    config={
        "name": "chapter21-durable-authority",
        "application_version": "ch21-full-1",
        "system_database_url": "sqlite:///"
        + str((R / "dbos-system.sqlite").resolve()).replace("\\", "/"),
        "enable_otlp": False,
        "run_admin_server": False,
    }
)
DBOS.launch()
if args.mode == "start":
    with SetWorkflowID(WF):
        DBOS.start_workflow(purchase_workflow, S, INPUT).get_result()
else:
    result = DBOS.retrieve_workflow(WF).get_result(polling_interval_sec=0.1)
    with SetWorkflowID(WF):
        repeat = purchase_workflow(S, INPUT)
    conflict = None
    try:
        bind(S, {**INPUT, "amount_minor": 3500})
    except ValueError as e:
        conflict = str(e)
    with SetWorkflowID(WF):
        raw_reuse = purchase_workflow(S, {**INPUT, "amount_minor": 3500})
    (R / "dbos-result.json").write_text(
        canon(
            {
                "workflow_id": WF,
                "result": result,
                "repeat": repeat,
                "business_input_conflict": conflict,
                "raw_id_reuse_returns_existing": raw_reuse,
            }
        ),
        encoding="utf-8",
    )
    DBOS.destroy()
