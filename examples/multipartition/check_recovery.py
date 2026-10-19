"""Independent recovery/lifecycle checker; imports only specification/checker code."""

import argparse
import collections
import hashlib
import json
from pathlib import Path

from check_run import Checks, active, lines, load, sql_snapshot
from expected_spec import ACTION, encode, expected
from ownership_invariants import violations

ROWS = [
    (1, "K1", "b1", 1, 1, 501),
    (2, "K1", "b2", 1, 2, 502),
    (3, "K1", "b3", 1, 3, 503),
    (4, "close-pending", "b1", 2, 4, 504),
    (6, "local-availability", "b3", 2, 5, 505),
    (7, "bundle", "b1", 3, 6, 506),
]
REFUND_SCOPE = "simulator:merchant-7:test:refund.v1"


def refund(local, client, amount, effect, pending=False):
    s = {
        "account_id": 1,
        "action": "refund.create.v1",
        "session_id": "RF-E1",
        "idempotency_key": client,
    }
    rf = f"RF:b1:{local}"
    key = "refund-" + rf
    inp = {"source_c": "C:b1:1", "currency": "CNY", "amount_minor": amount}
    pi = {
        "account_id": 1,
        "refund_id": rf,
        "source_c": "C:b1:1",
        "source_x": "X1",
        "currency": "CNY",
        "amount_minor": amount,
    }
    provider = {
        "status": "succeeded",
        "scope": REFUND_SCOPE,
        "key": key,
        "currency": "CNY",
        "amount_minor": amount,
        "refund_id": rf,
        "source_charge_id": "C:b1:1",
        "source_provider_charge_id": "X1",
        "provider_refund_id": f"PX-RF{effect}",
    }
    result = {
        "refund_id": rf,
        "status": "succeeded",
        "source_charge_id": "C:b1:1",
        "provider_result": provider,
    }
    return {
        "s": s,
        "input": inp,
        "rf": rf,
        "source_c": "C:b1:1",
        "scope": REFUND_SCOPE,
        "key": key,
        "provider_input": pi,
        "status": "pending" if pending else "succeeded",
        "provider_result": None if pending else provider,
        "result": None if pending else result,
    }


def ledger_want(count, refunds):
    rows = [expected(r) for r in ROWS[:count]]
    balances = collections.Counter(r[0] for r in ROWS[:count])
    balance = {a: 10000 - 3000 * n for a, n in balances.items()}
    operations = [r["platform_operation"] for r in rows]
    effects = [r["platform_effect"] for r in rows]
    for effect, rf in refunds:
        operations.append(
            {
                "scope": rf["scope"],
                "key": rf["key"],
                "kind": "refund",
                "input": encode(rf["provider_input"]),
                "status": rf["status"],
                "result": None if rf["provider_result"] is None else encode(rf["provider_result"]),
            }
        )
        if rf["status"] == "succeeded":
            amount = rf["input"]["amount_minor"]
            balance[1] += amount
            effects.append(
                {
                    "effect_id": effect,
                    "scope": rf["scope"],
                    "key": rf["key"],
                    "account_id": 1,
                    "kind": "refund",
                    "amount": amount,
                    "result": encode(rf["provider_result"]),
                }
            )
    platform = {
        "accounts": [{"account_id": a, "balance": b} for a, b in sorted(balance.items())],
        "operations": operations,
        "effects": effects,
    }
    consumer = {
        k: [r[f] for r in rows]
        for k, f in [
            ("fulfillments", "fulfillment"),
            ("business_guard", "guard"),
            ("inbox", "inbox"),
            ("dispatch", "dispatch"),
        ]
    }
    return platform, consumer


def check(run):
    c = Checks()
    c.equal(
        "runner/status",
        load(run / "run-status.json"),
        {"completed": True, "scope": "recovery-and-lifecycle"},
    )
    before = load(run / "03-before-full-snapshot-restart.json")
    after = load(run / "04-after-full-snapshot-restart.json")
    for group in before["groups"]:
        for field in ("buckets", "routes"):
            c.equal(
                "snapshot/full-domain/" + group + "/" + field,
                after["groups"][group][field],
                before["groups"][group][field],
            )
    c.equal("snapshot/platform-is-separate-domain", after["platform"], before["platform"])
    c.equal("snapshot/consumer-is-separate-domain", after["consumer"], before["consumer"])
    txid = "TX:P:b1:3"
    bundle = expected(ROWS[5])
    tx = {
        "id": txid,
        "s_key": bundle["s_key"],
        "participants": [
            {"bucket": "b2", "sku": "SKU-A", "qty": 1},
            {"bucket": "b3", "sku": "SKU-B", "qty": 1},
        ],
        "votes": {"b2": "yes", "b3": "yes"},
        "decision": "",
        "done": False,
    }
    for name, snapshot in (("before", before), ("after", after)):
        b = active(snapshot)
        c.equal("snapshot/" + name + "/transaction", b["b1"]["transactions"], {txid: tx})
        o = b["b1"]["operations"][bundle["s_key"]]
        c.equal(
            "snapshot/" + name + "/blocked-payment",
            [o["reservation_ready"], o["result"], o["provider_result"], o["event"]],
            [False, None, None, None],
        )
        for bid, sku in (("b2", "SKU-A"), ("b3", "SKU-B")):
            c.equal(
                "snapshot/" + name + "/" + bid + "/prepared",
                b[bid]["prepared"],
                {
                    txid + ":" + bid: {
                        "id": txid + ":" + bid,
                        "coordinator": "b1",
                        "input": {"sku": sku, "qty": 1},
                        "vote": "yes",
                        "state": "prepared",
                    }
                },
            )
            c.equal("snapshot/" + name + "/" + bid + "/stock", b[bid]["stock"], {sku: 3})
    for node in [f"{g}-{j}" for g in ("control", "G1", "G2", "G3") for j in range(3)]:
        events = lines(run / node / "events.jsonl")
        persisted = [e for e in events if e["event"] == "snapshot_persisted"]
        restored = [e for e in events if e["event"] == "fsm_restored"]
        c.true("snapshot/" + node + "/real-persist-and-restore", persisted and restored)
        if persisted and restored:
            c.true("snapshot/" + node + "/new-process", persisted[-1]["pid"] != restored[-1]["pid"])
            c.equal(
                "snapshot/" + node + "/same-image-sha",
                restored[-1]["sha256"],
                persisted[-1]["sha256"],
            )
        files = list((run / node / "snapshots").glob("*/state.bin"))
        c.true("snapshot/" + node + "/snapshot-file", bool(files))
        if files:
            path = max(files, key=lambda p: p.stat().st_mtime_ns)
            raw = path.read_bytes()
            s = json.loads(raw)
            group = node.rsplit("-", 1)[0]
            c.equal(
                "snapshot/" + node + "/persisted-domain-buckets",
                s["buckets"],
                before["groups"][group]["buckets"],
            )
            c.equal(
                "snapshot/" + node + "/persisted-domain-routes",
                s["routes"],
                before["groups"][group]["routes"],
            )
            c.true(
                "snapshot/" + node + "/file-hash-observed",
                any(e["sha256"] == hashlib.sha256(raw).hexdigest() for e in persisted),
            )
    stages = [
        ("01-session-lifecycle", 4, []),
        ("02-local-availability", 5, []),
        ("05-transaction-recovered", 6, []),
        ("06-refund-unknown-capacity", 6, [(7, refund(4, "RF-K1", 2000, 7, pending=True))]),
        (
            "07-refunds-recovered",
            6,
            [(7, refund(4, "RF-K1", 2000, 7)), (8, refund(5, "RF-K2", 1000, 8))],
        ),
        ("final-state", 6, [(7, refund(4, "RF-K1", 2000, 7)), (8, refund(5, "RF-K2", 1000, 8))]),
    ]
    for name, count, rfs in stages:
        s = load(run / (name + ".json"))
        buckets = active(s)
        c.equal(name + "/ownership", violations(s), [])
        c.equal(
            name + "/operation-identity-set",
            sorted(k for b in buckets.values() for k in b["operations"]),
            sorted(expected(r)["s_key"] for r in ROWS[:count]),
        )
        for row in ROWS[:count]:
            want = expected(row)
            actual = buckets[want["bucket"]]["operations"][want["s_key"]]
            c.equal(
                name + "/" + want["operation"]["c"] + "/business-fields",
                {k: actual[k] for k in want["operation"]},
                want["operation"],
            )
        p, consumer = ledger_want(count, rfs)
        c.equal(name + "/complete-platform-ledger", s["platform"], p)
        c.equal(name + "/complete-consumer-ledger", s["consumer"], consumer)
        want_refunds = {encode(rf["s"]): rf for _, rf in rfs}
        c.equal(name + "/complete-refund-bindings", buckets["b1"]["refunds"], want_refunds)
        for bid, b in buckets.items():
            sessions = {
                f"{a}/{ACTION}/E1": {
                    "account_id": a,
                    "action": ACTION,
                    "deadline": 1000,
                    "closed": a == 4,
                }
                for a in range(int(bid[1:]), 25, 3)
            }
            if bid == "b1":
                sessions["1/refund.create.v1/RF-E1"] = {
                    "account_id": 1,
                    "action": "refund.create.v1",
                    "deadline": 1000,
                    "closed": False,
                }
            c.equal(name + "/" + bid + "/fixed-session-lifecycle", b["sessions"], sessions)
    final = load(run / "final-state.json")
    terminal = active(final)
    c.equal(
        "transaction/recovered-commit-decision",
        terminal["b1"]["transactions"],
        {txid: dict(tx, decision="commit", done=True)},
    )
    for bid, sku in (("b2", "SKU-A"), ("b3", "SKU-B")):
        c.equal(
            "transaction/recovered-" + bid + "/committed",
            terminal[bid]["prepared"],
            {
                txid + ":" + bid: {
                    "id": txid + ":" + bid,
                    "coordinator": "b1",
                    "input": {"sku": sku, "qty": 1},
                    "vote": "yes",
                    "state": "committed",
                }
            },
        )
        c.equal(
            "transaction/recovered-" + bid + "/stock-conservation", terminal[bid]["stock"], {sku: 3}
        )
    c.equal("sqlite/platform-complete", sql_snapshot(run / "platform.sqlite"), final["platform"])
    c.equal("sqlite/consumer-complete", sql_snapshot(run / "consumer.sqlite"), final["consumer"])
    h = lines(run / "history.jsonl")
    c.equal("history/collector-order", [v["seq"] for v in h], list(range(1, len(h) + 1)))
    down = next(v["seq"] for v in h if v["event"] == "groups_fully_unavailable")
    done = next(v for v in h if v["event"] == "unaffected_partition_completed")
    for group in ("G2", "control"):
        for j in range(3):
            name = f"{group}-{j}"
            stopped = [
                v["seq"]
                for v in h
                if v["event"] == "process_exit" and v["name"] == name and v["seq"] < down
            ]
            restarted = [
                v["seq"]
                for v in h
                if v["event"] == "process_start" and v["name"] == name and v["seq"] > down
            ]
            c.true(
                "availability/" + name + "/stopped-during-other-completion",
                stopped and restarted and max(stopped) < down < done["seq"] < min(restarted),
            )
    c.equal(
        "availability/exact-original-result",
        done["original_result"],
        expected(ROWS[4])["operation"]["result"],
    )
    commands = [
        e
        for p in run.glob("*/events.jsonl")
        for e in lines(p)
        if e["event"] == "committed_response"
    ]
    for kind, code in (
        ("accept", "operation_expired"),
        ("session", "session_conflict"),
        ("refund_accept", "refund_capacity_exceeded"),
        ("refund_accept", "conflict"),
    ):
        c.true(
            "lifecycle/rejection/" + kind + "/" + code,
            any(e["command"]["kind"] == kind and e["result"].get("code") == code for e in commands),
        )
    c.true(
        "refund/worker-really-killed",
        any(v["event"] == "process_exit" and v["name"] == "refund-before-save" for v in h),
    )
    paused = [
        e
        for e in lines(run / "refund-before-save.jsonl")
        if e["event"] == "refund_provider_returned"
    ]
    recovered = [
        e
        for e in lines(run / "refund-after-crash.jsonl")
        if e["event"] == "refund_provider_returned"
    ]
    c.true(
        "refund/same-provider-result-after-crash",
        any(
            a["result"] == b["result"] and a["s_key"] == b["s_key"]
            for a in paused
            for b in recovered
        ),
    )
    c.equal(
        "cleanup/exited",
        all(
            e.get("returncode") is not None and "error" not in e for e in load(run / "cleanup.json")
        ),
        True,
    )
    manifest = load(run / "source-manifest.json")
    for e in manifest["files"]:
        path = run / ("source-freeze/" + e["path"] if "path" in e else e["run_file"])
        c.equal(
            "source/" + str(path.relative_to(run)),
            hashlib.sha256(path.read_bytes()).hexdigest(),
            e["sha256"],
        )
    return {
        "passed": all(e["passed"] for e in c.results),
        "checks": len(c.results),
        "results": c.results,
        "scope": "finite real-process lifecycle/quorum-loss/snapshot/refund history; controlled local platform",
        "limitations": [
            "Refunds and purchases use this lab platform, not a commercial provider.",
            "Snapshot restoration is process fail-stop with surviving local files, not disk destruction.",
            "All logical replicas run on one Windows host.",
            "Checks are independent finite evidence checks, not an exhaustive state-space proof.",
        ],
    }


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--run-dir", required=True)
    run = Path(p.parse_args().run_dir).resolve()
    report = check(run)
    report["checker_sha256"] = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    (run / "independent-check.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(encode({k: v for k, v in report.items() if k != "results"}))
    for e in report["results"]:
        if not e["passed"]:
            print("FAIL", encode(e))
    raise SystemExit(0 if report["passed"] else 1)
