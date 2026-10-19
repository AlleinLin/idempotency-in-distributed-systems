"""Read-only independent evidence checker. Never imports business implementation.

Checks the specified finite execution and recorded invocation/response intervals.
It is not a general linearizability proof, does not order independent clocks, and
keeps response-unknown invocations unresolved unless durable evidence binds them.
"""

import argparse
import collections
import hashlib
import json
import sqlite3
from pathlib import Path

from expected_spec import ACTION, INPUT, ROWS, SCOPE, STAGES, encode, expected, wire
from ownership_invariants import violations


class Checks:
    def __init__(self):
        self.results = []

    def equal(self, label, actual, want):
        ok = actual == want
        self.results.append(
            {"check": label, "passed": ok, **({} if ok else {"actual": actual, "expected": want})}
        )
        return ok

    def true(self, label, value):
        return self.equal(label, bool(value), True)


def load(path):
    return json.loads(path.read_text(encoding="utf-8"))


def lines(path):
    return [
        json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()
    ]


def active(snapshot):
    return {
        bid: b
        for g in snapshot["groups"].values()
        for bid, b in g["buckets"].items()
        if b["mode"] == "active"
    }


def sql_snapshot(path):
    c = sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)
    c.row_factory = sqlite3.Row
    names = [
        r[0]
        for r in c.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
        )
    ]
    result = {n: [dict(r) for r in c.execute(f'SELECT * FROM "{n}" ORDER BY rowid')] for n in names}
    c.close()
    return result


def ledger_check(checks, prefix, snapshot, count):
    rows = [expected(r) for r in ROWS[:count]]
    balances = collections.Counter(r[0] for r in ROWS[:count])
    want_platform = {
        "accounts": [
            {"account_id": a, "balance": 10000 - 3000 * n} for a, n in sorted(balances.items())
        ],
        "operations": [r["platform_operation"] for r in rows],
        "effects": [r["platform_effect"] for r in rows],
    }
    want_consumer = {
        k: [r[field] for r in rows]
        for k, field in [
            ("fulfillments", "fulfillment"),
            ("business_guard", "guard"),
            ("inbox", "inbox"),
            ("dispatch", "dispatch"),
        ]
    }
    checks.equal(prefix + "/platform-all-fields", snapshot["platform"], want_platform)
    checks.equal(prefix + "/consumer-all-fields", snapshot["consumer"], want_consumer)


def check(run):
    c = Checks()
    c.equal("runner/completed", load(run / "run-status.json"), {"completed": True})
    cleanup = load(run / "cleanup.json")
    c.true(
        "cleanup/all-recorded-processes-exited",
        cleanup and all(x.get("returncode") is not None and "error" not in x for x in cleanup),
    )
    for stage, count in STAGES:
        s = load(run / (stage + ".json"))
        buckets = active(s)
        c.equal(stage + "/global-ownership-and-binding-invariants", violations(s), [])
        c.equal(stage + "/three-logical-buckets", sorted(buckets), ["b1", "b2", "b3"])
        rows = [expected(r, handoff=True) for r in ROWS[:count]]
        c.equal(
            stage + "/exact-identity-set",
            sorted(k for b in buckets.values() for k in b["operations"]),
            sorted(r["s_key"] for r in rows),
        )
        for row in rows:
            actual = buckets[row["bucket"]]["operations"][row["s_key"]]
            c.equal(
                stage + "/" + row["operation"]["c"] + "/all-business-fields",
                {k: actual.get(k) for k in row["operation"]},
                row["operation"],
            )
            c.true(
                stage + "/" + row["operation"]["c"] + "/work-owner",
                actual["owner"].startswith("pay:") and actual["work_epoch"] >= 1,
            )
        ledger_check(c, stage, s, count)
        for bid, b in buckets.items():
            accounts = range(int(bid[1:]), 25, 3)
            sessions = {
                f"{a}/{ACTION}/E1": {
                    "account_id": a,
                    "action": ACTION,
                    "deadline": 1000,
                    "closed": False,
                }
                for a in accounts
            }
            if bid == "b1":
                sessions["1/refund.create.v1/RF-E1"] = {
                    "account_id": 1,
                    "action": "refund.create.v1",
                    "deadline": 1000,
                    "closed": False,
                }
            c.equal(stage + "/" + bid + "/sessions", b["sessions"], sessions)
            c.equal(stage + "/" + bid + "/no-unrequested-refunds", b["refunds"], {})
    final = load(run / "final-state.json")
    c.equal(
        "sqlite/platform-vs-collected-snapshot",
        sql_snapshot(run / "platform.sqlite"),
        final["platform"],
    )
    c.equal(
        "sqlite/consumer-vs-collected-snapshot",
        sql_snapshot(run / "consumer.sqlite"),
        final["consumer"],
    )
    payload = load(run / "migration-payload.json")
    frozen = payload["frozen"]["image"]
    raw = json.dumps(wire(frozen, "bucket"), ensure_ascii=False, separators=(",", ":")).encode()
    c.equal(
        "migration/typed-image-sha256", hashlib.sha256(raw).hexdigest(), payload["frozen"]["sha256"]
    )
    c.equal("migration/source-exactly-frozen-image", final["groups"]["G1"]["buckets"]["b1"], frozen)
    route = final["groups"]["control"]["routes"]
    c.equal(
        "migration/control-routes",
        route,
        {
            "b1": {
                "owner": "G2",
                "epoch": 2,
                "migration": "M-b1-1",
                "sha256": payload["frozen"]["sha256"],
            },
            "b2": {"owner": "G2", "epoch": 1, "migration": "", "sha256": ""},
            "b3": {"owner": "G3", "epoch": 1, "migration": "", "sha256": ""},
        },
    )
    target = final["groups"]["G2"]["buckets"]["b1"]
    c.equal(
        "migration/target-epoch-mode",
        [target["epoch"], target["mode"], target["migration"]],
        [2, "active", "M-b1-1"],
    )
    immutable = "s input input_version p c t scope key payment_input acceptance".split()
    for k, old in frozen["operations"].items():
        c.equal(
            "migration/stable-binding/" + old["c"],
            {f: target["operations"][k][f] for f in immutable},
            {f: old[f] for f in immutable},
        )
        if old["result"] is not None:
            c.equal(
                "migration/original-result/" + old["c"],
                target["operations"][k]["result"],
                old["result"],
            )
    # Full transaction and inventory facts at the crash boundary and final state.
    before = active(load(run / "06-commit-decision-before-notify.json"))
    txid = "TX:P:b1:5"
    want_tx = {
        "id": txid,
        "s_key": expected(ROWS[10])["s_key"],
        "participants": [
            {"bucket": "b2", "sku": "SKU-A", "qty": 1},
            {"bucket": "b3", "sku": "SKU-B", "qty": 1},
        ],
        "votes": {"b2": "yes", "b3": "yes"},
        "decision": "commit",
        "done": False,
    }
    c.equal(
        "transaction/durable-decision-before-notify", before["b1"]["transactions"], {txid: want_tx}
    )
    bundle = before["b1"]["operations"][expected(ROWS[10])["s_key"]]
    c.equal(
        "transaction/payment-before-finish",
        [bundle["reservation_ready"], bundle["result"]],
        [False, None],
    )
    c.equal(
        "transaction/no-platform-effect-before-finish",
        len(load(run / "06-commit-decision-before-notify.json")["platform"]["effects"]),
        10,
    )
    after = active(final)
    c.equal(
        "transaction/final-decision", after["b1"]["transactions"], {txid: dict(want_tx, done=True)}
    )
    for bid, sku in [("b2", "SKU-A"), ("b3", "SKU-B")]:
        c.equal(
            "transaction/" + bid + "/durable-prepared",
            before[bid]["prepared"],
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
        c.equal(
            "transaction/" + bid + "/committed",
            after[bid]["prepared"],
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
        c.equal("transaction/" + bid + "/inventory-conservation", after[bid]["stock"], {sku: 3})
    history = lines(run / "history.jsonl")
    c.equal(
        "history/single-collector-sequence",
        [h["seq"] for h in history],
        list(range(1, len(history) + 1)),
    )
    invoked = {}
    ended = set()
    unresolved = []
    transient_responses = []
    migration_start = min(
        (
            h["seq"]
            for h in history
            if h["event"] == "process_start" and h["name"] == "migration-freeze"
        ),
        default=next(h["seq"] for h in history if h["event"] == "migration_frozen"),
    )
    migration_end = next(h["seq"] for h in history if h["event"] == "migration_complete")
    expected_rows = {expected(r)["s_key"]: expected(r) for r in ROWS}
    for h in history:
        if h["event"] == "invocation":
            c.true("history/unique-invocation/" + h["id"], h["id"] not in invoked)
            invoked[h["id"]] = h
        elif h["event"] in ("response", "unknown_response"):
            i = invoked.get(h["id"])
            c.true(
                "history/response-has-earlier-invocation/" + h["id"],
                i is not None and i["seq"] < h["seq"] and h["id"] not in ended,
            )
            ended.add(h["id"])
            if h["event"] == "unknown_response":
                unresolved.append(h["id"])
                continue
            row = expected_rows.get(encode(i["s"]))
            c.true("history/expected-request-identity/" + h["id"], row is not None)
            if row:
                want = {"code": "conflict"} if i["input"] != row["operation"]["input"] else None
                transitional = (
                    i["s"] == expected(ROWS[11])["operation"]["s"]
                    and i["input"] == INPUT
                    and migration_start < h["seq"] < migration_end
                    and h["result"]
                    in (
                        {"code": "not_active", "bucket": "b1", "epoch": 1, "mode": "frozen"},
                        {"code": "not_active", "bucket": "b1", "epoch": 2, "mode": "inactive"},
                    )
                )
                if transitional:
                    transient_responses.append(
                        {
                            "id": h["id"],
                            "invocation_seq": i["seq"],
                            "response_seq": h["seq"],
                            "result": h["result"],
                        }
                    )
                c.true(
                    "history/exact-response/" + h["id"],
                    h["result"] == want
                    if want
                    else transitional
                    or h["result"] in (row["operation"]["acceptance"], row["operation"]["result"]),
                )
    unresolved.extend(set(invoked) - ended)
    c.equal(
        "history/specified-group-failovers",
        sorted(h["group"] for h in history if h["event"] == "group_failover_complete"),
        ["G1", "G2", "G3"],
    )
    # Committed records expose per-group log indexes. Sampled term is NOT log term.
    committed = collections.defaultdict(dict)
    all_commands = []
    for p in sorted(run.glob("*/events.jsonl")):
        for h in lines(p):
            if h["event"] != "committed_response":
                continue
            group, index = h["group"], h["index"]
            value = {"command": h["command"], "result": h["result"]}
            if index in committed[group]:
                c.equal(
                    f"log/{group}/{index}/single-recorded-value", value, committed[group][index]
                )
            committed[group][index] = value
            c.true(
                f"log/{group}/{index}/index-and-sampled-term",
                index > 0 and int(h.get("current_term", h.get("term", 0))) > 0,
            )
            all_commands.append(h)
    for group in ("control", "G1", "G2", "G3"):
        c.true("log/" + group + "/committed-records-present", bool(committed[group]))
    c.true(
        "migration/freeze-blocked-by-prepared",
        any(
            h["command"]["kind"] == "freeze"
            and h["result"] == {"code": "prepared_blocks_migration"}
            for h in all_commands
        ),
    )
    c.true(
        "migration/frozen-close-rejected",
        any(
            h["command"]["kind"] == "close" and h["result"].get("code") == "not_active"
            for h in all_commands
        ),
    )
    # Stable external key across the killed worker and the in-flight old request.
    proxy = lines(run / "proxy.jsonl")
    held = [
        h
        for h in proxy
        if h["event"] == "proxy_forwarded"
        and (h.get("body") or {}).get("input", {}).get("account_id") == 13
    ]
    c.true("provider/old-and-recovered-real-requests", len(held) >= 2)
    c.equal(
        "provider/same-key-and-input-after-worker-death",
        sorted({encode(h["body"]) for h in held}),
        [
            encode(
                {
                    "scope": SCOPE,
                    "key": "pay-C:b1:4",
                    "kind": "charge",
                    "input": expected(ROWS[7])["operation"]["payment_input"],
                }
            )
        ],
    )
    c.true(
        "provider/actual-response-drop",
        any(
            h["event"] == "proxy_response_dropped" and h["body"]["input"]["account_id"] == 13
            for h in proxy
        ),
    )
    for name in (
        "pay-held",
        "relay-paused",
        "consume-paused",
        "coordinate-prepared",
        "coordinate-decided",
    ):
        c.true(
            "crash/" + name + "/process-exit-record",
            any(h["event"] == "process_exit" and h["name"] == name for h in history),
        )
    pub1 = [h for h in lines(run / "relay-paused.jsonl") if h["event"] == "publish_confirmed"]
    pub2 = [h for h in lines(run / "relay-recovered.jsonl") if h["event"] == "publish_confirmed"]
    config = load(run / "config.json")
    kind = config.get("broker", {}).get("kind", "nats")

    def separate_publish(a, b):
        if a["envelope"] != b["envelope"]:
            return False
        if kind == "nats":
            return a["sequence"] != b["sequence"]
        if kind == "kafka":
            return (
                (a["topic"], a["partition"], a["offset"])
                != (b["topic"], b["partition"], b["offset"])
                and a["confirmed"]
                and b["confirmed"]
            )
        return (
            a["publisher_channel"] != b["publisher_channel"]
            and a["confirmed"]
            and b["confirmed"]
            and a["mandatory"]
            and b["mandatory"]
        )

    c.true(
        "broker/republication-after-confirmed-publish",
        any(separate_publish(a, b) for a in pub1 for b in pub2),
    )
    con1 = [h for h in lines(run / "consume-paused.jsonl") if h["event"] == "consumer_committed"]
    con2 = [h for h in lines(run / "consume-recovered.jsonl") if h["event"] == "consumer_committed"]
    c.true(
        "consumer/redelivery-after-commit-before-ack",
        any(
            a["envelope"] == b["envelope"] and a["receipt"] == b["receipt"]
            for a in con1
            for b in con2
        ),
    )
    pairs = [
        (a, b)
        for a in con1
        for b in con2
        if a["envelope"] == b["envelope"] and a["receipt"] == b["receipt"]
    ]
    if kind == "nats":
        c.true(
            "consumer/second-delivery-observed",
            any(
                "num_delivered=2"
                in (
                    h["metadata"].get("raw", "")
                    if isinstance(h["metadata"], dict)
                    else h["metadata"]
                )
                for h in con2
            ),
        )
    elif kind == "rabbit":
        c.true(
            "consumer/redelivered-with-new-channel-scope",
            any(
                b["metadata"]["redelivered"]
                and a["metadata"]["channel_id"] != b["metadata"]["channel_id"]
                for a, b in pairs
            ),
        )
    else:
        c.true(
            "consumer/same-topic-partition-offset-after-crash",
            any(
                all(a["metadata"][k] == b["metadata"][k] for k in ("topic", "partition", "offset"))
                for a, b in pairs
            ),
        )
    for group in ("G1", "G2", "G3", "control"):
        faults = [
            h
            for h in lines(run / (group + "-0") / "events.jsonl")
            if h["event"] == "transport_fault"
        ]
        c.true(
            "transport/" + group + "/isolate-and-heal",
            any(h["config"].get("isolated") for h in faults)
            and any(h["config"].get("isolated") is False for h in faults),
        )
    migration_phases = ("freeze", "install", "directory", "activate")
    killed_phases = [h["phase"] for h in history if h["event"] == "migration_worker_killed"]
    if killed_phases:
        c.equal("migration/four-real-crash-boundaries", killed_phases, list(migration_phases))
        for phase in migration_phases:
            worker = "migration-" + phase
            marker = load(run / (worker + "-paused.json"))
            c.equal("migration/" + phase + "/checkpoint", marker["phase"], phase)
            c.true(
                "migration/" + phase + "/killed-pid",
                any(
                    h["event"] == "process_exit"
                    and h["name"] == worker
                    and h["pid"] == marker["pid"]
                    and h["returncode"] != 0
                    for h in history
                ),
            )
        c.true(
            "migration/final-recovery-completed",
            any(
                h["event"] == "phase_complete" and h["phase"] == "activate"
                for h in lines(run / "migration-recovery.jsonl")
            ),
        )
    manifest = load(run / "source-manifest.json")
    for m in manifest["files"]:
        path = run / ("source-freeze/" + m["path"] if "path" in m else m["run_file"])
        c.equal(
            "source/" + str(path.relative_to(run)),
            hashlib.sha256(path.read_bytes()).hexdigest(),
            m["sha256"],
        )
    return {
        "scope": "finite prototype execution; exact business/ledger checks and recorded partial-order evidence",
        "passed": all(r["passed"] for r in c.results),
        "checks": len(c.results),
        "unresolved_invocations": unresolved,
        "transient_migration_responses": transient_responses,
        "limitations": [
            "No general linearizability proof or exhaustive schedule exploration.",
            "Only successful leader API applies were logged; recorded indexes have gaps and sampled terms are not entry terms.",
            *(
                []
                if killed_phases
                else [
                    "Migration recovery was a separate process; this batch did not kill an original migration process."
                ]
            ),
            "Dispatch effect is a controlled adapter in the consumer SQLite domain.",
            "NATS JetStream is this prototype broker; Kafka and RabbitMQ are separate outstanding implementations.",
            "Logical regional isolation is on one host, not physical regional deployment.",
        ],
        "results": c.results,
    }


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-dir", required=True)
    ap.add_argument("--output")
    args = ap.parse_args()
    run = Path(args.run_dir).resolve()
    report = check(run)
    # The checker revision is preserved alongside the original application freeze.
    report["checker_sha256"] = {
        p.name: hashlib.sha256(p.read_bytes()).hexdigest()
        for p in [
            Path(__file__),
            Path(__file__).with_name("expected_spec.py"),
            Path(__file__).with_name("ownership_invariants.py"),
        ]
    }
    out = Path(args.output) if args.output else run / "independent-check.json"
    out.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(encode({k: v for k, v in report.items() if k != "results"}))
    for r in report["results"]:
        if not r["passed"]:
            print("FAIL", r["check"], encode(r))
    raise SystemExit(0 if report["passed"] else 1)
