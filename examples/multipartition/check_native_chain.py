"""Independent finite checks, reading only frozen evidence and SQLite."""

import argparse
import hashlib
import json
from pathlib import Path

from check_run import Checks, check, lines, load, sql_snapshot
from expected_spec import ROWS, STAGES, expected


def native_check(run):
    base = check(run)
    checks = Checks()
    checks.results = base["results"]
    cfg = load(run / "config.json")
    kind = cfg["broker"]["kind"]
    cleanup = load(run / "brokers/cleanup.json")
    checks.equal("native/private-listeners-clean", cleanup["private_ports_still_open"], [])
    checks.true(
        "native/recorded-processes-exited",
        all(p["returncode"] is not None for p in cleanup["processes"].values()),
    )
    for stage, count in [*STAGES, ("final-state", 13), ("10-broker-failover", 13)]:
        state = load(run / (stage + ".json"))
        dispatch = [expected(row)["dispatch"] for row in ROWS[:count]]
        wanted = {
            "operations": [
                {"key": r["key"], "body": r["body"], "result": r["result"]} for r in dispatch
            ],
            "effects": [
                {
                    "key": r["key"],
                    "f": r["f"],
                    "order_id": json.loads(r["body"])["order_id"],
                    "result": r["result"],
                }
                for r in dispatch
            ],
        }
        checks.equal(
            "external-dispatch/" + stage + "/all-fields", state["external_dispatch"], wanted
        )
    checks.equal(
        "external-dispatch/sqlite",
        sql_snapshot(run / "dispatch.sqlite"),
        load(run / "final-state.json")["external_dispatch"],
    )
    marker = load(run / "dispatch-paused.json")
    checks.equal(
        "external-dispatch/crash-input",
        marker["input"],
        {
            "key": "dispatch:initial:O:b3:3",
            "body": {"fulfillment_id": 510, "kind": "initial", "order_id": "O:b3:3"},
        },
    )
    checks.equal(
        "external-dispatch/crash-result",
        marker["result"],
        json.loads(expected(ROWS[9])["dispatch"]["result"]),
    )
    attempts = [
        h
        for h in lines(run / "dispatch-api.jsonl")
        if h["event"] == "dispatch_call" and h["input"] == marker["input"]
    ]
    checks.true("external-dispatch/original-key-retried", len(attempts) >= 2)
    checks.true(
        "external-dispatch/original-result-preserved",
        all(h["result"] == marker["result"] for h in attempts),
    )
    history = lines(run / "history.jsonl")
    exits = [h for h in history if h["event"] == "process_exit" and h["name"] == "dispatch-paused"]
    starts = [
        h
        for h in history
        if h["event"] == "process_start" and h["name"] in ("dispatch-paused", "dispatch-recovered")
    ]
    checks.true(
        "external-dispatch/actual-kill-and-new-pid",
        len(exits) == 1 and exits[0]["returncode"] != 0 and len({h["pid"] for h in starts}) == 2,
    )
    before = load(run / "brokers/before-leader-kill-metadata.json")
    after = load(run / "brokers/after-leader-kill-metadata.json")
    killed = [h for h in history if h["event"] == "native_broker_leader_killed"]
    ack = [h for h in history if h["event"] == "native_broker_majority_confirmed"]
    checks.equal("native/exactly-one-leader-kill", len(killed), 1)
    checks.equal("native/majority-confirm-after-kill", len(ack), 1)
    checks.true("native/kill-confirm-order", killed[0]["seq"] < ack[0]["seq"])
    checks.equal(
        "native/failover-ledgers-preserved",
        {
            k: load(run / "10-broker-failover.json")[k]
            for k in ("platform", "consumer", "external_dispatch")
        },
        {
            k: load(run / "09-logical-region-healed.json")[k]
            for k in ("platform", "consumer", "external_dispatch")
        },
    )
    if kind == "rabbit":
        initial = load(run / "brokers/initial-metadata.json")
        cluster = json.loads(initial["cluster"])
        queue = json.loads(initial["queues"])[0]
        checks.equal(
            "rabbit/version",
            sorted({v["rabbitmq_version"] for v in cluster["versions"].values()}),
            ["4.3.6"],
        )
        checks.equal(
            "rabbit/erlang",
            sorted({v["erlang_version"] for v in cluster["versions"].values()}),
            ["28.5.0.8"],
        )
        checks.equal("rabbit/three-running-nodes", len(cluster["running_nodes"]), 3)
        checks.equal("rabbit/quorum-queue", queue["type"], "quorum")
        checks.equal("rabbit/three-queue-members", len(queue["members"]), 3)
        checks.equal("rabbit/three-online-replicas", len(queue["online"]), 3)
        checks.equal(
            "rabbit/killed-current-queue-leader",
            killed[0]["leader"],
            json.loads(before["queues"])[0]["leader"],
        )
        checks.true(
            "rabbit/new-queue-leader",
            json.loads(after["queues"])[0]["leader"] != killed[0]["leader"],
        )
        checks.equal(
            "rabbit/mandatory-return",
            load(run / "mandatory-return.json"),
            {
                "type": "basic.return",
                "reply_code": 312,
                "routing_key": "missing-route",
                "body": "unroutable-control",
                "messages": 1,
            },
        )
    else:
        initial = load(run / "brokers/initial-metadata.json")
        for partition, row in initial["topics"]["purchase.paid"].items():
            checks.equal(
                "kafka/partition-" + partition + "/three-replicas", len(row["replicas"]), 3
            )
            checks.equal("kafka/partition-" + partition + "/three-isr", len(row["isrs"]), 3)
        checks.equal("kafka/three-topic-partitions", len(initial["topics"]["purchase.paid"]), 3)
        position = ack[0]["metadata"]
        partition = str(position["partition"])
        checks.equal(
            "kafka/killed-current-partition-leader",
            before["topics"]["purchase.paid"][partition]["leader"],
            killed[0]["leader"],
        )
        checks.true(
            "kafka/new-partition-leader",
            after["topics"]["purchase.paid"][partition]["leader"] != killed[0]["leader"],
        )
        txn = load(run / "kafka-transactions.json")
        checks.equal(
            "kafka/input",
            txn["input"],
            {"topic": "transactions-input", "partition": 0, "offset": 0, "value": "original-input"},
        )
        checks.true("kafka/abort-did-not-commit-offset", txn["group_offset_after_abort"] < 0)
        checks.equal("kafka/explicit-reread-after-abort", txn["input_reread_offset"], 0)
        checks.equal("kafka/atomic-offset-commit", txn["group_offset_after_commit"], 1)
        checks.equal(
            "kafka/read-committed-hides-abort",
            txn["observations"]["read_committed"]["values"],
            ["committed-output"],
        )
        checks.equal(
            "kafka/read-uncommitted-sees-both",
            txn["observations"]["read_uncommitted"]["values"],
            ["aborted-output", "committed-output"],
        )
        checks.equal(
            "kafka/transactional-id-fences-old-epoch",
            txn["old_producer_fenced"],
            {"code": -144, "name": "_FENCED", "fatal": True},
        )
    return checks


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-dir", required=True)
    args = parser.parse_args()
    run = Path(args.run_dir)
    checks = native_check(run)
    report = {
        "passed": all(r["passed"] for r in checks.results),
        "checks": len(checks.results),
        "results": checks.results,
        "checker_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "scope": "finite full-chain local multi-process native broker, dispatch and authority history",
        "limits": [
            "All replicas on one Windows host.",
            "External payment and dispatch are controlled HTTP/SQLite services.",
            "Kafka transactions cover Kafka output and group offsets, not external SQLite or payment.",
        ],
    }
    (run / "independent-native-check.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(json.dumps({k: v for k, v in report.items() if k != "results"}, ensure_ascii=False))
    if not report["passed"]:
        print(
            json.dumps([r for r in checks.results if not r["passed"]], ensure_ascii=False, indent=2)
        )
        raise SystemExit(1)
