"""Native RabbitMQ TTL and the two durable handoff boundaries."""

import argparse
import copy
import json
import time
import traceback
from pathlib import Path

from common import canon, http, identity
from run_lab import Lab, wait_for


def main(out):
    lab = Lab(out, port_base=46100, broker="rabbit", external_dispatch=True)
    checks = []

    def check(name, actual, expected):
        item = {"name": name, "actual": actual, "expected": expected, "passed": actual == expected}
        checks.append(item)
        if not item["passed"]:
            raise AssertionError(item)

    def observed(s, evidence):
        return lab.cl.command(
            "b1", "consumer_observed", s_key=canon(s), event=lab.operation(s)["event"], **evidence
        )

    try:
        lab.initialize()
        lab.stop("consume")
        lab.stop("relay")
        lab.native.ctl(
            0,
            "ttl-policy",
            "set_policy",
            "--apply-to",
            "quorum_queues",
            "handoff-ttl",
            "^purchase\\.paid$",
            '{"message-ttl":600}',
        )
        lab.svc("relay", name="publisher")
        s = identity(1, "ttl-no-new-traffic")
        lab.invoke(s)
        lab.paid(s)
        wait_for(
            lambda: lab.operation(s)["published"], label="broker confirm and durable published"
        )
        lab.stop("publisher")
        original = copy.deepcopy(lab.operation(s)["result"])
        check(
            "broker publication is not consumer handoff",
            lab.operation(s).get("handoff_confirmed", False),
            False,
        )
        import pika

        connection = pika.BlockingConnection(pika.ConnectionParameters("127.0.0.1", 37400))
        channel = connection.channel()
        time.sleep(1)
        check(
            "TTL removes the only event",
            channel.basic_get("purchase.paid", auto_ack=False)[0],
            None,
        )
        check("consumer has accepted no event", http(lab.cfg["consumer"] + "/ledger")["inbox"], [])
        # No new purchase is submitted. The authority's existing recovery scan
        # republishes the old EV and obtains the committed consumer evidence.
        marker = out / "before-authority-handoff.json"
        lab.svc("consume")
        lab.svc("relay", name="before-handoff", pause="handoff_read", marker=marker)
        wait_for(marker.exists, label="consumer committed, authority receipt not saved")
        lab.stop("before-handoff")
        check(
            "crash before authority save retains responsibility",
            lab.operation(s).get("handoff_confirmed", False),
            False,
        )
        evidence = http(lab.cfg["consumer"] + "/receipt", lab.operation(s)["event"])
        check(
            "lost receipt recovered by exact Inbox query",
            evidence["receipt"]["event_id"],
            lab.operation(s)["event_id"],
        )
        after_marker = out / "after-authority-handoff.json"
        lab.svc("relay", name="after-handoff", pause="handoff_saved", marker=after_marker)
        wait_for(after_marker.exists, label="authority persisted handoff and progress")
        lab.stop("after-handoff")
        saved = lab.operation(s)
        check(
            "one authority commit saves receipt source and handoff",
            [
                saved.get("handoff_confirmed"),
                saved.get("consumer_receipt"),
                saved.get("consumer_source"),
            ],
            [True, evidence["receipt"], evidence["source"]],
        )
        check(
            "current fulfillment progress advances atomically",
            saved.get("current_progress"),
            {"business_state": "paid", "fulfillment_state": "fulfillment_accepted"},
        )
        check(
            "original payment response remains byte-for-byte equal",
            canon(saved["result"]),
            canon(original),
        )
        check("same-S accept replays original response", lab.invoke(s), original)
        query = http(lab.cfg["router"] + "/query", {"s": s})
        check(
            "query retains original response and newer progress",
            [query["result"], query["current_progress"]],
            [original, saved["current_progress"]],
        )
        before = canon(saved)
        lab.cl.command(
            "b1", "published", s_key=canon(s), event=saved["event"], retry_after=999999999999
        )
        check("late published cannot regress confirmed handoff", canon(lab.operation(s)), before)
        lab.svc("relay", name="recovered-relay")
        time.sleep(1.3)
        check(
            "post-save crash does not republish",
            [
                r
                for r in (out / "recovered-relay.jsonl").read_text("utf-8").splitlines()
                if json.loads(r)["event"] == "publish_confirmed"
            ],
            [],
        )
        lab.stop("recovered-relay")
        # A committed consumer receipt can arrive before the publisher records
        # the broker confirmation; it is retried after publication is saved.
        early = identity(4, "early-receipt")
        lab.invoke(early)
        lab.paid(early)
        event = lab.operation(early)["event"]
        original_early = copy.deepcopy(lab.operation(early)["result"])
        http(lab.cfg["consumer"] + "/consume", event)
        early_evidence = http(lab.cfg["consumer"] + "/receipt", event)
        check(
            "early receipt waits for durable published",
            observed(early, early_evidence),
            {"code": "publication_not_recorded"},
        )
        channel.confirm_delivery()
        confirmed = channel.basic_publish(
            "purchases",
            "purchase.paid",
            canon(event).encode(),
            properties=pika.BasicProperties(delivery_mode=2),
            mandatory=True,
        )
        check("actual broker confirms original EV", confirmed is not False, True)
        lab.cl.command("b1", "published", s_key=canon(early), event=event, retry_after=100)
        for label, mutate in [
            ("EV payload", lambda x: x["source"]["event"]["payload"].update(amount_minor=1)),
            ("order", lambda x: x["receipt"].update(order_id="O:wrong")),
            ("actual F", lambda x: x["receipt"].update(fulfillment_id=999)),
            ("business guard", lambda x: x["source"].update(business_guard="initial:O:wrong")),
            ("dispatch identity", lambda x: x["source"].update(dispatch_key="dispatch:wrong")),
            ("source domain", lambda x: x["source"].update(domain="untrusted")),
            ("source record", lambda x: x["source"].update(record_id="EV:wrong")),
        ]:
            damaged = copy.deepcopy(early_evidence)
            mutate(damaged)
            check(
                "damaged " + label + " rejected",
                observed(early, damaged),
                {"code": "receipt_conflict"},
            )
            check(
                "damaged " + label + " leaves no handoff",
                lab.operation(early).get("handoff_confirmed", False),
                False,
            )
        check(
            "original complete evidence is accepted",
            observed(early, early_evidence),
            {"code": "consumer_observed"},
        )
        check(
            "second original response remains immutable",
            lab.operation(early)["result"],
            original_early,
        )
        # Restart the complete authority group after an explicit snapshot.
        check(
            "snapshot includes handoff",
            lab.cl.group("G1", "/snapshot", {})["code"],
            "snapshot_complete",
        )
        names = [name for name in lab.nodes if name.startswith("G1-")]
        for name in names:
            lab.stop(name)
        lab.cl.leaders.pop("G1", None)
        for name in names:
            lab.restart(name)
        wait_for(lambda: lab.cl.state("G1"), label="handoff authority restart")
        check(
            "snapshot recovery retains all evidence",
            lab.operation(early)["consumer_source"],
            early_evidence["source"],
        )
        check(
            "snapshot recovery retains original response",
            lab.operation(early)["result"],
            original_early,
        )
        frozen = lab.cl.command("b1", "freeze", migration="handoff-image")
        imported = frozen["image"]["operations"][canon(early)]
        check(
            "frozen complete image retains handoff and current progress",
            [
                imported["handoff_confirmed"],
                imported["consumer_receipt"],
                imported["consumer_source"],
                imported["current_progress"],
            ],
            [
                True,
                early_evidence["receipt"],
                early_evidence["source"],
                {"business_state": "paid", "fulfillment_state": "fulfillment_accepted"},
            ],
        )
        lab.save("final-state")
        connection.close()
    finally:
        lab.cleanup()
        (out / "handoff-checks.json").write_text(
            json.dumps(checks, ensure_ascii=False, indent=2), encoding="utf-8"
        )
    summary = {
        "completed": True,
        "checks": len(checks),
        "all_passed": all(c["passed"] for c in checks),
        "scope": "native 3-node RabbitMQ quorum queue, finite TTL, independent consumer SQLite, Raft authority and real process crashes",
    }
    (out / "handoff-summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(canon(summary))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-dir", type=Path, required=True)
    args = parser.parse_args()
    try:
        main(args.run_dir)
    except Exception:
        (args.run_dir / "failure.txt").write_text(traceback.format_exc(), encoding="utf-8")
        raise
