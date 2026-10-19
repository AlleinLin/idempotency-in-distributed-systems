"""Timeout arbitration in actual Raft processes; independent expected outcomes.

The finite clock values in deterministic cases are trusted observer inputs, not
replica clocks. A separate worker case uses the real coordinator wall clock.
"""

import argparse
import hashlib
import json
import subprocess
import sys
import threading
import time
import traceback
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from common import canon, http, identity, purchase_input
from run_lab import ROOT, Lab, wait_for
from runtime import authority_binary

PARTICIPANTS = [
    {"bucket": "b2", "sku": "SKU-A", "qty": 1},
    {"bucket": "b3", "sku": "SKU-B", "qty": 1},
]


def negative_case(executable, out):
    """A deliberately broken timeout overwrites commit in the real Raft FSM."""
    out.mkdir()
    peer = {"id": "unsafe", "raft": "127.0.0.1:44970", "http": "http://127.0.0.1:44971"}
    log = (out / "process.log").open("ab")
    process = subprocess.Popen(
        [
            str(executable),
            "--id",
            "unsafe",
            "--group",
            "unsafe",
            "--raft",
            peer["raft"],
            "--http",
            "127.0.0.1:44971",
            "--dir",
            str(out / "data"),
            "--peers",
            canon([peer]),
            "--bootstrap",
        ],
        stdout=log,
        stderr=subprocess.STDOUT,
    )
    history = []

    def call(kind, **kw):
        request = {"bucket": "b1", "epoch": 1, "kind": kind, **kw}
        answer = http(peer["http"] + "/command", request)
        history.append({"request": request, "answer": answer})
        return answer

    try:
        wait_for(lambda: http(peer["http"] + "/status")["state"] == "Leader")
        call("init", stock={})
        s = identity(1, "unsafe")
        call("session", s=s, deadline=1000)
        call("accept", s=s, input=purchase_input(True), input_version="external-v2", now_upper=1)
        tx = call("tx_begin", s_key=canon(s), participants=PARTICIPANTS, deadline_ms=100)
        for p in PARTICIPANTS:
            call("tx_vote", tx_id=tx["id"], participant=p["bucket"], vote="yes")
        committed = call("tx_decide", tx_id=tx["id"])
        after = call(
            "tx_timeout", tx_id=tx["id"], coordinator="b1", deadline_ms=100, observed_ms=100
        )
        assert committed["decision"] == "commit" and after["decision"] == "abort"
        (out / "detected-counterexample.json").write_text(
            json.dumps(
                {
                    "detected": True,
                    "violated_invariant": "a committed TX must never become aborted",
                    "binary_sha256": hashlib.sha256(executable.read_bytes()).hexdigest(),
                    "history": history,
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
    finally:
        if process.poll() is None:
            process.kill()
            process.wait(timeout=10)
        log.close()
        (out / "cleanup.json").write_text(
            canon({"pid": process.pid, "returncode": process.poll()}), encoding="utf-8"
        )


def main(out, unsafe):
    lab = Lab(out, port_base=45100)
    checks = []
    histories = []
    lock = threading.Lock()

    def check(name, actual, expected):
        record = {
            "name": name,
            "actual": actual,
            "expected": expected,
            "passed": actual == expected,
        }
        checks.append(record)
        if not record["passed"]:
            raise AssertionError(record)

    def command(bucket, kind, **kw):
        answer = lab.cl.command(bucket, kind, **kw)
        with lock:
            histories.append(
                {
                    "at_ns": time.monotonic_ns(),
                    "thread": threading.get_ident(),
                    "bucket": bucket,
                    "kind": kind,
                    "input": kw,
                    "answer": answer,
                }
            )
        return answer

    def begin(name, deadline=100):
        s = identity(1, name)
        lab.invoke(s, purchase_input(True))
        tx = command(
            "b1", "tx_begin", s_key=canon(s), participants=PARTICIPANTS, deadline_ms=deadline
        )
        return s, tx["id"]

    def prepare(txid, participant):
        result = command(
            participant["bucket"],
            "prepare",
            tx_id=txid,
            coordinator="b1",
            input={"sku": participant["sku"], "qty": 1},
        )
        command("b1", "tx_vote", tx_id=txid, participant=participant["bucket"], vote=result["vote"])
        return result

    def timeout(txid, **overrides):
        args = {
            "tx_id": txid,
            "coordinator": "b1",
            "deadline_ms": 100,
            "observed_ms": 100,
            **overrides,
        }
        return command("b1", "tx_timeout", **args)

    def finish(txid, decision):
        done = []
        for p in PARTICIPANTS:
            done.append(
                command(
                    p["bucket"],
                    "finish",
                    tx_id=txid,
                    coordinator="b1",
                    input={"sku": p["sku"], "qty": 1},
                    decision=decision,
                )
            )
        return command("b1", "tx_done", tx_id=txid, completions=done)

    def state(txid):
        return lab.cl.bucket("b1")["transactions"][txid]

    try:
        lab.initialize()
        for role in ("pay", "relay", "consume"):
            lab.stop(role)
        s, txid = begin("timeout-first")
        prepare(txid, PARTICIPANTS[0])
        check(
            "early observation cannot abort",
            timeout(txid, observed_ms=99),
            {"code": "deadline_not_reached"},
        )
        check(
            "wrong coordinator rejected",
            timeout(txid, coordinator="b9"),
            {"code": "invalid_timeout_source"},
        )
        check(
            "wrong original deadline rejected",
            timeout(txid, deadline_ms=101),
            {"code": "timeout_conflict"},
        )
        check(
            "deadline cannot be extended",
            command("b1", "tx_begin", s_key=canon(s), participants=PARTICIPANTS, deadline_ms=200),
            {"code": "transaction_conflict"},
        )
        check("timeout first fixes abort", timeout(txid)["decision"], "abort")
        terminal = state(txid)
        check(
            "late last yes returns exact terminal",
            command("b1", "tx_vote", tx_id=txid, participant="b3", vote="yes"),
            terminal,
        )
        check("late decide cannot commit", command("b1", "tx_decide", tx_id=txid), terminal)
        # b3 has never seen prepare: abort creates a persistent terminal fence.
        complete = finish(txid, "abort")
        check("abort notified to all", complete["done"], True)
        check("late prepare has no hold", prepare(txid, PARTICIPANTS[1])["state"], "aborted")
        for bid, sku in (("b2", "SKU-A"), ("b3", "SKU-B")):
            check("released exactly once " + bid, lab.cl.bucket(bid)["stock"], {sku: 4})
        check(
            "aborted purchase has no payment eligibility",
            lab.operation(s)["reservation_ready"],
            False,
        )
        check(
            "abort ends purchase as rejected",
            lab.operation(s)["result"]["progress"]["business_state"],
            "rejected",
        )
        # Complete coordinator and participant restarts recover logs and snapshots.
        for group in ("G1", "G2", "G3"):
            check(
                "snapshot " + group,
                lab.cl.group(group, "/snapshot", {})["code"],
                "snapshot_complete",
            )
        for group in ("G1", "G2", "G3"):
            names = [n for n in lab.nodes if n.startswith(group + "-")]
            for name in names:
                lab.stop(name)
            lab.cl.leaders.pop(group, None)
            for name in names:
                lab.restart(name)
            wait_for(lambda group=group: lab.cl.state(group), label=group + " full restart")
        check("restart preserves abort and deadline", state(txid), complete)
        check(
            "restart late prepare remains terminal",
            prepare(txid, PARTICIPANTS[1])["state"],
            "aborted",
        )
        # Opposite order: commit wins, timeout cannot reverse it or open payment.
        s, committed_id = begin("commit-first")
        for p in PARTICIPANTS:
            prepare(committed_id, p)
        check("all yes is collecting before decide", state(committed_id)["decision"], "")
        committed = command("b1", "tx_decide", tx_id=committed_id)
        check("complete votes permit commit", committed["decision"], "commit")
        check("timeout after commit returns unchanged TX", timeout(committed_id), committed)
        check("commit alone does not open payment", lab.operation(s)["reservation_ready"], False)
        check(
            "missing finish receipts rejected",
            command("b1", "tx_done", tx_id=committed_id, completions=[]),
            {"code": "incomplete_participants"},
        )
        one = command(
            "b2",
            "finish",
            tx_id=committed_id,
            coordinator="b1",
            input={"sku": "SKU-A", "qty": 1},
            decision="commit",
        )
        check(
            "one finish does not open payment",
            command("b1", "tx_done", tx_id=committed_id, completions=[one]),
            {"code": "incomplete_participants"},
        )
        check("both finishes open original payment", finish(committed_id, "commit")["done"], True)
        lab.svc("pay", name="pay-resumed")
        lab.paid(s)
        lab.stop("pay-resumed")
        effects = http(lab.cfg["platform"] + "/ledger")["effects"]
        check("one actual platform charge", len(effects), 1)
        check(
            "payment uses original charge identity",
            effects[0]["key"],
            "pay-" + lab.operation(s)["c"],
        )
        # All yes still permits timeout while no decision is committed.
        s, all_yes = begin("all-yes-before-decision")
        for p in PARTICIPANTS:
            prepare(all_yes, p)
        check(
            "timeout can win after final yes before decide", timeout(all_yes)["decision"], "abort"
        )
        finish(all_yes, "abort")
        # Concurrent RPCs race at the actual coordinator Raft leader.
        for n in range(2):
            s, race_id = begin("concurrent-" + str(n))
            prepare(race_id, PARTICIPANTS[0])
            p = PARTICIPANTS[1]
            prepared = command(
                "b3", "prepare", tx_id=race_id, coordinator="b1", input={"sku": p["sku"], "qty": 1}
            )
            barrier = threading.Barrier(3)

            def vote():
                barrier.wait()
                return command(
                    "b1", "tx_vote", tx_id=race_id, participant="b3", vote=prepared["vote"]
                )

            def decide():
                barrier.wait()
                return command("b1", "tx_decide", tx_id=race_id)

            def expire():
                barrier.wait()
                return timeout(race_id)

            with ThreadPoolExecutor(max_workers=3) as pool:
                answers = list(pool.map(lambda action: action(), (vote, decide, expire)))
            tx = state(race_id)
            check(
                "concurrent terminal valid " + str(n), tx["decision"] in ("commit", "abort"), True
            )
            if tx["decision"] == "commit":
                check(
                    "concurrent commit has all yes " + str(n),
                    tx["votes"],
                    {"b2": "yes", "b3": "yes"},
                )
            frozen = canon(tx)
            with ThreadPoolExecutor(max_workers=12) as pool:
                list(
                    pool.map(
                        lambda i: (
                            timeout(race_id) if i % 2 else command("b1", "tx_decide", tx_id=race_id)
                        ),
                        range(36),
                    )
                )
            check("concurrent terminal immutable " + str(n), canon(state(race_id)), frozen)
            finish(race_id, tx["decision"])
            lab.trace(
                "concurrent_timeout_outcome",
                tx_id=race_id,
                decision=tx["decision"],
                answers=answers,
            )
        # Real time observer resumes an expired collecting TX without renewal.
        deadline = int(time.time() * 1000) - 10
        s, expired = begin("worker-expired", deadline)
        prepare(expired, PARTICIPANTS[0])
        config = {**lab.cfg, "transaction_timeout_ms": 2000}
        config_path = out / "timed-config.json"
        config_path.write_text(canon(config), encoding="utf-8")
        lab.start(
            "timed-coordinator",
            [
                sys.executable,
                "-X",
                "utf8",
                ROOT / "services.py",
                "--role",
                "coordinate",
                "--config",
                config_path,
                "--trace",
                out / "timed-coordinator.jsonl",
            ],
        )
        wait_for(lambda: state(expired)["done"], label="real clock timeout recovery")
        check("recovery worker aborts expired TX", state(expired)["decision"], "abort")
        check("recovery worker preserves deadline", state(expired)["deadline_ms"], deadline)
        check(
            "late prepare after worker finish",
            prepare(expired, PARTICIPANTS[1])["state"],
            "aborted",
        )
        # A first timed begin is also issued by this worker from wall clock.
        created = identity(1, "worker-created")
        before = int(time.time() * 1000)
        lab.invoke(created, purchase_input(True))
        created_id = "TX:" + lab.operation(created)["p"]
        wait_for(
            lambda: lab.cl.bucket("b1")["transactions"].get(created_id, {}).get("done"),
            label="worker creates timed TX",
        )
        created_tx = state(created_id)
        check(
            "worker fixes deadline on first begin",
            before + 2000 <= created_tx["deadline_ms"] <= int(time.time() * 1000) + 2000,
            True,
        )
        check("worker-created TX commits with all participants", created_tx["decision"], "commit")
        lab.stop("timed-coordinator")
        lab.save("final-state")
        # Raft apply indexes independently establish which terminal command won.
        applied = []
        for name in lab.nodes:
            for line in (out / name / "events.jsonl").read_text(encoding="utf-8").splitlines():
                row = json.loads(line)
                if row["event"] == "committed_response" and row.get("command", {}).get("kind") in (
                    "tx_vote",
                    "tx_decide",
                    "tx_timeout",
                ):
                    applied.append({"node": name, **row})
        (out / "raft-command-orders.json").write_text(
            json.dumps(applied, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        if unsafe:
            negative_case(unsafe, out / "unsafe-terminal-overwrite")
            check("unsafe implementation exposes terminal reversal", True, True)
    finally:
        lab.cleanup()
        (out / "timeout-history.json").write_text(
            json.dumps(histories, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        (out / "timeout-checks.json").write_text(
            json.dumps(checks, ensure_ascii=False, indent=2), encoding="utf-8"
        )
    summary = {
        "completed": True,
        "checks": len(checks),
        "all_passed": all(x["passed"] for x in checks),
        "binary_sha256": hashlib.sha256((authority_binary()).read_bytes()).hexdigest(),
        "scope": "actual 3-replica Raft groups, concurrent HTTP commands, full group restart, participant terminal fences and actual payment",
    }
    (out / "timeout-summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(canon(summary))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-dir", type=Path, required=True)
    parser.add_argument("--unsafe-binary", type=Path)
    args = parser.parse_args()
    try:
        main(args.run_dir, args.unsafe_binary)
    except Exception:
        (args.run_dir / "failure.txt").write_text(traceback.format_exc(), encoding="utf-8")
        raise
