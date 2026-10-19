"""One cold-start entry: new clusters, named faults, independent checks, cleanup.

Usage: python -X utf8 run_all.py --run-dir ABSOLUTE_NEW_DIRECTORY
The script refuses occupied ports/existing directories. It stops only clusters it
created. No user's PostgreSQL, Word or other service is stopped or modified.
"""

import argparse
import concurrent.futures
import hashlib
import json
import socket
import sys
import traceback
from pathlib import Path

import db
import expected_spec as e
import protocol as p
from db import PORTS, ROLE, command, pg_binary, snapshot, sql, value
from refund_model import explore

ROOT = Path(__file__).parent
parser = argparse.ArgumentParser()
parser.add_argument("--run-dir", type=Path, required=True)
args = parser.parse_args()
RUN = args.run_dir.resolve()
if RUN.exists():
    raise SystemExit("Refusing to overwrite an existing run directory.")
RUN.mkdir(parents=True)
(RUN / "sql").mkdir()
db.TRACE_DIR = RUN / "sql"
history = []
started = []
cleanup = []


def save(name, obj):
    (RUN / name).write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")


def record(label, actual, expected=None):
    if expected is not None:
        assert actual == expected, (label, actual, expected)
    state = snapshot()
    e.assert_financial_and_relational(state)
    event = {
        "step": len(history) + 1,
        "label": label,
        "actual": actual,
        "independent_expected": expected,
        "state": state,
        "all_financial_and_relational_checks": True,
    }
    history.append(event)
    save("history.json", history)
    print(f"PASS {len(history):02d} {label}", flush=True)
    return state


def reset():
    for d in PORTS:
        sql(d, "DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
    chunks = (ROOT / "schema.sql").read_text(encoding="utf-8").split("-- ")
    for chunk in chunks[1:]:
        label, body = chunk.split("\n", 1)
        sql(label.strip().lower(), body)


def baseline(lose=True):
    p.register()
    assert p.accept(e.S, e.INPUT) == e.ACC
    p.materialize()
    p.claim(owner="W1")
    p.claim(owner="W2")
    if lose:
        crash = command([sys.executable, "-X", "utf8", ROOT / "worker_fault.py"], check=False)
        assert crash.returncode == 73, ("worker fault did not fire", crash.stdout, crash.stderr)
        record(
            "actual worker process exits73 after E commits and before R_J",
            {"worker_exit_code": crash.returncode, "stdout": crash.stdout, "stderr": crash.stderr},
            {"worker_exit_code": 73, "stdout": "", "stderr": ""},
        )
    ans = p.execute()
    assert ans == e.PROVIDER
    assert p.observe_charge(ans, token=1, owner="W1") == {"code": "stale_owner"}
    assert p.observe_charge(ans, token=2, owner="W2") == e.PAYMENT
    p.materialize()
    assert p.consume(e.EVENT) == e.RECEIPT
    assert p.consume(e.EVENT) == e.RECEIPT
    p.dispatch()
    state = record(
        "complete S/P/C/T/X/O/EV/I/B/F501 and original dispatch", p.accept(e.S, e.INPUT), e.RESULT
    )
    assert e.normalized(state) == e.normalized(e.baseline()), "full independent baseline mismatch"
    save("independent-full-baseline.json", e.baseline())
    return state


try:
    for domain, port in PORTS.items():
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", port))
    for domain, port in PORTS.items():
        data = RUN / (domain + "-data")
        init = command(
            [
                pg_binary("initdb"),
                "-D",
                data,
                "-U",
                ROLE,
                "--auth=trust",
                "--encoding=UTF8",
                "--locale=C",
            ]
        )
        (RUN / (domain + "-init.log")).write_text(init.stdout + init.stderr, encoding="utf-8")
        command(
            [
                pg_binary("pg_ctl"),
                "-D",
                data,
                "-l",
                RUN / (domain + "-server.log"),
                "-o",
                f"-h 127.0.0.1 -p {port} -c fsync=on -c synchronous_commit=on -c full_page_writes=on",
                "-w",
                "start",
            ]
        )
        started.append((domain, data))
    reset()
    config = {
        d: {
            "port": PORTS[d],
            "version": sql(d, "SELECT version()"),
            "fsync": sql(d, "SHOW fsync"),
            "synchronous_commit": sql(d, "SHOW synchronous_commit"),
            "full_page_writes": sql(d, "SHOW full_page_writes"),
            "isolation": sql(d, "SHOW default_transaction_isolation"),
            "data_directory": str(RUN / (d + "-data")),
        }
        for d in PORTS
    }
    save("configuration.json", config)
    # B0 is genuinely a pre-admission local logical backup.
    command(
        [
            pg_binary("pg_dump"),
            "-h",
            "127.0.0.1",
            "-p",
            PORTS["local"],
            "-U",
            ROLE,
            "-d",
            "postgres",
            "-Fc",
            "-f",
            RUN / "local-before-A.dump",
        ]
    )
    baseline()
    assert p.accept(e.S, dict(e.INPUT, amount_minor=3500)) == {"code": "conflict"}
    record(
        "same S different amount preserves original input and all effects",
        p.accept(e.S, e.INPUT),
        e.RESULT,
    )
    # The complete new database history is discarded; other three clusters stay up.
    command(
        [
            pg_binary("pg_restore"),
            "-h",
            "127.0.0.1",
            "-p",
            PORTS["local"],
            "-U",
            ROLE,
            "-d",
            "postgres",
            "--clean",
            "--if-exists",
            RUN / "local-before-A.dump",
        ]
    )
    record(
        "real local logical-history rollback; independent X1/F501 survive",
        {
            "local_purchases": value("local", "SELECT count(*) FROM purchases"),
            "platform_balance": value("platform", "SELECT balance FROM accounts WHERE id=1"),
            "fulfillment_id": value("consumer", "SELECT id FROM fulfillments"),
        },
        {"local_purchases": 0, "platform_balance": 7000, "fulfillment_id": 501},
    )
    p.materialize()
    p.consume(e.EVENT)
    p.dispatch()
    rebuilt = record(
        "rebuild fixed original O1/EV1; duplicate delivery restores F501",
        p.accept(e.S, e.INPUT),
        e.RESULT,
    )
    assert e.normalized(rebuilt) == e.normalized(e.baseline())
    save("post-rollback-full-state.json", rebuilt)
    expectedquery = {
        "acceptance": e.ACC,
        "result": e.RESULT,
        "input_version": "external-v2",
        "original_input": e.INPUT,
        "charge_id": "C1",
        "task_id": "T1",
    }
    assert p.query_operation(e.S) == expectedquery
    assert p.query_operation(e.S, 2) == {"code": "unauthorized"}
    assert p.query_operation(p.operation(key="K-absent")) == {
        "code": "not_found_in_complete_authority"
    }
    record(
        "authorized complete-four-component-S query restores original relation after rollback",
        p.query_operation(e.S),
        expectedquery,
    )
    # Corrupt already existing rows after removing a prior table's row. A rebuild
    # would partially refill tasks before reaching order/outbox without rollback.
    damages = [
        (
            "purchase-input",
            "UPDATE purchases SET body=jsonb_set(body,'{input,quantity}','9') WHERE id='P1'",
        ),
        (
            "charge-original-result",
            "UPDATE payments SET body=jsonb_set(body,'{payment_result,amount_minor}','9') WHERE id='C1'",
        ),
        (
            "order-wrong-ownership",
            "UPDATE orders SET body=jsonb_set(body,'{purchase_id}','\"P-other\"') WHERE id='O1'",
        ),
        (
            "event-corrupt-payload",
            "UPDATE outbox SET body=jsonb_set(body,'{payload,amount_minor}','9') WHERE id='EV1'",
        ),
    ]
    for label, damage in damages:
        # Restore a known valid local image only through own dump/clean workflow.
        command(
            [
                pg_binary("pg_restore"),
                "-h",
                "127.0.0.1",
                "-p",
                PORTS["local"],
                "-U",
                ROLE,
                "-d",
                "postgres",
                "--clean",
                "--if-exists",
                RUN / "local-before-A.dump",
            ]
        )
        p.materialize()
        sql("local", "DELETE FROM tasks")
        sql("local", damage)
        before = snapshot()
        try:
            p.materialize()
        except RuntimeError as exc:
            failure = str(exc)
        else:
            raise AssertionError("corrupted projection accepted")
        after = snapshot()
        assert before == after, ("partial projection or overwrite", label)
        save(
            "conflict-" + label + ".json",
            {"before": before, "after": after, "error": failure, "all_tables_identical": True},
        )
        record("atomic conflict rollback " + label, failure)
    # Compare-and-set must report 1. Force zero after P has advanced and verify
    # rollback returns P to awaiting_payment, with no order/event partial write.
    reset()
    p.register()
    p.accept(e.S, e.INPUT)
    p.materialize()
    p.observe_charge(p.execute())
    before = snapshot()
    try:
        p.materialize(force_zero=True)
    except RuntimeError as exc:
        failure = str(exc)
    else:
        raise AssertionError("zero-row finalization accepted")
    after = snapshot()
    assert before == after
    save(
        "zero-row-cas.json",
        {"before": before, "after": after, "error": failure, "all_tables_identical": True},
    )
    p.materialize()
    record("zero-row update aborts entire projection transaction", failure)
    # Refund histories from their own clear initial state.
    reset()
    baseline(False)
    p.register("ER", "refund.create.v1")
    s1 = p.operation(action="refund.create.v1", session="ER", key="RK1")
    ri = {
        "source_charge_id": "C1",
        "currency": "CNY",
        "amount_minor": 1000,
        "reason": "customer_request",
    }
    first = p.accept_refund(s1, "C1", ri)
    assert first == {"refund_id": "RF1", "status": "pending", "amount_minor": 1000}
    assert p.accept_refund(s1, "C1", ri) == first
    assert p.accept_refund(s1, "C1", dict(ri, amount_minor=900)) == {"code": "conflict"}
    record("same refund retry reuses one reservation; different input conflicts", first, first)
    try:
        p.execute_refund("RF1", lose_response=True)
    except p.ProtocolError as exc:
        record("refund credited; response lost before authoritative result", str(exc))
    else:
        raise AssertionError("refund fault did not fire")
    assert p.accept_refund(s1, "C1", ri) == first
    result1 = p.observe_refund("RF1", p.execute_refund("RF1"))
    expected1 = {
        "refund_id": "RF1",
        "source_charge_id": "C1",
        "source_provider_charge_id": "X1",
        "status": "succeeded",
        "currency": "CNY",
        "amount_minor": 1000,
        "provider_refund_id": "PX-RF1",
    }
    assert result1 == expected1
    assert p.accept_refund(s1, "C1", ri) == expected1
    s2 = p.operation(action="refund.create.v1", session="ER", key="RK2")
    assert p.accept_refund(s2, "C1", ri) == {
        "refund_id": "RF2",
        "status": "pending",
        "amount_minor": 1000,
    }
    expected2 = dict(expected1, refund_id="RF2", provider_refund_id="PX-RF2")
    assert p.observe_refund("RF2", p.execute_refund("RF2")) == expected2
    record(
        "two legitimate equal-amount refunds preserve different RF and provider effects",
        [result1, expected2],
        [expected1, expected2],
    )
    s3 = p.operation(action="refund.create.v1", session="ER", key="RK3")
    p.accept_refund(s3, "C1", ri)
    pending = p.execute_refund("RF3", "pending")
    p.observe_refund("RF3", pending)
    s4 = p.operation(action="refund.create.v1", session="ER", key="RK4")
    assert p.accept_refund(s4, "C1", ri) == {"code": "refund_capacity_unavailable"}
    record(
        "unknown refund retains last 1000 reservation",
        pending,
        {"status": "pending", "operation_id": "H:refund-RF3"},
    )
    refusal = p.resolve_platform(p.REFUND_SCOPE, "refund-RF3", False)
    p.observe_refund("RF3", refusal)
    assert p.accept_refund(s4, "C1", ri) == {
        "refund_id": "RF4",
        "status": "pending",
        "amount_minor": 1000,
    }
    p.observe_refund("RF4", p.execute_refund("RF4"))
    p.materialize()
    record(
        "definitive rejection releases reserve; RF4 consumes released amount",
        value("platform", "SELECT balance FROM accounts WHERE id=1"),
        10000,
    )
    save("refund-complete-state.json", snapshot())
    # Real concurrent first admissions with distinct session rows; source-charge
    # row is the common arbiter. Either contestant may win; the spec permits both.
    reset()
    baseline(False)
    p.register("ER-A", "refund.create.v1")
    p.register("ER-B", "refund.create.v1")
    ca = p.operation(action="refund.create.v1", session="ER-A", key="RA")
    cb = p.operation(action="refund.create.v1", session="ER-B", key="RB")
    cr = dict(ri, amount_minor=2000)
    import threading

    barrier = threading.Barrier(2)

    def contestant(s):
        barrier.wait()
        return {"s": s, "result": p.accept_refund(s, "C1", cr)}

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(contestant, [ca, cb]))
    assert sorted(r["result"].get("code", "accepted") for r in results) == [
        "accepted",
        "refund_capacity_unavailable",
    ]
    assert value("authority", "SELECT sum(amount) FROM refunds WHERE status='pending'") == 2000
    record("concurrent 2000+2000 contend for original 3000 at common source row", results)
    # Session close ends client admission; server recovers already accepted C/T.
    reset()
    p.register()
    p.accept(e.S, e.INPUT)
    p.close_session()
    assert p.accept(e.S, e.INPUT) == {"code": "operation_expired"}
    p.observe_charge(p.execute())
    p.materialize()
    assert p.query_operation(e.S)["result"] == e.RESULT
    assert p.accept(p.operation(session="E-missing"), e.INPUT) == {"code": "operation_expired"}
    record(
        "closed/missing old session rejects entry; accepted C1 still finishes",
        p.charge()["payment_result"],
        e.PAYMENT,
    )
    # Seal fixture: absent identity is definitively closed at platform arbiter,
    # so a queued old request cannot create a success later.
    reset()
    p.register()
    p.accept(e.S, e.INPUT)
    rejection = p.seal()
    assert p.execute() == rejection
    p.observe_charge(rejection)
    record(
        "Seal missing identity prevents a late original request from succeeding",
        value("platform", "SELECT balance FROM accounts WHERE id=1"),
        10000,
    )
    # A pending operation survives Seal and retains its original H identity.
    reset()
    p.register()
    p.accept(e.S, e.INPUT)
    assert p.execute(mode="pending") == {"status": "pending", "operation_id": "H:pay-C1"}
    assert p.seal() == {"status": "pending", "operation_id": "H:pay-C1"}
    p.observe_charge(p.resolve_platform(p.CHARGE_SCOPE, "pay-C1"))
    record(
        "Seal accepted-pending preserves original work until definitive result",
        p.charge()["payment_result"],
        e.PAYMENT,
    )
    # Explicit old local-v1 vs external-v2. Old input/result are recovered using
    # the old action/version; current balances or v2 fields do not rewrite them.
    reset()
    p.register("EL", "purchase.create.v1")
    old1 = p.operation(action="purchase.create.v1", session="EL", key="KL1")
    old2 = p.operation(action="purchase.create.v1", session="EL", key="KL2")
    oldinp = p.purchase_input(version="local-v1")
    orig = p.accept(old1, oldinp, "local-v1")
    orig2 = p.accept(old2, oldinp, "local-v1")
    expectedold = {
        "idempotency_key": "KL1",
        "purchase_id": "P1",
        "status": "succeeded",
        "order_id": "O1",
        "debit_id": "D1",
        "product_id": "paper-001",
        "quote_id": "Q7",
        "currency": "CNY",
        "amount_minor": 3000,
        "balance_after_minor": 7000,
        "retry_until": "2026-10-18T10:00:00Z",
    }
    assert orig == expectedold and orig2["balance_after_minor"] == 4000
    p.register()
    new = p.accept(e.S, e.INPUT)
    assert new["purchase_id"] == "P3" and new["charge_id"] == "C1"
    assert p.accept(old1, oldinp, "local-v1") == expectedold
    assert p.accept(old1, e.INPUT, "local-v1") == {"code": "conflict"}
    assert p.accept(e.S, p.purchase_input(), "external-v2") == new
    record(
        "cross-version replay returns original v1 balance7000 and original v2 C1",
        {"old": p.accept(old1, oldinp, "local-v1"), "new": p.accept(e.S, e.INPUT)},
        {"old": expectedold, "new": new},
    )
    save("version-complete-state.json", snapshot())
    # No C1 is passed to this recovery entry: it discovers durable work, reads
    # original bindings, pays, records, projects, delivers and dispatches.
    reset()
    p.register()
    p.accept(e.S, e.INPUT)
    scan1 = p.recover_once()
    scan2 = p.recover_once()
    assert scan1["scanned_charges"] == 1 and scan2["scanned_charges"] == 0
    assert scan1["receipts"] == [e.RECEIPT] and scan2["receipts"] == [e.RECEIPT]
    assert p.query_operation(e.S)["result"] == e.RESULT
    record("durable-responsibility scanner discovers work and resumes whole chain", scan2)
    save("scanner-complete-state.json", snapshot())
    model = {"correct": explore(), "broken_ignores_pending": explore(True)}
    assert model["correct"]["safe"] and not model["broken_ignores_pending"]["safe"]
    save("refund-model.json", model)
    # Source hashes preserve exactly the code used, before any subsequent edits.
    sources = {
        str(path.relative_to(ROOT)): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in ROOT.glob("*")
        if path.is_file()
    }
    save("source-hashes.json", sources)
    save(
        "summary.json",
        {
            "status": "passed",
            "histories": len(history),
            "models": model,
            "independent_full_baseline": True,
            "projection_conflict_all_tables_compared": 5,
            "cold_start": "four new local clusters using installed PostgreSQL and Python",
            "not_run": [
                "commercial payment API",
                "RabbitMQ/Kafka",
                "consensus service",
                "physical PITR",
                "power loss",
            ],
        },
    )
except Exception:
    (RUN / "failure.txt").write_text(traceback.format_exc(), encoding="utf-8")
    raise
finally:
    for domain, data in reversed(started):
        stop = command([pg_binary("pg_ctl"), "-D", data, "-m", "fast", "-w", "stop"], check=False)
        cleanup.append(
            {
                "domain": domain,
                "data_directory": str(data),
                "port": PORTS[domain],
                "stop_returncode": stop.returncode,
                "stdout": stop.stdout,
                "stderr": stop.stderr,
            }
        )
    save("cleanup.json", cleanup)
