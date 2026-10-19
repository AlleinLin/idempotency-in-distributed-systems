"""Real independent HTTP/worker processes for the lab's own network and ledgers."""

import argparse
import asyncio
import json
import os
import socket
import sqlite3
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.error import HTTPError

from boundary import InputError, strict_loads
from boundary import identity as validate_identity
from boundary import purchase as validate_purchase
from common import Cluster, Trace, canon, http, partition


def database(path):
    c = sqlite3.connect(path, timeout=10, isolation_level=None)
    c.row_factory = sqlite3.Row
    c.execute("PRAGMA journal_mode=WAL")
    c.execute("PRAGMA synchronous=FULL")
    return c


def snapshot(c):
    names = [
        x[0]
        for x in c.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
        )
    ]
    return {n: [dict(r) for r in c.execute(f"SELECT * FROM {n} ORDER BY rowid")] for n in names}


PLATFORM_DDL = """
CREATE TABLE IF NOT EXISTS accounts(account_id INTEGER PRIMARY KEY,balance INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS operations(scope TEXT,key TEXT,kind TEXT,input TEXT,status TEXT,result TEXT,PRIMARY KEY(scope,key));
CREATE TABLE IF NOT EXISTS effects(effect_id INTEGER PRIMARY KEY AUTOINCREMENT,scope TEXT,key TEXT,account_id INTEGER,kind TEXT,amount INTEGER,result TEXT,UNIQUE(scope,key));
"""
CONSUMER_DDL = """
CREATE TABLE IF NOT EXISTS fulfillments(f INTEGER PRIMARY KEY,order_id TEXT UNIQUE,payload TEXT);
CREATE TABLE IF NOT EXISTS business_guard(b TEXT PRIMARY KEY,f INTEGER UNIQUE);
CREATE TABLE IF NOT EXISTS inbox(event_id TEXT PRIMARY KEY,event TEXT,f INTEGER,receipt TEXT);
CREATE TABLE IF NOT EXISTS dispatch(key TEXT PRIMARY KEY,f INTEGER UNIQUE,body TEXT,result TEXT);
"""
DISPATCH_DDL = """
CREATE TABLE IF NOT EXISTS operations(key TEXT PRIMARY KEY,body TEXT NOT NULL,result TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS effects(key TEXT PRIMARY KEY,f INTEGER UNIQUE,order_id TEXT UNIQUE,result TEXT NOT NULL);
"""


class SemanticConflict(ValueError):
    pass


def validate_event(event):
    if type(event) is not dict or set(event) != {
        "event_id",
        "event_type",
        "event_version",
        "occurred_at",
        "payload",
    }:
        raise SemanticConflict("invalid event envelope")
    if (
        event["event_type"] != "purchase.paid"
        or type(event["event_version"]) is not int
        or event["event_version"] != 1
    ):
        raise SemanticConflict("unsupported event version")
    payload = event["payload"]
    fields = {
        "purchase_id",
        "charge_id",
        "order_id",
        "provider_charge_id",
        "currency",
        "amount_minor",
    }
    if type(payload) is not dict or set(payload) != fields:
        raise SemanticConflict("invalid event payload")
    if (
        type(payload["amount_minor"]) is not int
        or not 0 < payload["amount_minor"] <= 9007199254740991
    ):
        raise SemanticConflict("invalid event amount")
    if any(type(payload[k]) is not str or not payload[k] for k in fields - {"amount_minor"}):
        raise SemanticConflict("invalid event identifier")
    if (
        type(event["event_id"]) is not str
        or not event["event_id"]
        or type(event["occurred_at"]) is not str
    ):
        raise SemanticConflict("invalid event identity or time")


def persist_blocker(path, delivery, error):
    """Fail closed across worker restarts until an operator resolves the record."""
    c = database(path)
    c.execute("""CREATE TABLE IF NOT EXISTS blocked(
      source TEXT PRIMARY KEY,metadata TEXT NOT NULL,raw BLOB NOT NULL,
      reason TEXT NOT NULL,state TEXT NOT NULL DEFAULT 'blocked')""")
    meta = delivery.metadata
    source = canon(
        {
            k: meta.get(k)
            for k in (
                "broker",
                "topic",
                "partition",
                "offset",
                "queue",
                "delivery_tag",
                "channel_id",
            )
        }
    )
    c.execute(
        "INSERT OR IGNORE INTO blocked(source,metadata,raw,reason) VALUES(?,?,?,?)",
        (source, canon(meta), delivery.raw, str(error)),
    )
    c.close()


def has_blocker(path):
    if not Path(path).exists():
        return False
    c = database(path)
    try:
        return (
            c.execute("SELECT 1 FROM blocked WHERE state='blocked' LIMIT 1").fetchone() is not None
        )
    finally:
        c.close()


def platform(c, x, resolve=False):
    scope, key, kind, inp = x["scope"], x["key"], x["kind"], x["input"]
    c.execute("BEGIN IMMEDIATE")
    try:
        old = c.execute("SELECT * FROM operations WHERE scope=? AND key=?", (scope, key)).fetchone()
        if old:
            if old["kind"] != kind or old["input"] != canon(inp):
                c.execute("COMMIT")
                return {"code": "provider_conflict"}
            if old["status"] != "pending" or not resolve:
                c.execute("COMMIT")
                return (
                    json.loads(old["result"])
                    if old["result"]
                    else {"status": "pending", "scope": scope, "key": key}
                )
        elif resolve:
            raise ValueError("no pending operation")
        elif x.get("mode") == "pending":
            c.execute(
                "INSERT INTO operations VALUES(?,?,?,?,?,NULL)",
                (scope, key, kind, canon(inp), "pending"),
            )
            c.execute("COMMIT")
            return {"status": "pending", "scope": scope, "key": key}
        account = inp["account_id"]
        amount = inp["amount_minor"]
        c.execute("INSERT OR IGNORE INTO accounts VALUES(?,10000)", (account,))
        if kind == "refund":
            original = [
                json.loads(r["result"])
                for r in c.execute(
                    "SELECT * FROM effects WHERE kind='charge' AND account_id=?", (account,)
                )
            ]
            source = [p for p in original if p["provider_charge_id"] == inp["source_x"]]
            if len(source) != 1:
                raise ValueError("missing source charge")
            refunded = sum(
                json.loads(r["result"])["amount_minor"]
                for r in c.execute(
                    "SELECT * FROM effects WHERE kind='refund' AND account_id=?", (account,)
                )
                if json.loads(r["result"])["source_provider_charge_id"] == inp["source_x"]
            )
            if refunded + amount > source[0]["amount_minor"]:
                raise ValueError("refund capacity")
        elif (
            c.execute("SELECT balance FROM accounts WHERE account_id=?", (account,)).fetchone()[0]
            < amount
        ):
            raise ValueError("insufficient platform funds in configured history")
        c.execute(
            "UPDATE accounts SET balance=balance+? WHERE account_id=?",
            ((amount if kind == "refund" else -amount), account),
        )
        cur = c.execute(
            "INSERT INTO effects(scope,key,account_id,kind,amount) VALUES(?,?,?,?,?)",
            (scope, key, account, kind, amount),
        )
        n = cur.lastrowid
        result = {
            "status": "succeeded",
            "scope": scope,
            "key": key,
            "currency": "CNY",
            "amount_minor": amount,
        }
        if kind == "charge":
            result.update(
                account_id=account, charge_id=inp["charge_id"], provider_charge_id=f"X{n}"
            )
        else:
            result.update(
                refund_id=inp["refund_id"],
                source_charge_id=inp["source_c"],
                source_provider_charge_id=inp["source_x"],
                provider_refund_id=f"PX-RF{n}",
            )
        c.execute("UPDATE effects SET result=? WHERE effect_id=?", (canon(result), n))
        if old:
            c.execute(
                "UPDATE operations SET status='succeeded',result=? WHERE scope=? AND key=?",
                (canon(result), scope, key),
            )
        else:
            c.execute(
                "INSERT INTO operations VALUES(?,?,?,?,?,?)",
                (scope, key, kind, canon(inp), "succeeded", canon(result)),
            )
        c.execute("COMMIT")
        return result
    except:
        c.execute("ROLLBACK")
        raise


def consume(c, event):
    validate_event(event)
    eid = event["event_id"]
    payload = event["payload"]
    order = payload["order_id"]
    c.execute("BEGIN IMMEDIATE")
    try:
        old = c.execute("SELECT * FROM inbox WHERE event_id=?", (eid,)).fetchone()
        if old:
            if old["event"] != canon(event):
                raise SemanticConflict("event payload conflict")
            receipt = consumer_receipt(c, event)["receipt"]
            c.execute("COMMIT")
            return receipt
        oldf = c.execute("SELECT * FROM fulfillments WHERE order_id=?", (order,)).fetchone()
        if oldf:
            if oldf["payload"] != canon(payload):
                raise SemanticConflict("business payload conflict")
            f = oldf["f"]
        else:
            f = c.execute("SELECT coalesce(max(f),500)+1 FROM fulfillments").fetchone()[0]
            c.execute("INSERT INTO fulfillments VALUES(?,?,?)", (f, order, canon(payload)))
            c.execute("INSERT INTO business_guard VALUES(?,?)", ("initial:" + order, f))
            body = {"fulfillment_id": f, "order_id": order, "kind": "initial"}
            key = "dispatch:initial:" + order
            # Intent and effect adapter are distinguished in the research scope.
            c.execute("INSERT INTO dispatch VALUES(?,?,?,NULL)", (key, f, canon(body)))
        receipt = {
            "event_id": eid,
            "order_id": order,
            "fulfillment_id": f,
            "status": "fulfillment_accepted",
        }
        c.execute("INSERT INTO inbox VALUES(?,?,?,?)", (eid, canon(event), f, canon(receipt)))
        c.execute("COMMIT")
        return receipt
    except:
        c.execute("ROLLBACK")
        raise


def consumer_receipt(c, event):
    """Return evidence only for the exact event committed by consume()."""
    validate_event(event)
    row = c.execute(
        "SELECT event,f,receipt FROM inbox WHERE event_id=?", (event["event_id"],)
    ).fetchone()
    if row is None:
        return {"receipt": None}
    if row["event"] != canon(event):
        raise SemanticConflict("event payload conflict")
    order = event["payload"]["order_id"]
    f = row["f"]
    receipt = {
        "event_id": event["event_id"],
        "order_id": order,
        "fulfillment_id": f,
        "status": "fulfillment_accepted",
    }
    fulfillment = c.execute("SELECT * FROM fulfillments WHERE f=?", (f,)).fetchone()
    guard = c.execute("SELECT * FROM business_guard WHERE b=?", ("initial:" + order,)).fetchone()
    intent = c.execute(
        "SELECT * FROM dispatch WHERE key=?", ("dispatch:initial:" + order,)
    ).fetchone()
    body = {"fulfillment_id": f, "order_id": order, "kind": "initial"}
    if (
        json.loads(row["receipt"]) != receipt
        or not fulfillment
        or fulfillment["order_id"] != order
        or fulfillment["payload"] != canon(event["payload"])
        or not guard
        or guard["f"] != f
        or not intent
        or intent["f"] != f
        or intent["body"] != canon(body)
    ):
        raise SemanticConflict("incomplete consumer handoff linkage")
    source = {
        "domain": "fulfillment-primary",
        "record_id": event["event_id"],
        "event": event,
        "business_guard": guard["b"],
        "dispatch_key": intent["key"],
        "dispatch_input": body,
    }
    return {"receipt": receipt, "source": source}


def dispatch(c):
    c.execute("BEGIN IMMEDIATE")
    for r in c.execute("SELECT * FROM dispatch WHERE result IS NULL").fetchall():
        result = {
            "status": "accepted",
            "dispatch_key": r["key"],
            "fulfillment_id": r["f"],
            "provider_dispatch_id": "DSP-" + str(r["f"]),
        }
        c.execute("UPDATE dispatch SET result=? WHERE key=?", (canon(result), r["key"]))
    c.execute("COMMIT")


def external_dispatch(c, x):
    key, body = x["key"], x["body"]
    if key != "dispatch:initial:" + body["order_id"] or body["kind"] != "initial":
        raise ValueError("dispatch identity/input mismatch")
    c.execute("BEGIN IMMEDIATE")
    try:
        old = c.execute("SELECT * FROM operations WHERE key=?", (key,)).fetchone()
        if old:
            if old["body"] != canon(body):
                raise ValueError("dispatch input conflict")
            result = json.loads(old["result"])
        else:
            result = {
                "status": "accepted",
                "dispatch_key": key,
                "fulfillment_id": body["fulfillment_id"],
                "provider_dispatch_id": "DSP-" + str(body["fulfillment_id"]),
            }
            c.execute("INSERT INTO operations VALUES(?,?,?)", (key, canon(body), canon(result)))
            c.execute(
                "INSERT INTO effects VALUES(?,?,?,?)",
                (key, body["fulfillment_id"], body["order_id"], canon(result)),
            )
        c.execute("COMMIT")
        return result
    except:
        c.execute("ROLLBACK")
        raise


def dispatch_observe(c, x):
    c.execute("BEGIN IMMEDIATE")
    try:
        row = c.execute("SELECT * FROM dispatch WHERE key=?", (x["key"],)).fetchone()
        result = x["result"]
        expected = {
            "status": "accepted",
            "dispatch_key": row["key"],
            "fulfillment_id": row["f"],
            "provider_dispatch_id": "DSP-" + str(row["f"]),
        }
        if result != expected:
            raise ValueError("dispatch result mismatch")
        if row["result"] and row["result"] != canon(result):
            raise ValueError("dispatch result conflict")
        c.execute("UPDATE dispatch SET result=? WHERE key=?", (canon(result), row["key"]))
        c.execute("COMMIT")
        return {"code": "observed"}
    except:
        c.execute("ROLLBACK")
        raise


def run_http(args, cfg, trace):
    cl = Cluster(cfg)
    gate = threading.Event()
    gate.set()
    settings = {"hold": False, "drop_responses": 0, "delay_ms": 0}
    mutex = threading.Lock()
    events = []
    if args.role in ("platform", "consumer-api", "dispatch-api"):
        c = database(args.db)
        c.executescript(
            {"platform": PLATFORM_DDL, "consumer-api": CONSUMER_DDL, "dispatch-api": DISPATCH_DDL}[
                args.role
            ]
        )
        c.close()

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *a):
            pass

        def do_GET(self):
            self.handle_request(False)

        def do_POST(self):
            self.handle_request(True)

        def handle_request(self, post):
            try:
                length = int(self.headers.get("Content-Length", 0))
                if length > 1024 * 1024:
                    raise InputError("request body exceeds one MiB")
                body = strict_loads(self.rfile.read(length) or "{}") if post else None
                if args.role == "proxy":
                    if self.path == "/config":
                        with mutex:
                            settings.update(body)
                            gate.clear() if settings["hold"] else gate.set()
                        r = {"code": "configured"}
                    elif self.path == "/events":
                        r = {"events": events}
                    else:
                        with mutex:
                            config = dict(settings)
                            events.append(
                                {"event": "received", "body": body, "ordinal": len(events) + 1}
                            )
                        trace("proxy_received", body=body, config=config)
                        if config["hold"]:
                            gate.wait()
                        if config["delay_ms"]:
                            time.sleep(config["delay_ms"] / 1000)
                        r = http(cfg["platform"] + self.path, body, timeout=20)
                        trace("proxy_forwarded", body=body, result=r)
                        with mutex:
                            drop = settings["drop_responses"] > 0
                            if drop:
                                settings["drop_responses"] -= 1
                        if drop:
                            trace("proxy_response_dropped", body=body, result=r)
                            self.connection.shutdown(socket.SHUT_RDWR)
                            self.connection.close()
                            return
                elif self.path == "/health":
                    r = {"status": "ready", "pid": os.getpid()}
                elif args.role == "platform":
                    c = database(args.db)
                    try:
                        r = (
                            snapshot(c)
                            if self.path == "/ledger"
                            else platform(c, body, self.path == "/resolve")
                        )
                    finally:
                        c.close()
                    trace("platform_" + self.path.strip("/"), input=body, result=r)
                elif args.role == "consumer-api":
                    c = database(args.db)
                    try:
                        if self.path == "/ledger":
                            r = snapshot(c)
                        elif self.path == "/consume":
                            r = consume(c, body)
                        elif self.path == "/receipt":
                            c.execute("BEGIN")
                            r = consumer_receipt(c, body)
                            c.execute("COMMIT")
                        elif self.path == "/dispatch":
                            dispatch(c)
                            r = {"code": "dispatched"}
                        elif self.path == "/dispatch_observe":
                            r = dispatch_observe(c, body)
                        else:
                            r = {"code": "unknown_path"}
                    finally:
                        c.close()
                    trace("consumer_" + self.path.strip("/"), input=body, result=r)
                elif args.role == "dispatch-api":
                    c = database(args.db)
                    try:
                        r = snapshot(c) if self.path == "/ledger" else external_dispatch(c, body)
                    finally:
                        c.close()
                    trace("dispatch_" + self.path.strip("/"), input=body, result=r)
                elif args.role == "router":
                    if self.path == "/refresh":
                        r = cl.refresh()
                    elif self.path == "/routes":
                        r = cl.routes
                    elif self.path == "/accept":
                        validate_purchase(body)
                        s = body["s"]
                        r = cl.command(
                            partition(s),
                            "accept",
                            s=s,
                            input=body["input"],
                            input_version="external-v2",
                            now_upper=1,
                        )
                    elif self.path == "/query":
                        if type(body) is not dict or set(body) != {"s"}:
                            raise InputError("query requires s")
                        validate_identity(body["s"])
                        s = body["s"]
                        b = cl.bucket(partition(s))
                        r = b["operations"].get(canon(s), {"code": "not_found"})
                    else:
                        r = {"code": "unknown_path"}
                else:
                    r = {"code": "unknown_role"}
                raw = canon(r).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(raw)))
                self.end_headers()
                self.wfile.write(raw)
            except (InputError, SemanticConflict) as exc:
                status = 409 if isinstance(exc, SemanticConflict) else 400
                r = {
                    "code": "semantic_conflict" if status == 409 else "invalid_input",
                    "error": str(exc),
                }
                trace("http_rejected", path=self.path, status=status, result=r)
                raw = canon(r).encode()
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(raw)))
                self.end_headers()
                self.wfile.write(raw)
            except Exception as exc:
                trace("http_error", path=self.path, error=repr(exc))
                raw = canon({"code": "service_unavailable", "error": repr(exc)}).encode()
                try:
                    self.send_response(503)
                    self.end_headers()
                    self.wfile.write(raw)
                except OSError:
                    pass

    trace("process_start", role=args.role, pid=os.getpid(), port=args.port)
    ThreadingHTTPServer(("127.0.0.1", args.port), Handler).serve_forever()


def checkpoint(args, trace, name, data):
    if args.pause_after == name:
        Path(args.marker).write_text(canon(data), encoding="utf-8")
        trace("paused", checkpoint=name, data=data)
        while True:
            time.sleep(0.1)


async def run_worker(args, cfg, trace):
    cl = Cluster(cfg)
    owner = f"{args.role}:{os.getpid()}"
    broker = None
    blocker_path = Path(
        cfg.get("consumer_quarantine", Path(args.trace).parent / "consumer-quarantine.sqlite")
    )
    if args.role == "consume" and has_blocker(blocker_path):
        trace("consumer_blocked_on_start", path=str(blocker_path))
        raise SystemExit("unresolved durable consumer blocker")
    if args.role in ("relay", "consume"):
        from broker_adapters import create_adapter

        broker = create_adapter(cfg, args.role)
        for attempt in range(8):
            try:
                await broker.setup()
                break
            except Exception as exc:
                trace("stream_setup_retry", error=repr(exc))
                if attempt == 7:
                    raise
                await asyncio.sleep(0.2)
    trace("process_start", role=args.role, pid=os.getpid())
    while True:
        try:
            if args.role == "consume":
                messages = await broker.fetch()
                for m in messages:
                    event = m.event
                    try:
                        if m.error:
                            raise SemanticConflict(m.error)
                        r = await asyncio.to_thread(http, cfg["consumer"] + "/consume", event)
                    except (HTTPError, SemanticConflict) as exc:
                        if isinstance(exc, SemanticConflict) or exc.code in (400, 409, 422):
                            persist_blocker(blocker_path, m, exc)
                            trace(
                                "consumer_durably_blocked",
                                metadata=m.metadata,
                                error=str(exc),
                                path=str(blocker_path),
                            )
                            raise SystemExit("consumer semantic conflict; operator review required")
                        await broker.retry(m)
                        raise
                    except Exception:
                        await broker.retry(m)
                        raise
                    trace("consumer_committed", event=event, receipt=r, metadata=m.metadata)
                    checkpoint(args, trace, "consume", {"event": event, "receipt": r})
                    await broker.ack(m)
                    trace("message_acked", event_id=event["event_id"], metadata=m.metadata)
                    if "dispatch" not in cfg:
                        await asyncio.to_thread(http, cfg["consumer"] + "/dispatch", {})
            elif args.role == "dispatch":
                ledger = await asyncio.to_thread(http, cfg["consumer"] + "/ledger")
                for row in ledger["dispatch"]:
                    if row["result"]:
                        continue
                    body = {"key": row["key"], "body": json.loads(row["body"])}
                    result = await asyncio.to_thread(http, cfg["dispatch"] + "/call", body)
                    trace("dispatch_provider_returned", input=body, result=result)
                    checkpoint(args, trace, "dispatch_provider", {"input": body, "result": result})
                    await asyncio.to_thread(
                        http,
                        cfg["consumer"] + "/dispatch_observe",
                        {"key": row["key"], "result": result},
                    )
                    trace("dispatch_observed", key=row["key"], result=result)
            else:
                buckets = list(await asyncio.to_thread(lambda: list(cl.active_buckets())))
                for bid, b in buckets:
                    if args.role == "refund":
                        for sk, r in b["refunds"].items():
                            if r["result"] is not None:
                                continue
                            pr = await asyncio.to_thread(
                                http,
                                cfg["proxy"] + "/call",
                                {
                                    "scope": r["scope"],
                                    "key": r["key"],
                                    "kind": "refund",
                                    "input": r["provider_input"],
                                },
                                2,
                            )
                            trace("refund_provider_returned", bucket=bid, s_key=sk, result=pr)
                            checkpoint(
                                args,
                                trace,
                                "refund_provider",
                                {"bucket": bid, "s_key": sk, "result": pr},
                            )
                            ans = await asyncio.to_thread(
                                cl.command,
                                bid,
                                "refund_observe",
                                s_key=sk,
                                provider_input=r["provider_input"],
                                provider_result=pr,
                            )
                            trace("refund_observed", bucket=bid, s_key=sk, result=ans)
                    for sk, o in b["operations"].items():
                        if args.role == "pay" and not o["result"] and o["reservation_ready"]:
                            claimed = await asyncio.to_thread(
                                cl.command, bid, "claim", s_key=sk, owner=owner
                            )
                            if "c" not in claimed:
                                continue
                            r = await asyncio.to_thread(
                                http,
                                cfg["proxy"] + "/call",
                                {
                                    "scope": o["scope"],
                                    "key": o["key"],
                                    "kind": "charge",
                                    "input": o["payment_input"],
                                },
                                2,
                            )
                            trace("platform_returned", bucket=bid, s_key=sk, result=r)
                            checkpoint(
                                args, trace, "provider", {"bucket": bid, "s_key": sk, "result": r}
                            )
                            ans = await asyncio.to_thread(
                                cl.command,
                                bid,
                                "observe",
                                s_key=sk,
                                owner=owner,
                                work_epoch=claimed["work_epoch"],
                                provider_result=r,
                            )
                            trace("result_observed", bucket=bid, s_key=sk, result=ans)
                        elif args.role == "relay" and o["event"] and not o.get("handoff_confirmed"):
                            if int(time.time() * 1000) < o.get("next_delivery_attempt", 0):
                                continue
                            evidence = (
                                await asyncio.to_thread(
                                    http, cfg["consumer"] + "/receipt", o["event"]
                                )
                                if o["published"]
                                else {"receipt": None}
                            )
                            if evidence["receipt"]:
                                checkpoint(
                                    args,
                                    trace,
                                    "handoff_read",
                                    {"bucket": bid, "s_key": sk, "evidence": evidence},
                                )
                                ans = await asyncio.to_thread(
                                    cl.command,
                                    bid,
                                    "consumer_observed",
                                    s_key=sk,
                                    event=o["event"],
                                    receipt=evidence["receipt"],
                                    source=evidence["source"],
                                )
                                trace(
                                    "consumer_handoff_observed",
                                    bucket=bid,
                                    s_key=sk,
                                    receipt=evidence["receipt"],
                                    result=ans,
                                )
                                checkpoint(
                                    args,
                                    trace,
                                    "handoff_saved",
                                    {
                                        "bucket": bid,
                                        "s_key": sk,
                                        "evidence": evidence,
                                        "result": ans,
                                    },
                                )
                            else:
                                ack = await broker.publish(o["event"])
                                trace(
                                    "publish_confirmed",
                                    bucket=bid,
                                    s_key=sk,
                                    event=o["event"],
                                    **ack,
                                )
                                checkpoint(
                                    args,
                                    trace,
                                    "publish",
                                    {"bucket": bid, "s_key": sk, "event": o["event"]},
                                )
                                await asyncio.to_thread(
                                    cl.command,
                                    bid,
                                    "published",
                                    s_key=sk,
                                    event=o["event"],
                                    retry_after=int(time.time() * 1000)
                                    + int(cfg.get("delivery_retry_ms", 1000)),
                                )
                        elif (
                            args.role == "coordinate"
                            and o["bundle"]
                            and not o["reservation_ready"]
                            and o["result"] is None
                        ):
                            tx = b["transactions"].get("TX:" + o["p"])
                            if tx is None:
                                # Existing lab batches omit this option. Timed deployments
                                # fix the absolute deadline in the first committed begin.
                                timeout_ms = int(cfg.get("transaction_timeout_ms", 0))
                                deadline = (
                                    {"deadline_ms": int(time.time() * 1000) + timeout_ms}
                                    if timeout_ms > 0
                                    else {}
                                )
                                tx = await asyncio.to_thread(
                                    cl.command,
                                    bid,
                                    "tx_begin",
                                    s_key=sk,
                                    participants=[
                                        {"bucket": "b2", "sku": "SKU-A", "qty": 1},
                                        {"bucket": "b3", "sku": "SKU-B", "qty": 1},
                                    ],
                                    **deadline,
                                )
                            if "id" not in tx:
                                continue
                            if not tx["decision"] and tx.get("deadline_ms", 0) > 0:
                                observed_ms = int(time.time() * 1000)
                                if observed_ms >= tx["deadline_ms"]:
                                    tx = await asyncio.to_thread(
                                        cl.command,
                                        bid,
                                        "tx_timeout",
                                        tx_id=tx["id"],
                                        coordinator=bid,
                                        deadline_ms=tx["deadline_ms"],
                                        observed_ms=observed_ms,
                                    )
                                    trace("timeout_decision_read", tx=tx, observed_ms=observed_ms)
                            if not tx["decision"]:
                                for p in tx["participants"]:
                                    vote = await asyncio.to_thread(
                                        cl.command,
                                        p["bucket"],
                                        "prepare",
                                        tx_id=tx["id"],
                                        coordinator=bid,
                                        input={"sku": p["sku"], "qty": p["qty"]},
                                    )
                                    await asyncio.to_thread(
                                        cl.command,
                                        bid,
                                        "tx_vote",
                                        tx_id=tx["id"],
                                        participant=p["bucket"],
                                        vote=vote["vote"],
                                    )
                                trace("all_prepared", tx_id=tx["id"])
                                checkpoint(args, trace, "prepare", {"tx_id": tx["id"]})
                                tx = await asyncio.to_thread(
                                    cl.command, bid, "tx_decide", tx_id=tx["id"]
                                )
                            if not tx.get("decision"):
                                continue
                            trace("decision_read", tx=tx)
                            checkpoint(args, trace, "decision", tx)
                            completions = []
                            for p in tx["participants"]:
                                # Recovery reads the coordinator's committed decision; participants never timeout-abort.
                                current = (await asyncio.to_thread(cl.bucket, bid))["transactions"][
                                    tx["id"]
                                ]
                                finished = await asyncio.to_thread(
                                    cl.command,
                                    p["bucket"],
                                    "finish",
                                    tx_id=tx["id"],
                                    decision=current["decision"],
                                    coordinator=bid,
                                    input={"sku": p["sku"], "qty": p["qty"]},
                                )
                                expected = (
                                    "committed" if current["decision"] == "commit" else "aborted"
                                )
                                if finished.get("state") != expected:
                                    raise ValueError("participant did not finish")
                                completions.append(finished)
                            done = await asyncio.to_thread(
                                cl.command, bid, "tx_done", tx_id=tx["id"], completions=completions
                            )
                            if not done.get("done"):
                                raise ValueError("coordinator did not record completion")
                            trace("transaction_completed", tx_id=tx["id"])
        except Exception as exc:
            trace("retryable", error=repr(exc))
        await asyncio.sleep(0.08)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--role", required=True)
    p.add_argument("--config", required=True)
    p.add_argument("--port", type=int)
    p.add_argument("--db")
    p.add_argument("--trace", required=True)
    p.add_argument("--pause-after", default="")
    p.add_argument("--marker", default="")
    args = p.parse_args()
    cfg = json.loads(Path(args.config).read_text(encoding="utf-8"))
    trace = Trace(args.trace)
    if args.role in ("platform", "consumer-api", "proxy", "router", "dispatch-api"):
        run_http(args, cfg, trace)
    else:
        asyncio.run(run_worker(args, cfg, trace))
