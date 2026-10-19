"""Native Runtime API protocol receiver/client and independent HTTP payment fixture.
The receiver is authored for this experiment. It is not AWS RIC, RIE or Lambda.
"""

import argparse
import json
import os
import socket
import sqlite3
import time
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path


def raw(x):
    return json.dumps(x, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()


def call(url, value=None):
    q = urllib.request.Request(
        url,
        data=raw(value) if value is not None else None,
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(q, timeout=20) as f:
            return f.status, json.load(f), dict(f.headers)
    except urllib.error.HTTPError as e:
        return e.code, json.load(e), dict(e.headers)


def platform(root, port):
    dbfile = root / "platform.sqlite"
    with sqlite3.connect(dbfile) as db:
        db.executescript(
            "PRAGMA journal_mode=WAL;PRAGMA synchronous=FULL;"
            "CREATE TABLE IF NOT EXISTS account(id INT PRIMARY KEY,balance INT);"
            "INSERT OR IGNORE INTO account VALUES (1,10000);"
            "CREATE TABLE IF NOT EXISTS effect(scope TEXT,key TEXT,input TEXT,result TEXT,PRIMARY KEY(scope,key));"
        )

    class H(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def do_GET(self):
            with sqlite3.connect(dbfile) as db:
                result = {
                    "balance": db.execute("SELECT balance FROM account WHERE id=1").fetchone()[0],
                    "effects": [
                        {"scope": s, "key": k, "input": json.loads(i), "result": json.loads(r)}
                        for s, k, i, r in db.execute("SELECT * FROM effect")
                    ],
                }
            self.send_response(200)
            self.end_headers()
            self.wfile.write(raw(result))

        def do_POST(self):
            x = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            with sqlite3.connect(dbfile, timeout=20) as db:
                db.execute("PRAGMA synchronous=FULL")
                db.execute("BEGIN IMMEDIATE")
                old = db.execute(
                    "SELECT input,result FROM effect WHERE scope=? AND key=?",
                    (x["scope"], x["key"]),
                ).fetchone()
                if old:
                    code = 200 if json.loads(old[0]) == x["input"] else 409
                    result = json.loads(old[1]) if code == 200 else {"code": "input_conflict"}
                else:
                    assert x["input"]["amount_minor"] == 3000 and x["input"]["currency"] == "CNY"
                    db.execute("UPDATE account SET balance=balance-3000 WHERE id=1")
                    result = {
                        "provider_charge_id": "X1",
                        "provider_scope": x["scope"],
                        "idempotency_key": x["key"],
                        "status": "succeeded",
                        "account_id": 1,
                        "amount_minor": 3000,
                        "currency": "CNY",
                    }
                    db.execute(
                        "INSERT INTO effect VALUES (?,?,?,?)",
                        (x["scope"], x["key"], raw(x["input"]).decode(), raw(result).decode()),
                    )
                    code = 200
                db.commit()
            if x.get("drop_reply"):
                self.close_connection = True
                self.connection.shutdown(socket.SHUT_RDWR)
                self.connection.close()
                return
            self.send_response(code)
            self.end_headers()
            self.wfile.write(raw(result))

    ThreadingHTTPServer(("127.0.0.1", port), H).serve_forever()


def runtime(root, port):
    dbfile = root / "runtime.sqlite"
    with sqlite3.connect(dbfile) as db:
        db.executescript(
            "PRAGMA journal_mode=WAL;PRAGMA synchronous=FULL;"
            "CREATE TABLE IF NOT EXISTS invocation(id INTEGER PRIMARY KEY AUTOINCREMENT,event TEXT,state TEXT,response TEXT);"
        )

    class H(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def send(self, code, value, headers=None):
            self.send_response(code)
            for k, v in (headers or {}).items():
                self.send_header(k, str(v))
            self.end_headers()
            self.wfile.write(raw(value))

        def do_GET(self):
            if self.path == "/2018-06-01/runtime/invocation/next":
                with sqlite3.connect(dbfile, timeout=20) as db:
                    db.execute("BEGIN IMMEDIATE")
                    row = db.execute(
                        "SELECT id,event FROM invocation WHERE state='pending' ORDER BY id LIMIT 1"
                    ).fetchone()
                    if row:
                        db.execute("UPDATE invocation SET state='inflight' WHERE id=?", (row[0],))
                    db.commit()
                if not row:
                    self.send(404, {"code": "empty"})
                    return
                self.send(
                    200,
                    json.loads(row[1]),
                    {
                        "Lambda-Runtime-Aws-Request-Id": f"request-{row[0]}",
                        "Lambda-Runtime-Deadline-Ms": int(time.time() * 1000) + 60000,
                        "Lambda-Runtime-Invoked-Function-Arn": "arn:aws:lambda:local:0:function:protocol-fixture",
                    },
                )
            else:
                with sqlite3.connect(dbfile) as db:
                    rows = db.execute(
                        "SELECT id,event,state,response FROM invocation ORDER BY id"
                    ).fetchall()
                self.send(
                    200,
                    [
                        {
                            "id": i,
                            "event": json.loads(e),
                            "state": s,
                            "response": json.loads(r) if r else None,
                        }
                        for i, e, s, r in rows
                    ],
                )

        def do_POST(self):
            x = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            with sqlite3.connect(dbfile, timeout=20) as db:
                db.execute("PRAGMA synchronous=FULL")
                if self.path == "/enqueue":
                    db.execute(
                        "INSERT INTO invocation(event,state) VALUES (?,'pending')",
                        (raw(x).decode(),),
                    )
                else:
                    request_id = self.path.split("/")[-2]
                    db.execute(
                        "UPDATE invocation SET response=?,state='responded' WHERE id=?",
                        (raw(x).decode(), int(request_id.split("-")[1])),
                    )
                db.commit()
            self.send(202, {"saved": True})

    ThreadingHTTPServer(("127.0.0.1", port), H).serve_forever()


def execute(event, provider, drop=False):
    status, value, _ = call(
        provider + "/charge",
        {"scope": event["scope"], "key": event["key"], "input": event["input"], "drop_reply": drop},
    )
    if status != 200:
        raise ValueError(value)
    payment = {
        "purchase_id": event["p"],
        "charge_id": event["c"],
        "order_id": event["o"],
        "provider_charge_id": value["provider_charge_id"],
        "provider_scope": event["scope"],
        "status": "succeeded",
        "amount_minor": event["input"]["amount_minor"],
        "currency": "CNY",
    }
    return {
        "s": event["s"],
        "input": event["input"],
        "p": event["p"],
        "c": event["c"],
        "t": event["t"],
        "scope": event["scope"],
        "key": event["key"],
        "provider_result": value,
        "result": {
            "result_version": "purchase-result-external-v2",
            "acceptance": event["acceptance"],
            "payment_result": payment,
            "progress": {"business_state": "paid", "fulfillment_state": "unfulfilled"},
        },
    }


def client(root, runtime_url, provider, pause, count):
    memory_counter = 0
    for _ in range(count):
        memory_counter += 1
        status, event, headers = call(runtime_url + "/2018-06-01/runtime/invocation/next")
        assert status == 200
        request_id = headers["Lambda-Runtime-Aws-Request-Id"]
        if pause:
            try:
                execute(event, provider, drop=True)
            except OSError:
                pass
            (root / "first-client-unknown.ready").write_text(str(os.getpid()))
            while True:
                time.sleep(0.05)
        result = execute(event, provider)
        result["execution"] = {
            "request_id": request_id,
            "pid": os.getpid(),
            "memory_counter": memory_counter,
        }
        call(runtime_url + f"/2018-06-01/runtime/invocation/{request_id}/response", result)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("mode", choices=["platform", "runtime", "client"])
    p.add_argument("--root", type=Path, required=True)
    p.add_argument("--port", type=int)
    p.add_argument("--runtime")
    p.add_argument("--provider")
    p.add_argument("--pause", action="store_true")
    p.add_argument("--count", type=int, default=1)
    a = p.parse_args()
    if a.mode == "platform":
        platform(a.root, a.port)
    elif a.mode == "runtime":
        runtime(a.root, a.port)
    else:
        client(a.root, a.runtime, a.provider, a.pause, a.count)
