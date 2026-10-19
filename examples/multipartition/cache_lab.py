"""Chapter18 portable mechanism service, not a Redis or Memcached implementation."""

import argparse
import json
import sqlite3
import threading
import time
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


def canon(v):
    return json.dumps(v, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--role", choices=["authority", "cache"], required=True)
    ap.add_argument("--port", type=int, required=True)
    ap.add_argument("--db")
    args = ap.parse_args()
    lock = threading.RLock()
    entries = {}
    leases = {}
    counter = 0
    incarnation = uuid.uuid4().hex
    if args.role == "authority":
        db = sqlite3.connect(args.db, check_same_thread=False, isolation_level=None)
        db.executescript("""PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;
        CREATE TABLE IF NOT EXISTS binding(s TEXT PRIMARY KEY,input TEXT NOT NULL,result TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS effects(id INTEGER PRIMARY KEY,amount INTEGER NOT NULL);
        CREATE TABLE IF NOT EXISTS projection(id INTEGER PRIMARY KEY,revision INTEGER NOT NULL,state TEXT NOT NULL);""")

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *a):
            pass

        def do_GET(self):
            self.answer({})

        def do_POST(self):
            try:
                body = json.loads(self.rfile.read(int(self.headers.get("Content-Length", 0))))
            except Exception:
                self.send_error(400)
                return
            self.answer(body)

        def answer(self, b):
            nonlocal counter
            with lock:
                if self.path == "/health":
                    r = {"role": args.role, "incarnation": incarnation}
                elif args.role == "authority":
                    if self.path == "/accept":
                        s = b["s"]
                        inp = b["input"]
                        key = canon(s)
                        assert set(s) == {"account_id", "action", "session_id", "idempotency_key"}
                        db.execute("BEGIN IMMEDIATE")
                        old = db.execute(
                            "select input,result from binding where s=?", (key,)
                        ).fetchone()
                        if old:
                            r = json.loads(old[1]) if old[0] == canon(inp) else {"code": "conflict"}
                        else:
                            r = {
                                "s": s,
                                "input": inp,
                                "purchase_id": "P1",
                                "charge_id": "C1",
                                "work_id": "T1",
                                "provider_scope": "lab:merchant-7:test:charge.v1",
                                "provider_key": "pay-C1",
                                "payment_result": {
                                    "provider_charge_id": "X1",
                                    "amount_minor": 3000,
                                    "status": "succeeded",
                                },
                            }
                            db.execute(
                                "insert into binding values(?,?,?)", (key, canon(inp), canon(r))
                            )
                            db.execute("insert into effects values(1,3000)")
                            db.execute("insert into projection values(1,1,'paid')")
                        db.execute("COMMIT")
                    elif self.path == "/projection":
                        v = db.execute(
                            "select revision,state from projection where id=1"
                        ).fetchone()
                        r = {"purchase_id": "P1", "revision": v[0], "state": v[1]}
                    elif self.path == "/advance":
                        db.execute(
                            "update projection set revision=revision+1,state=? where id=1",
                            (b["state"],),
                        )
                        r = {"code": "advanced"}
                    elif self.path == "/ledger":
                        r = {
                            t: db.execute("select * from " + t).fetchall()
                            for t in ["binding", "effects", "projection"]
                        }
                    else:
                        r = {"code": "unknown"}
                else:
                    key = b.get("key", "P1")
                    entry = entries.get(key)
                    if entry and entry["expires"] <= time.monotonic():
                        entries.pop(key)
                        entry = None
                    if self.path == "/get":
                        r = (
                            {"value": entry["value"], "cas": entry["cas"]}
                            if entry
                            else {"miss": True}
                        )
                    elif self.path in {"/set", "/cas"}:
                        if self.path == "/cas" and (not entry or entry["cas"] != b["cas"]):
                            r = {"code": "NOT_FOUND" if not entry else "EXISTS"}
                        else:
                            counter += 1
                            entries[key] = {
                                "value": b["value"],
                                "cas": counter,
                                "expires": time.monotonic() + b.get("ttl", 60),
                            }
                            r = {"code": "STORED", "cas": counter}
                    elif self.path == "/invalidate":
                        entries.pop(key, None)
                        leases.pop(key, None)
                        r = {"code": "invalidated"}
                    elif self.path == "/lease_get":
                        if entry:
                            r = {"value": entry["value"], "cas": entry["cas"]}
                        elif key in leases and leases[key]["until"] > time.monotonic():
                            r = {"code": "busy"}
                        else:
                            counter += 1
                            token = incarnation + ":" + str(counter)
                            leases[key] = {
                                "token": token,
                                "until": time.monotonic() + b.get("lease_seconds", 30),
                            }
                            r = {"miss": True, "token": token}
                    elif self.path == "/lease_set":
                        lease = leases.get(key)
                        if (
                            not lease
                            or lease["token"] != b["token"]
                            or lease["until"] <= time.monotonic()
                        ):
                            r = {"code": "stale_lease"}
                        else:
                            counter += 1
                            entries[key] = {
                                "value": b["value"],
                                "cas": counter,
                                "expires": time.monotonic() + b.get("ttl", 60),
                            }
                            leases.pop(key)
                            r = {"code": "STORED", "cas": counter}
                    else:
                        r = {"code": "unknown"}
            raw = canon(r).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)

    ThreadingHTTPServer(("127.0.0.1", args.port), Handler).serve_forever()


if __name__ == "__main__":
    main()
