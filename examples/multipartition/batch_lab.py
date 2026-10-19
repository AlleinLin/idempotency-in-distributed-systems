"""Independent native MapReduce mechanism service. Not Hadoop or Spark."""

import argparse
import hashlib
import json
import os
import socket
import sqlite3
import time
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path


def raw(x):
    return json.dumps(x, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()


def sha(x):
    return hashlib.sha256(x).hexdigest()


def read(p):
    return json.loads(Path(p).read_text(encoding="utf-8"))


def call(url, x):
    with urllib.request.urlopen(
        urllib.request.Request(url, data=raw(x), headers={"Content-Type": "application/json"}),
        timeout=15,
    ) as f:
        return json.load(f)


def serve(root, port):
    dbpath = root / "master.sqlite"
    with sqlite3.connect(dbpath) as db:
        db.executescript(
            "PRAGMA journal_mode=WAL;PRAGMA synchronous=FULL;"
            "CREATE TABLE IF NOT EXISTS generation(task TEXT PRIMARY KEY,n INT NOT NULL);"
            "CREATE TABLE IF NOT EXISTS winner(task TEXT PRIMARY KEY,generation INT,attempt TEXT,path TEXT,digest TEXT);"
            "CREATE TABLE IF NOT EXISTS publication(id INT PRIMARY KEY,manifest TEXT NOT NULL);"
        )

    class H(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def do_GET(self):
            with sqlite3.connect(dbpath) as db:
                d = {
                    "winners": [
                        dict(zip(("task", "generation", "attempt", "path", "digest"), x))
                        for x in db.execute("SELECT * FROM winner ORDER BY task")
                    ],
                    "publication": [
                        json.loads(x[0]) for x in db.execute("SELECT manifest FROM publication")
                    ],
                }
            b = raw(d)
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b)

        def do_POST(self):
            x = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            with sqlite3.connect(dbpath, timeout=20) as db:
                db.execute("PRAGMA synchronous=FULL")
                db.execute("BEGIN IMMEDIATE")
                if self.path == "/commit":
                    path = Path(x["path"]).resolve()
                    if (
                        not path.is_relative_to(root)
                        or not path.is_file()
                        or sha(path.read_bytes()) != x["digest"]
                    ):
                        result = {"code": "missing_or_bad_output"}
                    else:
                        task = x["task"]
                        db.execute("INSERT OR IGNORE INTO generation VALUES (?,1)", (task,))
                        gen = db.execute(
                            "SELECT n FROM generation WHERE task=?", (task,)
                        ).fetchone()[0]
                        old = db.execute(
                            "SELECT generation,attempt,path,digest FROM winner WHERE task=?",
                            (task,),
                        ).fetchone()
                        if x["generation"] != gen:
                            result = {"code": "stale_attempt"}
                        elif old:
                            result = {
                                "code": "existing"
                                if old == (gen, x["attempt"], str(path), x["digest"])
                                else "lost",
                                "winner": old[1],
                            }
                        else:
                            db.execute(
                                "INSERT INTO winner VALUES (?,?,?,?,?)",
                                (task, gen, x["attempt"], str(path), x["digest"]),
                            )
                            result = {"code": "won", "winner": x["attempt"]}
                elif self.path == "/invalidate":
                    old = db.execute(
                        "SELECT path FROM winner WHERE task=?", (x["task"],)
                    ).fetchone()
                    if old and not Path(old[0]).exists():
                        db.execute("DELETE FROM winner WHERE task=?", (x["task"],))
                        db.execute("UPDATE generation SET n=n+1 WHERE task=?", (x["task"],))
                        result = {"code": "invalidated"}
                    else:
                        result = {"code": "reject"}
                elif self.path == "/publish":
                    row = db.execute(
                        "SELECT path,digest FROM winner WHERE task='reduce-0'"
                    ).fetchone()
                    if not row or not Path(row[0]).is_file():
                        result = {"code": "incomplete"}
                    else:
                        manifest = {
                            "job_id": x["job_id"],
                            "input_sha256": x["input_sha256"],
                            "output": row[0],
                            "output_sha256": row[1],
                        }
                        old = db.execute("SELECT manifest FROM publication WHERE id=1").fetchone()
                        if old:
                            result = {
                                "code": "existing"
                                if json.loads(old[0]) == manifest
                                else "conflict",
                                "manifest": json.loads(old[0]),
                            }
                        else:
                            db.execute(
                                "INSERT INTO publication VALUES (1,?)", (raw(manifest).decode(),)
                            )
                            result = {"code": "published", "manifest": manifest}
                else:
                    result = {"code": "unknown"}
                db.commit()
            # The persistent commit has happened; deliberately close the actual HTTP
            # connection without writing a status line for one selected response.
            if x.get("drop_reply"):
                self.close_connection = True
                self.connection.shutdown(socket.SHUT_RDWR)
                self.connection.close()
                return
            b = raw(result)
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b)

    ThreadingHTTPServer(("127.0.0.1", port), H).serve_forever()


def worker(spec):
    x = read(spec)
    out = Path(x["path"])
    out.parent.mkdir(parents=True, exist_ok=True)
    if x["kind"] == "map":
        value = read(x["input"])
        mapped = [{"kind": value["side"], "record": row} for row in value["rows"]]
    else:
        records = []
        for path in x["inputs"]:
            records.extend(read(path))
        ops = [z["record"] for z in records if z["kind"] == "authority"]
        effects = {
            (z["record"]["scope"], z["record"]["key"]): z["record"]
            for z in records
            if z["kind"] == "platform"
        }
        mapped = []
        for op in sorted(ops, key=lambda o: raw(o["s"])):
            effect = effects.get((op["scope"], op["key"]))
            state = (op["result"] or {}).get("progress", {}).get("business_state")
            if effect:
                provider = json.loads(effect["result"])
                payment = op["payment_result"] or {}
                matched = (
                    state == "paid"
                    and effect["kind"] == "charge"
                    and effect["account_id"] == op["s"]["account_id"]
                    and effect["amount"] == op["input"]["amount_minor"]
                    and provider == op["provider_result"]
                    and provider.get("currency") == op["input"]["currency"]
                    and provider.get("charge_id") == op["c"]
                    and provider.get("scope") == op["scope"]
                    and provider.get("key") == op["key"]
                    and provider.get("status") == "succeeded"
                    and all(
                        payment.get(k) == provider.get(k)
                        for k in (
                            "amount_minor",
                            "currency",
                            "charge_id",
                            "provider_charge_id",
                            "status",
                        )
                    )
                    and payment.get("purchase_id") == op["p"]
                    and op["result"].get("payment_result") == payment
                )
                finding = "matched" if matched else "mismatch"
            else:
                finding = (
                    "business_rejected"
                    if state == "rejected" and op["payment_result"] is None
                    else "unresolved"
                )
            mapped.append(
                {
                    "s": op["s"],
                    "input": op["input"],
                    "p": op["p"],
                    "c": op["c"],
                    "t": op["t"],
                    "provider_scope": op["scope"],
                    "provider_key": op["key"],
                    "original_result": op["result"],
                    "payment_result": op["payment_result"],
                    "provider_result": op["provider_result"],
                    "platform_effect": effect,
                    "finding": finding,
                }
            )
    b = raw(mapped)
    with out.open("wb") as f:
        f.write(b)
        f.flush()
        os.fsync(f.fileno())
    Path(x["ready"]).write_text(str(os.getpid()))
    if x.get("release"):
        while not Path(x["release"]).exists():
            time.sleep(0.05)
    response = call(
        x["master"] + "/commit",
        {k: x[k] for k in ("task", "generation", "attempt", "path")}
        | {"digest": sha(b), "drop_reply": x.get("drop_reply", False)},
    )
    Path(x["response"]).write_bytes(raw(response))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("mode", choices=["master", "worker"])
    p.add_argument("--root", type=Path)
    p.add_argument("--port", type=int)
    p.add_argument("--spec", type=Path)
    a = p.parse_args()
    if a.mode == "master":
        serve(a.root.resolve(), a.port)
    else:
        worker(a.spec)
