"""Independent resource. Fencing check, binding and effect share one SQLite txn."""

import argparse
import json
import sqlite3
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from common import canon

p = argparse.ArgumentParser()
p.add_argument("--db")
p.add_argument("--port", type=int)
a = p.parse_args()
with sqlite3.connect(a.db) as db:
    db.executescript(
        "CREATE TABLE fence(resource TEXT PRIMARY KEY, token INTEGER NOT NULL);"
        "CREATE TABLE binding(s TEXT PRIMARY KEY,input TEXT NOT NULL,result TEXT NOT NULL);"
        "CREATE TABLE effects(id INTEGER PRIMARY KEY AUTOINCREMENT,s TEXT UNIQUE NOT NULL,token INTEGER NOT NULL);"
    )


class H(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def reply(self, r):
        raw = canon(r).encode()
        self.send_response(200)
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def do_GET(self):
        with sqlite3.connect(a.db) as d:
            self.reply(
                {
                    t: d.execute("SELECT * FROM " + t).fetchall()
                    for t in ["fence", "binding", "effects"]
                }
            )

    def do_POST(self):
        q = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        with sqlite3.connect(a.db, isolation_level=None) as d:
            d.execute("BEGIN IMMEDIATE")
            current = d.execute(
                "SELECT token FROM fence WHERE resource=?", ("inventory-1",)
            ).fetchone()
            token = q["token"]
            if self.path == "/advance":
                if current and token < current[0]:
                    r = {"code": "stale_fence"}
                else:
                    d.execute(
                        "INSERT INTO fence VALUES(?,?) ON CONFLICT(resource) DO UPDATE SET token=excluded.token",
                        ("inventory-1", token),
                    )
                    r = {"code": "advanced", "token": token}
            elif not current or token != current[0]:
                r = {"code": "stale_fence", "current": current[0] if current else None}
            else:
                s, inp = canon(q["s"]), canon(q["input"])
                old = d.execute("SELECT input,result FROM binding WHERE s=?", (s,)).fetchone()
                if old:
                    r = json.loads(old[1]) if old[0] == inp else {"code": "conflict"}
                else:
                    n = d.execute("INSERT INTO effects(s,token) VALUES(?,?)", (s, token)).lastrowid
                    r = {"effect_id": n, "s": q["s"], "input": q["input"], "status": "committed"}
                    d.execute("INSERT INTO binding VALUES(?,?,?)", (s, inp, canon(r)))
            d.execute("COMMIT")
        self.reply(r)


ThreadingHTTPServer(("127.0.0.1", a.port), H).serve_forever()
