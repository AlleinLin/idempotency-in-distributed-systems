"""TCP/SQLite OCC demonstration, not an implementation of FaRM."""

import argparse
import json
import sqlite3
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path


def canon(x):
    return json.dumps(x, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def do_GET(self):
        with sqlite3.connect(self.server.db) as db:
            stock = db.execute("SELECT quantity,version FROM stock").fetchone()
            quote = db.execute("SELECT amount,version FROM quote").fetchone()
            bindings = [json.loads(row[0]) for row in db.execute("SELECT record FROM bindings")]
        self.reply(
            {
                "stock": {"quantity": stock[0], "version": stock[1]},
                "quote": {"amount_minor": quote[0], "version": quote[1]},
                "bindings": bindings,
            }
        )

    def reply(self, value):
        data = canon(value).encode()
        self.send_response(200)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_POST(self):
        body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        with sqlite3.connect(self.server.db, isolation_level=None) as db:
            db.execute("PRAGMA synchronous=FULL")
            db.execute("BEGIN IMMEDIATE")
            if self.path == "/quote":
                db.execute("UPDATE quote SET amount=?,version=version+1", (body["amount_minor"],))
                db.execute("COMMIT")
                return self.reply({"code": "changed"})
            skey = canon(body["s"])
            inp = body["input"]
            prior = db.execute("SELECT record FROM bindings WHERE s=?", (skey,)).fetchone()
            if prior:
                record = json.loads(prior[0])
                result = record["result"] if record["input"] == inp else {"code": "conflict"}
                db.execute("COMMIT")
                return self.reply(result)
            quantity, sv = db.execute("SELECT quantity,version FROM stock").fetchone()
            amount, qv = db.execute("SELECT amount,version FROM quote").fetchone()
            if body["stock_version"] != sv or body["quote_version"] != qv:
                db.execute("ROLLBACK")
                return self.reply({"code": "validation_conflict"})
            if quantity < 1 or amount != inp["amount_minor"]:
                db.execute("ROLLBACK")
                return self.reply({"code": "business_rejected"})
            number = db.execute("SELECT COALESCE(MAX(n),0)+1 FROM bindings").fetchone()[0]
            result = {
                "purchase_id": f"P{number}",
                "charge_id": f"C{number}",
                "task_id": f"T{number}",
                "status": "reservation_committed",
                "amount_minor": amount,
                "currency": "CNY",
                "product_id": inp["product_id"],
                "quantity": 1,
            }
            record = {"s": body["s"], "input": inp, "result": result}
            db.execute("UPDATE stock SET quantity=quantity-1,version=version+1")
            db.execute("INSERT INTO bindings VALUES(?,?,?)", (skey, number, canon(record)))
            db.execute("COMMIT")
        if body.get("drop_reply"):
            self.close_connection = True
        else:
            self.reply(result)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--db", type=Path, required=True)
    p.add_argument("--port", type=int, required=True)
    a = p.parse_args()
    a.db.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(a.db) as db:
        db.executescript(
            "PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; CREATE TABLE IF NOT EXISTS stock(quantity INTEGER,version INTEGER); CREATE TABLE IF NOT EXISTS quote(amount INTEGER,version INTEGER); CREATE TABLE IF NOT EXISTS bindings(s TEXT PRIMARY KEY,n INTEGER UNIQUE,record TEXT NOT NULL);"
        )
        if not db.execute("SELECT * FROM stock").fetchone():
            db.execute("INSERT INTO stock VALUES(1,1)")
            db.execute("INSERT INTO quote VALUES(3000,1)")
    server = ThreadingHTTPServer(("127.0.0.1", a.port), Handler)
    server.db = str(a.db)
    server.serve_forever()


if __name__ == "__main__":
    main()
