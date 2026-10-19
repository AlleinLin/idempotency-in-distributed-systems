"""Chapter 19 immutable-block/manifest mechanism. Not GFS or HDFS."""

import argparse
import base64
import hashlib
import json
import os
import re
import sqlite3
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.request import Request, urlopen


def canonical(v):
    return json.dumps(v, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()


def call(url, body=None):
    with urlopen(
        Request(
            url,
            data=None if body is None else canonical(body),
            headers={"Content-Type": "application/json"},
        ),
        timeout=5,
    ) as response:
        return json.load(response)


def digest(data):
    return hashlib.sha256(data).hexdigest()


def materialize(manifest):
    pieces = []
    for item in manifest["blocks"]:
        failures = []
        for replica in item["replicas"]:
            try:
                result = call(replica + "/get/" + item["sha256"])
                raw = base64.b64decode(result["data"], validate=True)
                if len(raw) != item["length"] or digest(raw) != item["sha256"]:
                    raise ValueError("block_digest")
                pieces.append(raw)
                break
            except Exception as exc:
                failures.append(type(exc).__name__)
        else:
            raise ValueError("unavailable_block:" + item["sha256"])
    data = b"".join(pieces)
    if len(data) != manifest["length"] or digest(data) != manifest["sha256"]:
        raise ValueError("image_digest")
    image = json.loads(data)
    if image["id"] != manifest["partition"] or image["epoch"] != manifest["epoch"]:
        raise ValueError("source_identity")
    required = {
        "id",
        "epoch",
        "mode",
        "migration",
        "next",
        "operations",
        "sessions",
        "stock",
        "prepared",
        "transactions",
        "refunds",
    }
    if set(image) != required:
        raise ValueError("incomplete_image_schema")
    return data, image


class Server(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, args):
        super().__init__(("127.0.0.1", args.port), Handler)
        self.args = args
        self.root = Path(args.directory)
        self.root.mkdir(parents=True, exist_ok=True)
        self.lock = threading.Lock()
        if args.role == "metadata":
            with sqlite3.connect(self.root / "metadata.sqlite") as db:
                db.execute("PRAGMA journal_mode=WAL")
                db.execute("PRAGMA synchronous=FULL")
                db.execute(
                    "CREATE TABLE IF NOT EXISTS images(partition TEXT PRIMARY KEY,generation INTEGER,manifest TEXT NOT NULL)"
                )
        self.journal = self.root / "requests.jsonl"

    def record(self, path, body, result):
        with self.lock:
            with self.journal.open("ab") as f:
                f.write(canonical({"path": path, "body": body, "result": result}) + b"\n")


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def do_GET(self):
        self.dispatch(None)

    def do_POST(self):
        try:
            body = json.loads(self.rfile.read(int(self.headers.get("Content-Length", "0"))))
        except Exception:
            return self.reply(400, {"code": "bad_json"})
        self.dispatch(body)

    def reply(self, status, value):
        data = canonical(value)
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def dispatch(self, body):
        try:
            status, result = self.perform(body)
        except Exception as exc:
            status, result = 422, {"code": "rejected", "reason": str(exc)}
        self.server.record(self.path, body, result)
        if body and body.get("drop_reply") and status == 200:
            self.close_connection = True
            return
        self.reply(status, result)

    def perform(self, body):
        srv = self.server
        if self.path == "/health":
            return 200, {"role": srv.args.role}
        if srv.args.role == "block":
            sha = self.path.rsplit("/", 1)[-1]
            if not re.fullmatch("[0-9a-f]{64}", sha):
                raise ValueError("digest_syntax")
            target = srv.root / (sha + ".block")
            if self.path.startswith("/get/"):
                raw = target.read_bytes()
                if digest(raw) != sha:
                    raise ValueError("corrupt_block")
                return 200, {"sha256": sha, "data": base64.b64encode(raw).decode()}
            raw = base64.b64decode(body["data"], validate=True)
            if digest(raw) != sha:
                raise ValueError("digest_conflict")
            with srv.lock:
                if target.exists():
                    if target.read_bytes() != raw:
                        if not self.path.startswith("/repair/"):
                            raise ValueError("existing_corruption")
                        temp = target.with_suffix(".repair")
                        with temp.open("wb") as f:
                            f.write(raw)
                            f.flush()
                            os.fsync(f.fileno())
                        os.replace(temp, target)
                        return 200, {"code": "repaired", "sha256": sha}
                    return 200, {"code": "existing", "sha256": sha}
                with target.open("xb") as f:
                    f.write(raw)
                    f.flush()
                    os.fsync(f.fileno())
            return 200, {"code": "stored", "sha256": sha}
        if self.path.startswith("/latest/"):
            partition = self.path.rsplit("/", 1)[-1]
            with sqlite3.connect(srv.root / "metadata.sqlite") as db:
                row = db.execute(
                    "SELECT generation,manifest FROM images WHERE partition=?", (partition,)
                ).fetchone()
            return (
                (200, {"generation": row[0], "manifest": json.loads(row[1])})
                if row
                else (404, {"code": "missing"})
            )
        if self.path != "/publish":
            raise ValueError("path")
        manifest = body["manifest"]
        # Publication requires two validated durable copies of every referenced block.
        for block in manifest["blocks"]:
            if len(set(block["replicas"])) < 2:
                raise ValueError("replication_requirement")
            for replica in block["replicas"]:
                data = base64.b64decode(
                    call(replica + "/get/" + block["sha256"])["data"], validate=True
                )
                if digest(data) != block["sha256"] or len(data) != block["length"]:
                    raise ValueError("invalid_replica")
        materialize(manifest)
        with sqlite3.connect(srv.root / "metadata.sqlite", isolation_level=None) as db:
            db.execute("PRAGMA synchronous=FULL")
            db.execute("BEGIN IMMEDIATE")
            row = db.execute(
                "SELECT generation,manifest FROM images WHERE partition=?", (manifest["partition"],)
            ).fetchone()
            generation = row[0] if row else 0
            if row and json.loads(row[1]) == manifest:
                db.execute("COMMIT")
                return 200, {"code": "existing", "generation": generation, "manifest": manifest}
            if generation != body["expected_generation"]:
                raise ValueError("generation_conflict")
            if row:
                old = json.loads(row[1])
                if manifest["epoch"] < old["epoch"] or (
                    manifest["epoch"] == old["epoch"]
                    and manifest["applied_index"] <= old["applied_index"]
                ):
                    raise ValueError("stale_source")
            db.execute(
                "INSERT INTO images VALUES(?,?,?) ON CONFLICT(partition) DO UPDATE SET generation=excluded.generation,manifest=excluded.manifest",
                (manifest["partition"], generation + 1, canonical(manifest).decode()),
            )
            db.execute("COMMIT")
        return 200, {"code": "published", "generation": generation + 1, "manifest": manifest}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--role", choices=["block", "metadata"], required=True)
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--directory", required=True)
    Server(parser.parse_args()).serve_forever()


if __name__ == "__main__":
    main()
