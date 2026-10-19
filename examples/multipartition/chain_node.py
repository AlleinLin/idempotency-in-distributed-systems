"""Bounded fail-stop Chain Replication comparator over actual TCP.

The external controller serializes reconfiguration and kills removed processes.
This is not a production failure detector or a Byzantine membership protocol.
"""

import argparse
import json
import os
import socket
import socketserver
import threading
import time
from pathlib import Path

from common import canon


def rpc(port, body, timeout=5):
    with socket.create_connection(("127.0.0.1", port), timeout=timeout) as s:
        s.sendall(canon(body).encode() + b"\n")
        return json.loads(s.makefile("rb").readline())


p = argparse.ArgumentParser()
p.add_argument("--port", type=int)
p.add_argument("--dir")
a = p.parse_args()
root = Path(a.dir)
root.mkdir(exist_ok=True)
path = root / "state.json"
lock = threading.RLock()
state = (
    json.loads(path.read_text())
    if path.exists()
    else {
        "epoch": 0,
        "head": False,
        "tail": False,
        "next": None,
        "records": [],
        "sent": [],
        "pause": False,
    }
)


def persist():
    tmp = path.with_suffix(".tmp")
    with tmp.open("w") as f:
        f.write(canon(state))
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


def forward(record):
    with lock:
        epoch, nxt, tail = state["epoch"], state["next"], state["tail"]
    if tail:
        answer = record["result"]
    else:
        answer = rpc(nxt, {"kind": "replicate", "epoch": epoch, "record": record})
        if answer.get("code"):
            return answer
    with lock:
        if record["sn"] in state["sent"]:
            state["sent"].remove(record["sn"])
            persist()
    return answer


def handle(q):
    kind = q["kind"]
    if kind == "config":
        with lock:
            if q["epoch"] <= state["epoch"]:
                return {"code": "stale_config"}
            for k in ["epoch", "head", "tail", "next"]:
                state[k] = q[k]
            persist()
            return {"code": "configured"}
    if kind == "pause":
        with lock:
            state["pause"] = q["value"]
            persist()
        return {"code": "paused" if q["value"] else "resumed"}
    if kind == "state":
        with lock:
            return json.loads(canon(state))
    if kind == "query":
        with lock:
            if not state["tail"] or q["epoch"] != state["epoch"]:
                return {"code": "not_current_tail"}
            return {"records": state["records"], "value": len(state["records"])}
    if kind == "repair":
        with lock:
            records = [r for r in state["records"] if r["sn"] > q["successor_sn"]]
        for r in records:
            answer = forward(r)
            if answer.get("code"):
                return answer
        return {"code": "repaired", "suffix": [r["sn"] for r in records]}
    with lock:
        if q["epoch"] != state["epoch"]:
            return {"code": "stale_epoch"}
        if kind == "update":
            if not state["head"]:
                return {"code": "not_head"}
            old = next((r for r in state["records"] if r["s"] == q["s"]), None)
            if old:
                if old["input"] != q["input"]:
                    return {"code": "conflict"}
                record = old
            else:
                sn = len(state["records"]) + 1
                record = {
                    "sn": sn,
                    "s": q["s"],
                    "input": q["input"],
                    "result": {"receipt": "R" + str(sn), "s": q["s"], "input": q["input"]},
                }
                state["records"].append(record)
                state["sent"].append(sn)
                persist()
        elif kind == "replicate":
            record = q["record"]
            sn = record["sn"]
            if sn <= len(state["records"]):
                if state["records"][sn - 1] != record:
                    return {"code": "sequence_conflict"}
            elif sn == len(state["records"]) + 1:
                state["records"].append(record)
                state["sent"].append(sn)
                persist()
            else:
                return {"code": "sequence_gap"}
        else:
            return {"code": "unknown"}
    # The marker proves persistence before the controlled crash.
    while state["pause"]:
        (root / "paused.json").write_text(canon({"sn": record["sn"], "port": a.port}))
        time.sleep(0.03)
    return forward(record)


class H(socketserver.StreamRequestHandler):
    def handle(self):
        try:
            r = handle(json.loads(self.rfile.readline()))
        except Exception as e:
            r = {"code": "unknown", "error": repr(e)}
        try:
            self.wfile.write(canon(r).encode() + b"\n")
        except OSError:
            pass


class Server(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True


Server(("127.0.0.1", a.port), H).serve_forever()
