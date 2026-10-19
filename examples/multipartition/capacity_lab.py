"""One-gateway capacity mechanism over the frozen full partition authorities.

This is an isolated teaching entry, not a distributed admission controller.
"""

import argparse
import json
import threading
import time
from concurrent.futures import Future
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from common import Cluster, Trace, canon, http, partition

ap = argparse.ArgumentParser()
ap.add_argument("--config", required=True)
ap.add_argument("--trace", required=True)
ap.add_argument("--port", type=int, required=True)
args = ap.parse_args()
cfg = json.loads(Path(args.config).read_text(encoding="utf-8"))
trace = Trace(args.trace)
routes = Cluster(cfg).refresh()
locks = {b: threading.Lock() for b in routes}
guard = threading.Lock()
active = {}
settings = {
    "new_limit_per_partition": 3,
    "accept_delay_ms": 200,
    "recovery_enabled": False,
    "recovery_interval_ms": 100,
    "platform_timeout": 2,
    "force_directory": False,
}
counts = {
    "http": 0,
    "new": 0,
    "retry": 0,
    "coalesced": 0,
    "busy": 0,
    "authority_accept": 0,
    "external_attempts": 0,
    "recovery_completed": 0,
}
turns = []
samples = []
recovery_owner = "ch21-fair-recovery"
request_slots = threading.BoundedSemaphore(64)


def client():
    cl = Cluster(cfg)
    cl.routes = {b: dict(x) for b, x in routes.items()}
    return cl


def timed(stage, fn):
    start = time.monotonic()
    result = fn()
    ms = (time.monotonic() - start) * 1000
    with guard:
        samples.append({"stage": stage, "ms": ms})
    return result


def submit(s, inp):
    sk = canon(s)
    bid = partition(s)
    with guard:
        counts["http"] += 1
        previous = active.get(sk)
        if previous and previous["input"] == inp:
            counts["coalesced"] += 1
            future = previous["future"]
            leader = False
        else:
            future = Future()
            active[sk] = {"input": inp, "future": future}
            leader = True
    if not leader:
        return future.result(timeout=15)
    try:
        # A single gateway owns these locks. A production cluster would need a
        # partition-authoritative quota or disjoint delegated token ranges.
        with locks[bid]:
            cl = client()
            if settings["force_directory"]:
                timed("directory", cl.refresh)
            b = timed("authority_read", lambda: cl.bucket(bid))
            old = b["operations"].get(sk)
            if old:
                with guard:
                    counts["retry"] += 1
                if old["input"] != inp:
                    result = {"code": "conflict"}
                else:
                    result = old["result"] or {
                        "code": "in_progress",
                        "purchase_id": old["p"],
                        "charge_id": old["c"],
                        "task_id": old["t"],
                    }
            else:
                pending = sum(o["result"] is None for o in b["operations"].values())
                if pending >= settings["new_limit_per_partition"]:
                    with guard:
                        counts["busy"] += 1
                    result = {"code": "busy", "retry_after_ms": 250, "accepted": False}
                else:
                    time.sleep(settings["accept_delay_ms"] / 1000)
                    result = timed(
                        "authority_accept",
                        lambda: cl.command(
                            bid, "accept", s=s, input=inp, input_version="external-v2", now_upper=1
                        ),
                    )
                    with guard:
                        counts["new"] += 1
                        counts["authority_accept"] += 1
            trace("admission", s=s, input=inp, result=result, partition=bid)
        future.set_result(result)
        return result
    except Exception as exc:
        future.set_exception(exc)
        raise
    finally:
        with guard:
            if active.get(sk, {}).get("future") is future:
                active.pop(sk, None)


def recover():
    while True:
        if settings["recovery_enabled"]:
            # Each round visits every partition, taking at most one eligible
            # responsibility per partition. New admission uses its own budget.
            for bid in sorted(routes):
                try:
                    cl = client()
                    b = cl.bucket(bid)
                    candidates = [
                        (sk, o)
                        for sk, o in b["operations"].items()
                        if o["result"] is None and o["reservation_ready"]
                    ]
                    if not candidates:
                        continue
                    sk, o = min(candidates, key=lambda pair: pair[1]["t"])
                    claimed = cl.command(bid, "claim", s_key=sk, owner=recovery_owner)
                    if "c" not in claimed:
                        continue
                    with guard:
                        counts["external_attempts"] += 1
                        turns.append({"partition": bid, "s": o["s"], "t": o["t"]})
                    pr = timed(
                        "platform",
                        lambda: http(
                            cfg["proxy"] + "/call",
                            {
                                "scope": o["scope"],
                                "key": o["key"],
                                "kind": "charge",
                                "input": o["payment_input"],
                            },
                            settings["platform_timeout"],
                        ),
                    )
                    ans = cl.command(
                        bid,
                        "observe",
                        s_key=sk,
                        owner=recovery_owner,
                        work_epoch=claimed["work_epoch"],
                        provider_result=pr,
                    )
                    trace(
                        "recovery",
                        partition=bid,
                        s=o["s"],
                        binding={k: o[k] for k in ("p", "c", "t", "scope", "key", "payment_input")},
                        provider_result=pr,
                        result=ans,
                    )
                    with guard:
                        counts["recovery_completed"] += 1
                except Exception as e:
                    trace("recovery_error", partition=bid, error=repr(e))
        time.sleep(settings["recovery_interval_ms"] / 1000)


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def do_GET(self):
        with guard:
            result = {
                "counts": dict(counts),
                "turns": list(turns),
                "samples": list(samples),
                "settings": dict(settings),
            }
        self.reply(result)

    def do_POST(self):
        body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        if self.path == "/config":
            with guard:
                settings.update(body)
            return self.reply(dict(settings))
        if not request_slots.acquire(blocking=False):
            return self.reply(
                {
                    "code": "busy",
                    "accepted": False,
                    "reason": "bounded_request_slots",
                    "retry_after_ms": 250,
                }
            )
        try:
            try:
                self.reply(submit(body["s"], body["input"]))
            except Exception as e:
                self.reply({"code": "transport_unknown", "error": repr(e)})
        finally:
            request_slots.release()

    def reply(self, result):
        data = canon(result).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)


class Server(ThreadingHTTPServer):
    request_queue_size = 128


threading.Thread(target=recover, daemon=True).start()
Server(("127.0.0.1", args.port), Handler).serve_forever()
