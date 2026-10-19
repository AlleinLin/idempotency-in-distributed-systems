import json
import threading
import time
from pathlib import Path
from urllib.request import Request, urlopen


def canon(v):
    return json.dumps(v, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def http(url, body=None, timeout=4):
    raw = None if body is None else canon(body).encode()
    with urlopen(
        Request(url, data=raw, headers={"Content-Type": "application/json"}), timeout=timeout
    ) as r:
        return json.load(r)


class Trace:
    def __init__(self, path):
        self.path = Path(path)
        self.lock = threading.Lock()
        self.seq = 0

    def __call__(self, event_name, **kw):
        with self.lock:
            self.seq += 1
            if "event" in kw:
                kw["envelope"] = kw.pop("event")
            with self.path.open("a", encoding="utf-8") as f:
                f.write(canon({"seq": self.seq, "event": event_name, **kw}) + "\n")


class Cluster:
    """Leader discovery is per group; route cache is control-plane state."""

    def __init__(self, config):
        self.cfg = config
        self.leaders = {}
        self.routes = {}

    def group(self, group, path, body=None, attempts=5):
        urls = self.cfg["groups"][group]
        last = None
        for n in range(attempts):
            candidates = [self.leaders[group]] if group in self.leaders else urls
            for url in candidates:
                try:
                    r = http(url + path, body, timeout=3)
                    if r.get("code") in ("not_leader", "unavailable", "unknown"):
                        if r.get("leader"):
                            self.leaders[group] = r["leader"]
                        else:
                            self.leaders.pop(group, None)
                        last = r
                        continue
                    self.leaders[group] = url
                    return r
                except Exception as exc:
                    last = repr(exc)
                    self.leaders.pop(group, None)
            time.sleep(0.08)
        raise RuntimeError(f"{group}{path} unavailable: {last}")

    def state(self, group):
        return self.group(group, "/state")

    def refresh(self):
        if self.cfg.get("directory", {}).get("kind") == "etcd":
            from coordination import EtcdDirectory

            self.routes, self.directory_revision = EtcdDirectory(self.cfg["directory"]).read()
            return self.routes
        self.routes = self.state("control")["routes"]
        return self.routes

    def command(self, bucket, kind, **kw):
        if bucket not in self.routes:
            self.refresh()
        for _ in range(4):
            route = self.routes[bucket]
            r = self.group(
                route["owner"],
                "/command",
                {"bucket": bucket, "epoch": route["epoch"], "kind": kind, **kw},
            )
            if r.get("code") in ("not_owner", "not_active", "stale_route"):
                self.refresh()
                time.sleep(0.03)
                continue
            return r
        return r

    def bucket(self, bucket):
        if bucket not in self.routes:
            self.refresh()
        route = self.routes[bucket]
        return self.state(route["owner"])["buckets"][bucket]

    def active_buckets(self):
        # Scans are local group reads, not a global per-operation directory lock.
        for group in self.cfg["groups"]:
            if group == "control":
                continue
            try:
                for bid, b in self.state(group)["buckets"].items():
                    if b["mode"] == "active":
                        self.routes[bid] = {"owner": group, "epoch": b["epoch"]}
                        yield bid, b
            except Exception:
                continue


def identity(account, key, action="purchase.create.external.v2", session="E1"):
    return {"account_id": account, "action": action, "session_id": session, "idempotency_key": key}


def purchase_input(bundle=False):
    i = {
        "product_id": "paper-001",
        "quantity": 1,
        "quote_id": "Q7",
        "currency": "CNY",
        "amount_minor": 3000,
        "payment_method": "saved-card-7",
    }
    if bundle:
        i["bundle"] = True
    return i


def partition(s):
    return "b" + str((s["account_id"] - 1) % 3 + 1)
