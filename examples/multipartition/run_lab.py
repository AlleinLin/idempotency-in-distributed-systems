"""Cold local deployment. Uses only its recorded ports and child PIDs."""

import argparse
import json
import socket
import subprocess
import sys
import threading
import time
import traceback
from pathlib import Path

from common import Cluster, Trace, canon, http, identity, partition, purchase_input
from evidence import capture_configuration, capture_sources
from runtime import authority_binary, nats_server

ROOT = Path(__file__).parent
PY = Path(sys.executable)


def wait_for(fn, seconds=25, label="condition"):
    until = time.monotonic() + seconds
    last = None
    while time.monotonic() < until:
        try:
            value = fn()
            if value:
                return value
        except Exception as exc:
            last = repr(exc)
        time.sleep(0.12)
    raise TimeoutError(f"{label}: {last}")


class Lab:
    def __init__(self, out, port_base=36100, broker="nats", external_dispatch=False):
        self.out = out.resolve()
        if self.out.exists():
            raise ValueError("run directory must be new")
        self.out.mkdir(parents=True)
        capture_sources(ROOT, self.out)
        self.trace = Trace(self.out / "history.jsonl")
        self.processes = {}
        self.logs = {}
        self.nodes = {}
        self.cfg = {"groups": {}}
        self.config_path = self.out / "config.json"
        self.port_base = port_base
        self.broker_kind = broker
        self.external_dispatch = external_dispatch
        self.native = None

    def start(self, name, cmd):
        log = (self.out / (name + ".log")).open("ab")
        p = subprocess.Popen([str(x) for x in cmd], stdout=log, stderr=subprocess.STDOUT, cwd=ROOT)
        self.processes[name] = (p, cmd)
        self.logs[name] = log
        self.trace("process_start", name=name, pid=p.pid, command=[str(x) for x in cmd])
        return p

    def stop(self, name):
        p, cmd = self.processes[name]
        if p.poll() is None:
            p.kill()
            p.wait(timeout=10)
        self.trace("process_exit", name=name, pid=p.pid, returncode=p.returncode)

    def restart(self, name):
        _, cmd = self.processes[name]
        return self.start(name, cmd)

    def svc(self, role, name=None, port=None, db=None, pause="", marker=None):
        name = name or role
        cmd = [
            PY,
            "-X",
            "utf8",
            ROOT / "services.py",
            "--role",
            role,
            "--config",
            self.config_path,
            "--trace",
            self.out / (name + ".jsonl"),
        ]
        if port:
            cmd += ["--port", port]
        if db:
            cmd += ["--db", db]
        if pause:
            cmd += ["--pause-after", pause, "--marker", marker]
        return self.start(name, cmd)

    def initialize(self):
        base = self.port_base
        ports = (
            list(range(base, base + 12))
            + list(range(base + 100, base + 112))
            + [base + 200, base + 201, base + 210, base + 211, base + 212, base + 213]
        )
        if self.external_dispatch:
            ports.append(base + 214)
        for port in ports:
            with socket.socket() as s:
                s.bind(("127.0.0.1", port))
        (self.out / "port-ownership.json").write_text(
            canon({"ports": ports, "owner": "multipartition", "run": str(self.out)}),
            encoding="utf-8",
        )
        for gi, g in enumerate(["control", "G1", "G2", "G3"]):
            peers = [
                {
                    "id": f"{g}-{j}",
                    "raft": f"127.0.0.1:{base + gi * 3 + j}",
                    "http": f"http://127.0.0.1:{base + 100 + gi * 3 + j}",
                }
                for j in range(3)
            ]
            self.cfg["groups"][g] = [p["http"] for p in peers]
            for j, peer in enumerate(peers):
                data = self.out / peer["id"]
                cmd = [
                    authority_binary(),
                    "--id",
                    peer["id"],
                    "--group",
                    g,
                    "--raft",
                    peer["raft"],
                    "--http",
                    peer["http"].removeprefix("http://"),
                    "--dir",
                    data,
                    "--peers",
                    canon(peers),
                ]
                if j == 0:
                    cmd += ["--bootstrap"]
                self.nodes[peer["id"]] = peer
                self.start(peer["id"], cmd)
        self.cfg.update(
            platform=f"http://127.0.0.1:{base + 210}",
            proxy=f"http://127.0.0.1:{base + 211}",
            consumer=f"http://127.0.0.1:{base + 212}",
            router=f"http://127.0.0.1:{base + 213}",
            nats=f"nats://127.0.0.1:{base + 200}",
        )
        if self.broker_kind != "nats":
            from native_brokers import NativeBrokers

            self.native = NativeBrokers(self.broker_kind, self.out / "brokers", self.trace)
            self.cfg["broker"] = self.native.initialize()
        if self.external_dispatch:
            self.cfg["dispatch"] = f"http://127.0.0.1:{base + 214}"
        self.config_path.write_text(canon(self.cfg), encoding="utf-8")
        capture_configuration(self.out)
        self.cl = Cluster(self.cfg)
        for g in self.cfg["groups"]:
            wait_for(lambda: self.cl.state(g), label=g + " leader")
        for i in range(1, 4):
            bid = f"b{i}"
            group = f"G{i}"
            stock = {"SKU-A": 4} if i == 2 else {"SKU-B": 4} if i == 3 else {}
            assert (
                self.cl.group(
                    group, "/command", {"kind": "init", "bucket": bid, "epoch": 1, "stock": stock}
                )["code"]
                == "created"
            )
            self.cl.group(
                "control",
                "/command",
                {
                    "kind": "route",
                    "bucket": bid,
                    "route": {"owner": group, "epoch": 1, "migration": "", "sha256": ""},
                },
            )
        self.cl.refresh()
        for account in range(1, 25):
            s = identity(account, "setup")
            self.cl.command(partition(s), "session", s=s, deadline=1000)
        self.cl.command(
            "b1",
            "session",
            s=identity(1, "setup", action="refund.create.v1", session="RF-E1"),
            deadline=1000,
        )
        if self.broker_kind == "nats":
            self.start(
                "nats",
                [
                    nats_server(),
                    "-a",
                    "127.0.0.1",
                    "-p",
                    base + 200,
                    "-m",
                    base + 201,
                    "-js",
                    "-sd",
                    self.out / "nats-data",
                ],
            )
        self.svc("platform", port=base + 210, db=self.out / "platform.sqlite")
        self.svc("consumer-api", port=base + 212, db=self.out / "consumer.sqlite")
        self.svc("proxy", port=base + 211)
        self.svc("router", port=base + 213)
        if self.external_dispatch:
            self.svc("dispatch-api", port=base + 214, db=self.out / "dispatch.sqlite")
        for key in ("platform", "consumer", "proxy", "router") + (
            ("dispatch",) if self.external_dispatch else ()
        ):
            wait_for(lambda key=key: http(self.cfg[key] + "/health"), label=key)
        self.svc("pay")
        self.svc("relay")
        self.svc("consume")
        if self.external_dispatch:
            self.svc("dispatch")
        self.trace("deployment_ready", config=self.cfg)

    def invoke(self, s, inp=None, router=True):
        inp = inp or purchase_input()
        invocation = f"invoke-{threading.get_ident()}-{time.monotonic_ns()}"
        self.trace("invocation", id=invocation, s=s, input=inp)
        try:
            r = (
                http(self.cfg["router"] + "/accept", {"s": s, "input": inp}, timeout=8)
                if router
                else self.cl.command(
                    partition(s), "accept", s=s, input=inp, input_version="external-v2", now_upper=1
                )
            )
            self.trace("response", id=invocation, s=s, result=r)
            return r
        except Exception as exc:
            self.trace("unknown_response", id=invocation, error=repr(exc))
            raise

    def operation(self, s):
        return self.cl.bucket(partition(s))["operations"].get(canon(s))

    def paid(self, s):
        return wait_for(
            lambda: (self.operation(s) or {}).get("result"), seconds=35, label="paid " + canon(s)
        )

    def fulfilled(self, s):
        op = self.operation(s)

        def done():
            ledger = http(self.cfg["consumer"] + "/ledger")
            row = next((r for r in ledger["inbox"] if r["event_id"] == op["event_id"]), None)
            published = (self.operation(s) or {}).get("published")
            handoff = (self.operation(s) or {}).get("handoff_confirmed")
            return (
                row
                if published
                and handoff
                and row
                and any(d["f"] == row["f"] and d["result"] for d in ledger["dispatch"])
                else None
            )

        return wait_for(done, seconds=30, label="fulfilled")

    def save(self, name):
        data = {
            "groups": {g: self.cl.state(g) for g in self.cfg["groups"]},
            "platform": http(self.cfg["platform"] + "/ledger"),
            "consumer": http(self.cfg["consumer"] + "/ledger"),
        }
        if self.external_dispatch:
            data["external_dispatch"] = http(self.cfg["dispatch"] + "/ledger")
        (self.out / (name + ".json")).write_text(canon(data), encoding="utf-8")
        self.trace("snapshot", name=name)
        return data

    def leader_name(self, group):
        for name, p in self.nodes.items():
            if name.startswith(group + "-") and http(p["http"] + "/status")["state"] == "Leader":
                return name
        raise RuntimeError("no leader")

    def cases(self):
        self.baseline = []
        for account in (1, 2, 3):
            s = identity(account, "K1")
            self.invoke(s)
            self.paid(s)
            self.fulfilled(s)
            self.baseline.append(s)
        self.save("01-baseline")
        s = self.baseline[0]
        original = self.operation(s)
        inp = purchase_input()
        inp["amount_minor"] = 3500
        assert self.invoke(s, inp)["code"] == "conflict"
        assert self.invoke(s) == original["result"]
        fresh = identity(1, "K2")
        self.invoke(fresh)
        self.paid(fresh)
        self.fulfilled(fresh)
        self.save("02-identity")
        # Every data group is independently replicated and actually fails over.
        for i, account in enumerate((7, 8, 9), 1):
            g = f"G{i}"
            leader = self.leader_name(g)
            before = http(self.nodes[leader]["http"] + "/status")
            self.stop(leader)
            self.cl.leaders.pop(g, None)
            wait_for(lambda: self.cl.state(g), label=g + " failover")
            s = identity(account, "failover")
            self.invoke(s)
            self.paid(s)
            self.fulfilled(s)
            assert (
                self.invoke(self.baseline[i - 1]) == self.operation(self.baseline[i - 1])["result"]
            )
            self.restart(leader)
            wait_for(
                lambda: (
                    http(self.nodes[leader]["http"] + "/status")["stats"]["applied_index"] != "0"
                ),
                label="old leader recovery",
            )
            self.trace(
                "group_failover_complete",
                group=g,
                old_leader=leader,
                before=before,
                after=self.cl.state(g)["applied_index"],
            )
        self.save("03-failover-all-groups")
        # Actual HTTP request stays in proxy after worker dies; new worker uses original key.
        self.stop("pay")
        self.svc("pay", name="pay-held")
        http(self.cfg["proxy"] + "/config", {"hold": True, "drop_responses": 1})
        s = identity(13, "held-platform")
        self.invoke(s)
        wait_for(
            lambda: next(
                (
                    e
                    for e in http(self.cfg["proxy"] + "/events")["events"]
                    if e.get("body") and e["body"].get("input", {}).get("account_id") == 13
                ),
                None,
            ),
            label="held actual request",
        )
        self.stop("pay-held")
        before = http(self.cfg["platform"] + "/ledger")
        assert all(e["account_id"] != 13 for e in before["effects"])
        self.svc("pay", name="pay-recovered")
        http(self.cfg["proxy"] + "/config", {"hold": False})
        self.paid(s)
        self.fulfilled(s)
        self.save("04-platform-inflight")
        # Publish is confirmed, process dies before authoritative publication mark.
        self.stop("relay")
        marker = self.out / "relay-paused.json"
        self.svc("relay", name="relay-paused", pause="publish", marker=marker)
        s = identity(14, "relay-crash")
        self.invoke(s)
        self.paid(s)
        wait_for(marker.exists, label="publish checkpoint")
        self.stop("relay-paused")
        self.svc("relay", name="relay-recovered")
        self.fulfilled(s)
        # Consumer commit occurs before ACK, then process actually exits.
        if self.external_dispatch:
            self.stop("dispatch")
            self.svc(
                "dispatch",
                name="dispatch-paused",
                pause="dispatch_provider",
                marker=self.out / "dispatch-paused.json",
            )
        self.stop("consume")
        marker = self.out / "consume-paused.json"
        self.svc("consume", name="consume-paused", pause="consume", marker=marker)
        s = identity(15, "ack-crash")
        self.invoke(s)
        self.paid(s)
        wait_for(marker.exists, label="consumer checkpoint")
        self.stop("consume-paused")
        self.svc("consume", name="consume-recovered")
        if self.external_dispatch:
            wait_for(
                (self.out / "dispatch-paused.json").exists, label="external dispatch checkpoint"
            )
            self.stop("dispatch-paused")
            self.svc("dispatch", name="dispatch-recovered")
        self.fulfilled(s)
        time.sleep(1.2)
        self.save("05-real-message-crashes")
        # Coordinator durable prepare, then coordinator process termination.
        s = identity(10, "bundle")
        self.invoke(s, purchase_input(bundle=True))
        marker = self.out / "coordinator-prepared.json"
        self.svc("coordinate", name="coordinate-prepared", pause="prepare", marker=marker)
        wait_for(marker.exists, label="all prepared")
        self.stop("coordinate-prepared")
        b = self.cl.bucket("b1")
        assert b["operations"][canon(s)]["result"] is None
        freeze = self.cl.command("b2", "freeze", migration="blocked-prepared", epoch=1)
        assert freeze["code"] == "prepared_blocks_migration"
        # A participant leader is killed while it holds prepared stock.
        leader = self.leader_name("G2")
        self.stop(leader)
        self.cl.leaders.pop("G2", None)
        wait_for(lambda: self.cl.state("G2"), label="prepared participant failover")
        marker = self.out / "coordinator-decision.json"
        self.svc("coordinate", name="coordinate-decided", pause="decision", marker=marker)
        wait_for(marker.exists, label="durable decision")
        self.stop("coordinate-decided")
        self.save("06-commit-decision-before-notify")
        self.restart(leader)
        self.svc("coordinate", name="coordinate-recovered")
        self.paid(s)
        self.fulfilled(s)
        self.save("07-cross-partition-complete")
        # Freeze applies to all state mutations. Concurrent old route calls are kept.
        self.migrate_with_requests()
        # An entire logical region's nodes are isolated at actual TCP transport.
        isolated = []
        for g in ("G1", "G2", "G3", "control"):
            name = f"{g}-0"
            url = self.nodes[name]["http"]
            http(url + "/fault", {"isolated": True})
            isolated.append(name)
        self.trace("logical_region_isolated", nodes=isolated)
        for g in self.cfg["groups"]:
            self.cl.leaders.pop(g, None)
        wait_for(
            lambda: all(self.cl.state(g) for g in self.cfg["groups"]),
            seconds=35,
            label="majority side",
        )
        s = identity(22, "regional")
        self.invoke(s)
        self.paid(s)
        self.fulfilled(s)
        for name in isolated:
            http(self.nodes[name]["http"] + "/fault", {"isolated": False, "delay_ms": 20})
        for name in isolated:
            http(self.nodes[name]["http"] + "/fault", {"isolated": False, "delay_ms": 0})
        self.save("09-logical-region-healed")

    def migrate_with_requests(self):
        bid = "b1"
        old = "G1"
        migration = "M-b1-1"
        errors = []
        responses = []
        stop = threading.Event()
        s = identity(16, "during-migration")

        def calls():
            while not stop.is_set():
                try:
                    responses.append(self.invoke(s))
                except Exception as exc:
                    errors.append(repr(exc))
                time.sleep(0.04)

        thread = threading.Thread(target=calls)
        thread.start()
        time.sleep(0.2)
        payload = self.out / "migration-payload.json"
        payload.write_text(canon({"migration": migration}), encoding="utf-8")
        # Each checkpoint is reached inside a real migration process. Kill it,
        # then start a different process that re-reads durable group state.
        for phase in ("freeze", "install", "directory", "activate"):
            name = "migration-" + phase
            marker = self.out / (name + "-paused.json")
            self.start(
                name,
                [
                    PY,
                    "-X",
                    "utf8",
                    ROOT / "migration.py",
                    "--config",
                    self.config_path,
                    "--payload",
                    payload,
                    "--trace",
                    self.out / (name + ".jsonl"),
                    "--pause-after",
                    phase,
                    "--marker",
                    marker,
                ],
            )
            wait_for(marker.exists, label=name + " checkpoint")
            self.stop(name)
            self.trace(
                "migration_worker_killed",
                phase=phase,
                marker=json.loads(marker.read_text(encoding="utf-8")),
            )
            if phase == "freeze":
                frozen = json.loads(payload.read_text(encoding="utf-8"))["frozen"]
                self.trace("migration_frozen", receipt=frozen)
                assert (
                    self.cl.group(
                        old, "/command", {"kind": "close", "bucket": bid, "epoch": 1, "s": s}
                    )["code"]
                    == "not_active"
                )
            if phase == "install":
                self.trace("migration_imported_before_directory", migration=migration)
        p = self.start(
            "migration-recovery",
            [
                PY,
                "-X",
                "utf8",
                ROOT / "migration.py",
                "--config",
                self.config_path,
                "--payload",
                payload,
                "--trace",
                self.out / "migration-recovery.jsonl",
            ],
        )
        assert p.wait(timeout=20) == 0
        self.cl.refresh()
        http(self.cfg["router"] + "/refresh", {})
        time.sleep(0.3)
        stop.set()
        thread.join(timeout=10)
        self.paid(s)
        self.fulfilled(s)
        source = self.cl.state(old)["buckets"][bid]
        target = self.cl.bucket(bid)
        assert source["mode"] == "frozen" and target["mode"] == "active"
        # Frozen complete image is immutable; target retains every original ID.
        for key, o in frozen["image"]["operations"].items():
            n = target["operations"][key]
            for field in (
                "s",
                "input",
                "input_version",
                "p",
                "c",
                "t",
                "scope",
                "key",
                "payment_input",
                "acceptance",
            ):
                assert n[field] == o[field]
        self.trace(
            "migration_complete", responses=len(responses), unknowns=len(errors), errors=errors
        )
        self.save("08-migration-complete")

    def cleanup(self):
        result = []
        for name, (p, cmd) in list(self.processes.items())[::-1]:
            if p.poll() is None:
                try:
                    p.kill()
                    p.wait(timeout=10)
                except Exception as exc:
                    result.append({"name": name, "error": repr(exc)})
            result.append({"name": name, "pid": p.pid, "returncode": p.poll()})
        (self.out / "cleanup.json").write_text(canon(result), encoding="utf-8")
        for f in self.logs.values():
            f.close()
        if self.native:
            self.native.close()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-dir", required=True)
    args = ap.parse_args()
    lab = Lab(Path(args.run_dir))
    ok = False
    try:
        lab.initialize()
        lab.cases()
        lab.save("final-state")
        ok = True
    except Exception:
        (lab.out / "failure.txt").write_text(traceback.format_exc(), encoding="utf-8")
        raise
    finally:
        lab.cleanup()
        (lab.out / "run-status.json").write_text(canon({"completed": ok}), encoding="utf-8")
