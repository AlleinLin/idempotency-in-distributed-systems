"""Official etcd 3.7.2: real three-node HTTP API, lease, failover and snapshot."""

import argparse
import base64
import hashlib
import json
import os
import re
import socket
import subprocess
import time
import traceback
from pathlib import Path
from urllib.request import Request, urlopen

from evidence import capture_sources
from runtime import etcd_binary

ROOT = Path(__file__).parent


def provenance():
    configured = os.environ.get("ETCD_PROVENANCE")
    if not configured:
        raise RuntimeError(
            "Set ETCD_PROVENANCE to a verified release archive record; see README.md."
        )
    record = json.loads(Path(configured).read_text(encoding="utf-8"))
    digest = record.get("sha256", "")
    if (
        record.get("release") != "v3.7.2"
        or not re.fullmatch(r"[0-9a-f]{64}", digest)
        or record.get("official_digest") != "sha256:" + digest
        or not record.get("url", "").startswith("https://github.com/etcd-io/etcd/releases/")
    ):
        raise ValueError("Incomplete or invalid etcd v3.7.2 release provenance.")
    for name in ("etcd", "etcdctl", "etcdutl"):
        check = subprocess.run(
            [str(etcd_binary(name)), "--version" if name == "etcd" else "version"],
            check=True,
            capture_output=True,
            text=True,
        )
        if "3.7.2" not in check.stdout + check.stderr:
            raise ValueError(f"{name} must be version 3.7.2")
    return record


def b64(value):
    return base64.b64encode(value.encode()).decode()


def main(out):
    acquisition = provenance()
    out = out.resolve()
    out.mkdir()
    capture_sources(ROOT, out)
    processes = {}
    logs = []
    seq = 0

    def trace(event, **kw):
        nonlocal seq
        seq += 1
        with (out / "history.jsonl").open("a", encoding="utf-8") as f:
            f.write(json.dumps({"seq": seq, "event": event, **kw}, ensure_ascii=False) + "\n")

    endpoints = [f"http://127.0.0.1:{37100 + i}" for i in range(3)]
    peers = [f"http://127.0.0.1:{37200 + i}" for i in range(3)]
    cluster = ",".join(f"e{i}={peers[i]}" for i in range(3))

    def start(i):
        cmd = [
            str(etcd_binary("etcd")),
            "--name",
            f"e{i}",
            "--data-dir",
            str(out / f"e{i}"),
            "--listen-client-urls",
            endpoints[i],
            "--advertise-client-urls",
            endpoints[i],
            "--listen-peer-urls",
            peers[i],
            "--initial-advertise-peer-urls",
            peers[i],
            "--initial-cluster",
            cluster,
            "--initial-cluster-token",
            "multipartition-" + out.name,
            "--initial-cluster-state",
            "new",
            "--log-level",
            "info",
        ]
        log = (out / f"e{i}.log").open("ab")
        logs.append(log)
        p = subprocess.Popen(cmd, stdout=log, stderr=subprocess.STDOUT, cwd=ROOT)
        processes[i] = (p, cmd)
        trace("process_start", node=i, pid=p.pid, command=cmd)

    def stop(i):
        p, _ = processes[i]
        if p.poll() is None:
            p.kill()
            p.wait(timeout=10)
        trace("process_exit", node=i, pid=p.pid, returncode=p.returncode)

    def api(i, path, body, timeout=3):
        token = seq + 1
        trace("invocation", id=token, node=i, path=path, body=body)
        try:
            req = Request(
                endpoints[i] + path,
                data=json.dumps(body).encode(),
                headers={"Content-Type": "application/json"},
            )
            with urlopen(req, timeout=timeout) as r:
                answer = json.load(r)
            trace("response", id=token, result=answer)
            if "error" in answer:
                raise RuntimeError(answer)
            return answer
        except Exception as e:
            trace("unknown_or_error_response", id=token, error=repr(e))
            raise

    def ready(fn, seconds=25):
        until = time.monotonic() + seconds
        last = None
        while time.monotonic() < until:
            try:
                value = fn()
                if value:
                    return value
            except Exception as e:
                last = repr(e)
            time.sleep(0.1)
        raise TimeoutError(last)

    def any_api(path, body):
        for i in range(3):
            if processes[i][0].poll() is not None:
                continue
            try:
                return api(i, path, body, 2)
            except Exception:
                pass
        raise RuntimeError("no available etcd endpoint")

    def rng(key):
        return any_api("/v3/kv/range", {"key": b64(key)})

    ok = False
    ports = list(range(37100, 37103)) + list(range(37200, 37203))
    result = {}
    try:
        for port in ports:
            with socket.socket() as s:
                s.bind(("127.0.0.1", port))
        (out / "port-ownership.json").write_text(
            json.dumps({"owner": "multipartition-etcd", "ports": ports}), encoding="utf-8"
        )
        for i in range(3):
            start(i)
        for i in range(3):
            ready(lambda i=i: api(i, "/v3/maintenance/status", {}))
        route_key = "mp/route/b1"
        route_value = '{"owner":"G1","epoch":1}'
        create = {
            "compare": [
                {"key": b64(route_key), "target": "VERSION", "result": "EQUAL", "version": "0"}
            ],
            "success": [{"request_put": {"key": b64(route_key), "value": b64(route_value)}}],
            "failure": [{"request_range": {"key": b64(route_key)}}],
        }
        first = ready(lambda: any_api("/v3/kv/txn", create))
        second = any_api("/v3/kv/txn", create)
        assert first["succeeded"] is True and second.get("succeeded", False) is False
        lease_a = any_api("/v3/lease/grant", {"TTL": "2"})["ID"]
        owner = "mp/owner"
        resource = "mp/resource"

        def acquire(lease, worker):
            return {
                "compare": [
                    {"key": b64(owner), "target": "VERSION", "result": "EQUAL", "version": "0"}
                ],
                "success": [
                    {"request_put": {"key": b64(owner), "value": b64(worker), "lease": lease}}
                ],
                "failure": [{"request_range": {"key": b64(owner)}}],
            }

        acquired = any_api("/v3/kv/txn", acquire(lease_a, "worker-A"))
        assert acquired["succeeded"]
        old_fence = int(rng(owner)["kvs"][0]["create_revision"])
        statuses = [api(i, "/v3/maintenance/status", {}) for i in range(3)]
        leader = next(i for i, s in enumerate(statuses) if s["header"]["member_id"] == s["leader"])
        stop(leader)
        alive = [i for i in range(3) if i != leader]
        ready(lambda: api(alive[0], "/v3/kv/range", {"key": b64(route_key)}))
        failover = rng(route_key)
        assert base64.b64decode(failover["kvs"][0]["value"]).decode() == route_value
        start(leader)
        ready(lambda: api(leader, "/v3/maintenance/status", {}))
        ready(lambda: not rng(owner).get("kvs"), seconds=12)
        lease_b = any_api("/v3/lease/grant", {"TTL": "20"})["ID"]
        assert any_api("/v3/kv/txn", acquire(lease_b, "worker-B"))["succeeded"]
        new_fence = int(rng(owner)["kvs"][0]["create_revision"])
        assert new_fence > old_fence

        def write(fence, value):
            return {
                "compare": [
                    {
                        "key": b64(owner),
                        "target": "CREATE",
                        "result": "EQUAL",
                        "create_revision": str(fence),
                    }
                ],
                "success": [{"request_put": {"key": b64(resource), "value": b64(value)}}],
                "failure": [{"request_range": {"key": b64(resource)}}],
            }

        stale = any_api("/v3/kv/txn", write(old_fence, "stale-A"))
        current = any_api("/v3/kv/txn", write(new_fence, "current-B"))
        assert stale.get("succeeded", False) is False and current["succeeded"] is True
        any_api("/v3/lease/revoke", {"ID": lease_b})
        assert not rng(owner).get("kvs")
        snapshot = out / "etcd.snapshot"
        save = subprocess.run(
            [
                str(etcd_binary("etcdctl")),
                "--endpoints",
                endpoints[0],
                "snapshot",
                "save",
                str(snapshot),
            ],
            capture_output=True,
            text=True,
            encoding="utf-8",
            timeout=25,
        )
        (out / "snapshot-save.stdout").write_text(save.stdout, encoding="utf-8")
        (out / "snapshot-save.stderr").write_text(save.stderr, encoding="utf-8")
        assert save.returncode == 0
        status = subprocess.run(
            [str(etcd_binary("etcdutl")), "snapshot", "status", str(snapshot), "--write-out=json"],
            capture_output=True,
            text=True,
            encoding="utf-8",
            timeout=15,
        )
        (out / "snapshot-status.json").write_text(status.stdout, encoding="utf-8")
        assert status.returncode == 0
        # The snapshot is restored offline to a NEW isolated one-node cluster.
        restore_data = out / "isolated-restore"
        restore = subprocess.run(
            [
                str(etcd_binary("etcdutl")),
                "snapshot",
                "restore",
                str(snapshot),
                "--name",
                "restored",
                "--data-dir",
                str(restore_data),
                "--initial-cluster",
                "restored=http://127.0.0.1:37204",
                "--initial-advertise-peer-urls",
                "http://127.0.0.1:37204",
                "--initial-cluster-token",
                "isolated-restore-" + out.name,
            ],
            capture_output=True,
            text=True,
            encoding="utf-8",
            timeout=25,
        )
        (out / "snapshot-restore.stdout").write_text(restore.stdout, encoding="utf-8")
        (out / "snapshot-restore.stderr").write_text(restore.stderr, encoding="utf-8")
        assert restore.returncode == 0
        for port in (37104, 37204):
            with socket.socket() as s:
                s.bind(("127.0.0.1", port))
        endpoints.append("http://127.0.0.1:37104")
        peers.append("http://127.0.0.1:37204")
        ports.extend([37104, 37204])
        log = (out / "restored.log").open("ab")
        logs.append(log)
        cmd = [
            str(etcd_binary("etcd")),
            "--name",
            "restored",
            "--data-dir",
            str(restore_data),
            "--listen-client-urls",
            endpoints[3],
            "--advertise-client-urls",
            endpoints[3],
            "--listen-peer-urls",
            peers[3],
            "--initial-advertise-peer-urls",
            peers[3],
            "--initial-cluster",
            "restored=" + peers[3],
            "--log-level",
            "info",
        ]
        p = subprocess.Popen(cmd, stdout=log, stderr=subprocess.STDOUT, cwd=ROOT)
        processes[3] = (p, cmd)
        trace("process_start", node=3, pid=p.pid, command=cmd)
        ready(lambda: api(3, "/v3/maintenance/status", {}))
        restored_route = api(3, "/v3/kv/range", {"key": b64(route_key)})
        restored_resource = api(3, "/v3/kv/range", {"key": b64(resource)})
        result = {
            "created": first,
            "duplicate_create": second,
            "old_fence": old_fence,
            "new_fence": new_fence,
            "stale_write": stale,
            "current_write": current,
            "killed_leader": leader,
            "failover_route": failover,
            "restored_route": restored_route,
            "restored_resource": restored_resource,
            "final_route": rng(route_key),
            "final_resource": rng(resource),
            "final_owner": rng(owner),
            "snapshot_sha256": hashlib.sha256(snapshot.read_bytes()).hexdigest(),
            "binary_sha256": {
                name: hashlib.sha256(etcd_binary(name).read_bytes()).hexdigest()
                for name in ("etcd", "etcdctl", "etcdutl")
            },
            "acquisition": acquisition,
        }
        (out / "result.json").write_text(
            json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        ok = True
    except Exception:
        (out / "failure.txt").write_text(traceback.format_exc(), encoding="utf-8")
        raise
    finally:
        cleanup = []
        for i in list(processes)[::-1]:
            stop(i)
            p, _ = processes[i]
            cleanup.append({"node": i, "pid": p.pid, "returncode": p.returncode})
        for f in logs:
            f.close()
        (out / "port-ownership.json").write_text(
            json.dumps({"owner": "multipartition-etcd", "ports": ports}), encoding="utf-8"
        )
        (out / "cleanup.json").write_text(json.dumps(cleanup), encoding="utf-8")
        (out / "run-status.json").write_text(
            json.dumps(
                {"completed": ok, "version": "etcd3.7.2", "scope": "etcd-native-coordination"}
            ),
            encoding="utf-8",
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-dir", required=True)
    main(Path(parser.parse_args().run_dir))
