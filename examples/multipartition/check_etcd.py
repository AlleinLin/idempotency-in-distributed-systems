"""Independent exact-value checks for the official native etcd experiment."""

import argparse
import base64
import hashlib
import json
from pathlib import Path


def decoded(kv):
    return {
        **kv,
        "key": base64.b64decode(kv["key"]).decode(),
        "value": base64.b64decode(kv["value"]).decode(),
    }


def main(run):
    result = json.loads((run / "result.json").read_text(encoding="utf-8"))
    checks = []

    def verify(label, condition, detail=None):
        checks.append(
            {"check": label, "passed": bool(condition), **({} if condition else {"detail": detail})}
        )

    status = json.loads((run / "run-status.json").read_text())
    verify(
        "official-native-run-completed",
        status == {"completed": True, "version": "etcd3.7.2", "scope": "etcd-native-coordination"},
    )
    verify(
        "create-only-once",
        result["created"]["succeeded"] is True
        and not result["duplicate_create"].get("succeeded", False),
    )
    create_revision = int(result["created"]["header"]["revision"])
    for name in ("failover_route", "final_route", "restored_route"):
        values = [decoded(v) for v in result[name].get("kvs", [])]
        verify(
            name + "/one-version",
            len(values) == 1
            and values[0]["key"] == "mp/route/b1"
            and values[0]["value"] == '{"owner":"G1","epoch":1}'
            and int(values[0]["version"]) == 1
            and int(values[0]["create_revision"]) == create_revision
            and int(values[0]["mod_revision"]) == create_revision,
            values,
        )
    verify(
        "lease-new-create-revision-is-greater",
        create_revision < result["old_fence"] < result["new_fence"],
    )
    verify("expired-owner-transaction-rejected", not result["stale_write"].get("succeeded", False))
    verify("new-owner-transaction-accepted", result["current_write"]["succeeded"] is True)
    resource_revision = int(result["current_write"]["header"]["revision"])
    for name in ("final_resource", "restored_resource"):
        values = [decoded(v) for v in result[name].get("kvs", [])]
        verify(
            name + "/one-current-effect",
            len(values) == 1
            and values[0]["key"] == "mp/resource"
            and values[0]["value"] == "current-B"
            and int(values[0]["version"]) == 1
            and int(values[0]["create_revision"]) == resource_revision
            and int(values[0]["mod_revision"]) == resource_revision,
            values,
        )
    verify("revoked-lease-key-absent", not result["final_owner"].get("kvs"))
    verify(
        "snapshot-file-sha",
        hashlib.sha256((run / "etcd.snapshot").read_bytes()).hexdigest()
        == result["snapshot_sha256"],
    )
    snapshot = json.loads((run / "snapshot-status.json").read_text())
    verify("official-snapshot-revision", int(snapshot["revision"]) >= resource_revision)
    verify(
        "snapshot-restores-to-distinct-cluster",
        result["restored_route"]["header"]["cluster_id"]
        != result["final_route"]["header"]["cluster_id"],
    )
    official = result["acquisition"]
    verify(
        "official-asset-digest-verified",
        official["release"] == "v3.7.2"
        and official["official_digest"] == "sha256:" + official["sha256"],
    )
    history = [json.loads(line) for line in (run / "history.jsonl").read_text().splitlines()]
    verify(
        "single-collector-order", [h["seq"] for h in history] == list(range(1, len(history) + 1))
    )
    killed = result["killed_leader"]
    stops = [h for h in history if h["event"] == "process_exit" and h["node"] == killed]
    starts = [h for h in history if h["event"] == "process_start" and h["node"] == killed]
    verify(
        "actual-leader-process-kill-and-new-pid",
        len(starts) >= 2
        and stops
        and starts[0]["pid"] == stops[0]["pid"]
        and starts[1]["pid"] != starts[0]["pid"]
        and starts[0]["seq"] < stops[0]["seq"] < starts[1]["seq"],
    )
    verify(
        "all-native-processes-exited",
        all(v["returncode"] is not None for v in json.loads((run / "cleanup.json").read_text())),
    )
    manifest = json.loads((run / "source-manifest.json").read_text())
    for entry in manifest["files"]:
        verify(
            "source/" + entry["path"],
            hashlib.sha256((run / "source-freeze" / entry["path"]).read_bytes()).hexdigest()
            == entry["sha256"],
        )
    report = {
        "passed": all(c["passed"] for c in checks),
        "checks": checks,
        "count": len(checks),
        "checker_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "scope": "official etcd3.7.2 native coordination and isolated snapshot restoration",
        "limitations": [
            "Conditional resource writes are in the same etcd transaction; an external service needs its own fencing check.",
            "This experiment does not integrate etcd into the full purchase authority FSM.",
            "Snapshot restoration is isolated and creates a new cluster; it is not a live failover procedure.",
            "Learner promotion, membership changes and disk destruction are outside this batch.",
        ],
    }
    (run / "independent-check.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(json.dumps({k: v for k, v in report.items() if k != "checks"}, ensure_ascii=False))
    for c in checks:
        if not c["passed"]:
            print("FAIL", c)
    raise SystemExit(0 if report["passed"] else 1)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--run-dir", required=True)
    main(Path(p.parse_args().run_dir).resolve())
