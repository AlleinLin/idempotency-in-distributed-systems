"""Recoverable complete migration worker, with real process crash checkpoints."""

import argparse
import json
import os
import time
from pathlib import Path

from common import Cluster, Trace

p = argparse.ArgumentParser()
p.add_argument("--config")
p.add_argument("--payload")
p.add_argument("--trace")
p.add_argument("--pause-after", default="")
p.add_argument("--marker", default="")
a = p.parse_args()
cfg = json.loads(Path(a.config).read_text(encoding="utf-8"))
payload = json.loads(Path(a.payload).read_text(encoding="utf-8"))
cl = Cluster(cfg)
trace = Trace(a.trace)
migration = payload["migration"]


def persist():
    path = Path(a.payload)
    temporary = path.with_suffix(".tmp")
    with temporary.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        f.flush()
        os.fsync(f.fileno())
    os.replace(temporary, path)


def checkpoint(phase):
    payload["last_completed_phase"] = phase
    persist()
    trace("phase_complete", phase=phase)
    if a.pause_after == phase:
        Path(a.marker).write_text(
            json.dumps({"phase": phase, "pid": os.getpid()}), encoding="utf-8"
        )
        trace("paused", phase=phase)
        while True:
            time.sleep(0.1)


# The durable source accepts repeated freeze with the same migration identity.
frozen = cl.group(
    "G1", "/command", {"kind": "freeze", "bucket": "b1", "epoch": 1, "migration": migration}
)
assert frozen["code"] == "frozen"
if "frozen" in payload:
    assert frozen["sha256"] == payload["frozen"]["sha256"]
else:
    payload["frozen"] = frozen
checkpoint("freeze")
result = cl.group(
    "G2",
    "/command",
    {
        "kind": "install",
        "bucket": "b1",
        "new_epoch": 2,
        "sha256": frozen["sha256"],
        "image": frozen["image"],
    },
)
assert result["code"] == "installed"
checkpoint("install")
routes = cl.refresh()
current = routes["b1"]
if current["owner"] == "G1":
    r = cl.group(
        "control",
        "/command",
        {
            "kind": "route",
            "bucket": "b1",
            "old_owner": "G1",
            "old_epoch": 1,
            "route": {
                "owner": "G2",
                "epoch": 2,
                "migration": migration,
                "sha256": frozen["sha256"],
            },
        },
    )
    assert r["owner"] == "G2"
    trace("directory_committed", route=r)
checkpoint("directory")
# Activation is issued only after reading the committed control route; caller is trusted lab admin.
route = cl.refresh()["b1"]
assert (
    route["owner"] == "G2"
    and route["migration"] == migration
    and route["sha256"] == frozen["sha256"]
)
r = cl.group(
    "G2", "/command", {"kind": "activate", "bucket": "b1", "epoch": 2, "migration": migration}
)
assert r["code"] == "active"
trace("target_activated", result=r)
checkpoint("activate")
