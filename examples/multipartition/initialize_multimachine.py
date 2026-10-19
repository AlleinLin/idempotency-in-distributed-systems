"""Explicit once-only initialization for a fresh generated multi-machine config."""

import argparse
import json
from pathlib import Path

from common import Cluster, identity, partition

ap = argparse.ArgumentParser()
ap.add_argument("--config", required=True)
ap.add_argument("--initialize-fresh", action="store_true")
args = ap.parse_args()
if not args.initialize_fresh:
    raise SystemExit(
        "Fresh cluster initialization requires --initialize-fresh; do not run on a restored cluster."
    )
cl = Cluster(json.loads(Path(args.config).read_text(encoding="utf-8")))
for g in ("control", "G1", "G2", "G3"):
    state = cl.state(g)
    if state.get("routes") or state.get("buckets"):
        raise SystemExit(f"{g} already has application state; initialization refused")
for i in (1, 2, 3):
    g = f"G{i}"
    b = f"b{i}"
    stock = {"SKU-A": 4} if i == 2 else {"SKU-B": 4} if i == 3 else {}
    assert (
        cl.group(g, "/command", {"kind": "init", "bucket": b, "epoch": 1, "stock": stock})["code"]
        == "created"
    )
    cl.group(
        "control",
        "/command",
        {
            "kind": "route",
            "bucket": b,
            "route": {"owner": g, "epoch": 1, "migration": "", "sha256": ""},
        },
    )
cl.refresh()
for account in range(1, 25):
    s = identity(account, "setup")
    cl.command(partition(s), "session", s=s, deadline=1000)
print("Fresh application state initialized. Existing/restored clusters must skip this operation.")
