"""Generate explicit Windows multi-machine launch files; never executes them."""

import argparse
import json
from pathlib import Path

ap = argparse.ArgumentParser()
ap.add_argument("--hosts", required=True, help="JSON: A/B/C/platform/worker -> IP or SSH hostname")
ap.add_argument("--output", required=True)
ap.add_argument("--remote-root", default=r"C:\idempotency-lab")
ap.add_argument(
    "--python", default="python", help="Python executable installed on the target hosts"
)
ap.add_argument(
    "--authority", default="authority.exe", help="Authority executable name/path on target hosts"
)
ap.add_argument(
    "--nats-server", default="nats-server", help="NATS executable name/path on worker host"
)
args = ap.parse_args()
hosts = json.loads(Path(args.hosts).read_text(encoding="utf-8-sig"))
R = Path(args.output)
R.mkdir(exist_ok=False)
for role in ("A", "B", "C", "platform", "worker"):
    assert hosts.get(role), role
root = Path(args.remote_root)
cfg = {
    "groups": {},
    "platform": "http://127.0.0.1:37510",
    "proxy": "http://127.0.0.1:37511",
    "consumer": "http://127.0.0.1:37512",
    "router": "http://127.0.0.1:37513",
    "dispatch": "http://127.0.0.1:37514",
    "nats": "nats://127.0.0.1:37500",
    "consumer_quarantine": str(root / "worker/consumer-quarantine.sqlite"),
}


def ps(value):
    return "'" + str(value).replace("'", "''") + "'"


for j, region in enumerate(("A", "B", "C")):
    for gi, g in enumerate(("control", "G1", "G2", "G3")):
        peers = [
            {
                "id": f"{g}-{k}",
                "raft": f"{hosts[reg]}:{37100 + gi}",
                "http": f"http://{hosts[reg]}:{37200 + gi}",
            }
            for k, reg in enumerate(("A", "B", "C"))
        ]
        cfg["groups"][g] = [x["http"] for x in peers]
        argv = [
            "--id",
            f"{g}-{j}",
            "--group",
            g,
            "--raft",
            f"{hosts[region]}:{37100 + gi}",
            "--http",
            f"{hosts[region]}:{37200 + gi}",
            "--dir",
            str(root / "data" / f"{g}-{j}"),
            "--peers",
            json.dumps(peers, separators=(",", ":")),
        ]
        # --bootstrap is explicit and only belongs to the first creation script.
        lines = [
            "param([switch]$FirstCreation)",
            "$ErrorActionPreference='Stop'",
            "$nodeArgs=@(" + ",".join(ps(x) for x in argv) + ")",
        ]
        if j == 0:
            lines.append("if($FirstCreation){$nodeArgs+='--bootstrap'}")
        else:
            lines.append("if($FirstCreation){throw 'Only node 0 can bootstrap this group'}")
        lines.append(f"& {ps(root / args.authority)} @nodeArgs")
        (R / f"replica-{region}-{g}.ps1").write_text("\n".join(lines) + "\n", encoding="utf-8-sig")
(R / "config.json").write_text(json.dumps(cfg, ensure_ascii=False, indent=2), encoding="utf-8")
platform_args = [
    "--role",
    "platform",
    "--config",
    str(root / "config.json"),
    "--port",
    "37510",
    "--db",
    str(root / "platform/platform.sqlite"),
    "--trace",
    str(root / "platform/platform.jsonl"),
]
(R / "platform-command.json").write_text(
    json.dumps(
        {
            "host": hosts["platform"],
            "executable": args.python,
            "args": ["-X", "utf8", str(root / "services.py")] + platform_args,
            "worker_to_platform_tunnel": [
                "ssh",
                "-N",
                "-L",
                "127.0.0.1:37510:127.0.0.1:37510",
                hosts["platform"],
            ],
        },
        ensure_ascii=False,
        indent=2,
    ),
    encoding="utf-8",
)


def script(name, exe, argv, directory):
    lines = [
        "$ErrorActionPreference='Stop'",
        f"New-Item -ItemType Directory -Force -Path {ps(directory)} | Out-Null",
        "$launchArgs=@(" + ",".join(ps(x) for x in argv) + ")",
        f"& {ps(exe)} @launchArgs",
    ]
    (R / name).write_text("\n".join(lines) + "\n", encoding="utf-8-sig")


script(
    "platform.ps1",
    args.python,
    ["-X", "utf8", str(root / "services.py")] + platform_args,
    root / "platform",
)
script(
    "platform-tunnel.ps1",
    "ssh",
    ["-N", "-L", "127.0.0.1:37510:127.0.0.1:37510", hosts["platform"]],
    root / "worker",
)
script(
    "nats.ps1",
    args.nats_server,
    ["-a", "127.0.0.1", "-p", "37500", "-m", "37501", "-js", "-sd", str(root / "worker/nats-data")],
    root / "worker",
)
for role, port in (
    ("consumer-api", 37512),
    ("proxy", 37511),
    ("router", 37513),
    ("dispatch-api", 37514),
    ("pay", None),
    ("coordinate", None),
    ("relay", None),
    ("consume", None),
    ("dispatch", None),
    ("refund", None),
):
    argv = [
        "-X",
        "utf8",
        str(root / "services.py"),
        "--role",
        role,
        "--config",
        str(root / "config.json"),
        "--trace",
        str(root / "worker" / (role + ".jsonl")),
    ]
    if port:
        argv += ["--port", str(port)]
    if role in ("consumer-api", "dispatch-api"):
        argv += ["--db", str(root / "worker" / (role + ".sqlite"))]
    script(role + ".ps1", args.python, argv, root / "worker")
(R / "deployment.json").write_text(
    json.dumps(
        {
            "executed": False,
            "hosts": hosts,
            "root": str(root),
            "replica_files": "Each replica-X-G.ps1 runs one blocking node command under its own terminal or service supervisor.",
            "persistence": "Each node keeps its own data directory; only initial creation passes -FirstCreation to region A scripts. Restarts omit it.",
            "transport": "Private isolated lab network for authority HTTP/Raft; authenticated SSH tunnel for loopback-only platform endpoint.",
            "worker_roles": "Worker host runs NATS, consumer-api, proxy, router, dispatch-api, pay/coordinate/relay/consume/dispatch/refund from individual generated scripts.",
            "setup": "Copy identical frozen binary and Python sources/dependencies to every required host; initialize groups, buckets, routes and sessions once using initialize_multimachine.py.",
            "not_implemented": "Production TLS, membership automation, cross-machine deployment execution and geographic RPO/RTO measurement",
        },
        ensure_ascii=False,
        indent=2,
    ),
    encoding="utf-8",
)
print(R)
