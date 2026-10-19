"""Capture run provenance before processes start; never rewrite earlier batches."""

import datetime
import hashlib
import importlib.metadata
import json
import os
import shutil
import sys
from pathlib import Path

from runtime import authority_binary, etcd_binary, nats_server


def capture_sources(root, run):
    destination = run / "source-freeze"
    destination.mkdir()
    paths = [
        *root.glob("*.py"),
        *root.glob("requirements*.txt"),
        *root.joinpath("authority").glob("*.go"),
        root / "authority/go.mod",
        root / "authority/go.sum",
    ]
    # Preserve the actual authority binary even when configured outside this tree.
    # Standalone mechanism experiments may run without the authority installed.
    try:
        binary = authority_binary()
    except FileNotFoundError:
        binary = None
    if binary is not None:
        output = destination / ("authority.exe" if os.name == "nt" else "authority-server")
        shutil.copy2(binary, output)
        paths.append(output)
    files = []
    for path in sorted(paths):
        if path.parent == destination:
            relative = Path(path.name)
        else:
            relative = path.relative_to(root)
            output = destination / relative
            output.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(path, output)
        files.append(
            {
                "path": relative.as_posix(),
                "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                "bytes": path.stat().st_size,
            }
        )
    dependencies = []
    for name, resolve in (
        ("nats-server", nats_server),
        ("etcd", lambda: etcd_binary("etcd")),
        ("etcdctl", lambda: etcd_binary("etcdctl")),
        ("etcdutl", lambda: etcd_binary("etcdutl")),
    ):
        try:
            path = resolve()
        except FileNotFoundError:
            continue
        dependencies.append(
            {
                "name": name,
                "path": str(path),
                "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
            }
        )
    packages = {}
    for name in ("nats-py", "pika", "confluent-kafka", "dbos", "ray"):
        try:
            distribution = importlib.metadata.distribution(name)
        except importlib.metadata.PackageNotFoundError:
            continue
        # Freeze each installed package's source and native extensions by hash.
        members = []
        for member in distribution.files or []:
            if member.suffix not in (".py", ".pyd", ".dll", ".so") and member.name != "METADATA":
                continue
            path = Path(distribution.locate_file(member))
            if path.is_file():
                members.append(
                    {"path": str(member), "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}
                )
        packages[name] = {"version": distribution.version, "files": members}
    record = {
        "captured_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "status": "captured before any process start",
        "files": files,
        "dependencies": dependencies,
        "packages": packages,
        "runtime": {
            "python": sys.version,
            "executable": sys.executable,
            "python_sha256": hashlib.sha256(Path(sys.executable).read_bytes()).hexdigest(),
        },
    }
    (run / "source-manifest.json").write_text(
        json.dumps(record, ensure_ascii=False, indent=2), encoding="utf-8"
    )


def capture_configuration(run):
    path = run / "source-manifest.json"
    record = json.loads(path.read_text(encoding="utf-8"))
    paths = [
        run / "config.json",
        run / "port-ownership.json",
        *run.glob("brokers/*.properties"),
        *run.glob("brokers/*.conf"),
    ]
    for item in paths:
        data = item.read_bytes()
        record["files"].append(
            {
                "run_file": item.relative_to(run).as_posix(),
                "sha256": hashlib.sha256(data).hexdigest(),
                "bytes": len(data),
            }
        )
    path.write_text(json.dumps(record, ensure_ascii=False, indent=2), encoding="utf-8")
