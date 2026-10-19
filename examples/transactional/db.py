"""Small psql transport; every call uses a fresh READ COMMITTED connection."""

import json
import os
import shutil
import subprocess
import tempfile
import threading
from pathlib import Path


def pg_binary(name):
    """Find PostgreSQL tools in PG_BIN or PATH."""
    if os.environ.get("PG_BIN"):
        candidate = Path(os.environ["PG_BIN"]).expanduser() / (
            name + (".exe" if os.name == "nt" else "")
        )
        if candidate.is_file():
            return candidate.resolve()
        raise FileNotFoundError(f"PostgreSQL tool missing in PG_BIN: {candidate}")
    located = shutil.which(name)
    if located:
        return Path(located)
    raise FileNotFoundError(
        f"Install PostgreSQL {name}, add its bin directory to PATH, or set PG_BIN."
    )


ROLE = "chapter15"
PORTS = {"authority": 55487, "local": 55486, "platform": 55488, "consumer": 55489}
if os.environ.get("IDEMPOTENCY_PG_PORT_BASE"):
    base = int(os.environ["IDEMPOTENCY_PG_PORT_BASE"])
    if not 1024 <= base <= 65532:
        raise ValueError("IDEMPOTENCY_PG_PORT_BASE must be between 1024 and 65532.")
    PORTS = {domain: base + offset for offset, domain in enumerate(PORTS)}
TRACE_DIR = None
TRACE_COUNT = 0
TRACE_LOCK = threading.Lock()


def command(args, check=True):
    with tempfile.TemporaryFile() as out, tempfile.TemporaryFile() as err:
        p = subprocess.run([str(a) for a in args], stdout=out, stderr=err)
        out.seek(0)
        err.seek(0)
        p.stdout = out.read().decode("utf-8", "replace")
        p.stderr = err.read().decode("utf-8", "replace")
    if check and p.returncode:
        raise RuntimeError(p.stderr.strip() or p.stdout.strip())
    return p


def sql(domain, text):
    global TRACE_COUNT
    if TRACE_DIR is not None:
        with TRACE_LOCK:
            TRACE_COUNT += 1
            trace = TRACE_DIR / f"{TRACE_COUNT:05d}-{domain}.sql"
        trace.write_text(text, encoding="utf-8")
    else:
        with tempfile.NamedTemporaryFile(suffix=".sql", delete=False) as temporary:
            trace = Path(temporary.name)
        trace.write_text(text, encoding="utf-8")
    try:
        p = command(
            [
                pg_binary("psql"),
                "-X",
                "-qAt",
                "-v",
                "ON_ERROR_STOP=1",
                "-h",
                "127.0.0.1",
                "-p",
                PORTS[domain],
                "-U",
                ROLE,
                "-d",
                "postgres",
                "-f",
                trace,
            ]
        )
        if TRACE_DIR is not None:
            trace.with_suffix(".stdout").write_text(p.stdout, encoding="utf-8")
        return p.stdout.strip()
    except RuntimeError as exc:
        if TRACE_DIR is not None:
            trace.with_suffix(".error").write_text(str(exc), encoding="utf-8")
        raise
    finally:
        if TRACE_DIR is None:
            trace.unlink()


def value(domain, text):
    return json.loads(sql(domain, text))


def lit(value_):
    if value_ is None:
        return "NULL"
    if isinstance(value_, bool):
        return "true" if value_ else "false"
    if isinstance(value_, int):
        return str(value_)
    return "'" + str(value_).replace("'", "''") + "'"


def jb(value_):
    return (
        lit(json.dumps(value_, ensure_ascii=False, sort_keys=True, separators=(",", ":")))
        + "::jsonb"
    )


def table(domain, name):
    return value(
        domain,
        f"SELECT coalesce(jsonb_agg(to_jsonb(rowdata) ORDER BY "
        f"to_jsonb(rowdata)::text),'[]'::jsonb) FROM {name} rowdata",
    )


TABLES = {
    "authority": [
        "directory",
        "sessions",
        "counters",
        "wallets",
        "debits",
        "operations",
        "charges",
        "workflow",
        "refunds",
    ],
    "local": [
        "gate",
        "purchases",
        "payments",
        "tasks",
        "orders",
        "outbox",
        "refund_projection",
        "projection_cursor",
    ],
    "platform": ["accounts", "platform_operations"],
    "consumer": ["fulfillments", "business_guard", "inbox", "dispatch"],
}


def snapshot():
    return {d: {t: table(d, t) for t in names} for d, names in TABLES.items()}
