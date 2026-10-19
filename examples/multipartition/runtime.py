"""Resolve installed dependencies without bundling private runtime directories."""

import os
import shutil
from pathlib import Path

ROOT = Path(__file__).resolve().parent
EXE_SUFFIX = ".exe" if os.name == "nt" else ""


def executable(variable, command, default=None):
    """Use an explicit environment override, a local build, then PATH."""
    configured = os.environ.get(variable)
    if configured:
        candidate = Path(configured).expanduser()
        resolved = str(candidate.resolve()) if candidate.is_file() else shutil.which(configured)
        if not resolved:
            raise FileNotFoundError(f"{variable} does not name an executable: {configured}")
        return Path(resolved)
    if default is not None and Path(default).is_file():
        return Path(default).resolve()
    located = shutil.which(command)
    if located:
        return Path(located)
    raise FileNotFoundError(f"Install {command} and add it to PATH, or set {variable}.")


def installation(variable):
    """Require an explicit installation root for optional broker runtimes."""
    configured = os.environ.get(variable)
    if not configured:
        raise RuntimeError(f"Set {variable} to the installed runtime directory.")
    root = Path(configured).expanduser().resolve()
    if not root.is_dir():
        raise FileNotFoundError(f"{variable} is not a directory: {root}")
    return root


def authority_binary():
    name = "authority.exe" if os.name == "nt" else "authority-server"
    return executable("AUTHORITY_BINARY", name, ROOT / name)


def nats_server():
    return executable("NATS_SERVER", "nats-server")


def etcd_binary(name):
    configured = os.environ.get("ETCD_BIN")
    if configured:
        path = installation("ETCD_BIN") / (name + EXE_SUFFIX)
        if not path.is_file():
            raise FileNotFoundError(path)
        return path
    return executable(name.upper() + "_BINARY", name)


def java_binary():
    if os.environ.get("JAVA_HOME"):
        path = installation("JAVA_HOME") / "bin" / ("java" + EXE_SUFFIX)
        if not path.is_file():
            raise FileNotFoundError(path)
        return path
    return executable("JAVA_BINARY", "java")
