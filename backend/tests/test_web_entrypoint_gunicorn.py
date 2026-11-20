"""The web entrypoint's gunicorn command line (scripts/docker-entrypoint-web.sh).

Threads, the worker timeout and the graceful timeout are env-configurable
(GUNICORN_THREADS / GUNICORN_TIMEOUT / GUNICORN_GRACEFUL_TIMEOUT) with the values the
script always hardcoded as defaults: 4 threads, 120 s, 30 s. Run for real with ``python3``
and ``gunicorn`` replaced by shims that print their arguments.
"""

from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
ENTRYPOINT = REPO / "scripts" / "docker-entrypoint-web.sh"

pytestmark = pytest.mark.skipif(shutil.which("sh") is None, reason="needs a POSIX sh")


def _run(tmp_path: Path, **env: str) -> list[str]:
    shims = tmp_path / "bin"
    shims.mkdir(exist_ok=True)
    (shims / "python3").write_text("#!/bin/sh\nexit 0\n")
    (shims / "gunicorn").write_text('#!/bin/sh\nfor a in "$@"; do echo "$a"; done\n')
    for f in shims.iterdir():
        f.chmod(0o755)
    base = {
        "PATH": f"{shims}:/usr/bin:/bin",
        "KMAC_VAULT_AUTO": "0",
        "MIGRATE_ON_BOOT": "0",
        "GUNICORN_CONF": "/dev/null",
        "PORT": "8123",
    }
    proc = subprocess.run(
        ["sh", str(ENTRYPOINT)], env={**base, **env}, capture_output=True, text=True, timeout=30, cwd=tmp_path
    )
    assert proc.returncode == 0, proc.stderr
    return proc.stdout.splitlines()


def _opt(args: list[str], flag: str) -> str:
    return args[args.index(flag) + 1]


def test_defaults_are_the_previous_hardcoded_values(tmp_path):
    args = _run(tmp_path)
    assert _opt(args, "--threads") == "4"
    assert _opt(args, "--timeout") == "120"
    assert _opt(args, "--graceful-timeout") == "30"
    assert _opt(args, "-w") == "3"  # the unset-variable fallback; prod sets GUNICORN_WORKERS=1
    assert "--preload" in args and args[-1] == "backend.main:app"
    assert _opt(args, "-b") == "0.0.0.0:8123"


def test_each_value_is_env_configurable(tmp_path):
    args = _run(
        tmp_path,
        GUNICORN_WORKERS="1",
        GUNICORN_THREADS="8",
        GUNICORN_TIMEOUT="60",
        GUNICORN_GRACEFUL_TIMEOUT="10",
    )
    assert (_opt(args, "-w"), _opt(args, "--threads"), _opt(args, "--timeout"), _opt(args, "--graceful-timeout")) == (
        "1",
        "8",
        "60",
        "10",
    )


def test_the_script_has_no_hardcoded_thread_or_timeout_left():
    text = ENTRYPOINT.read_text(encoding="utf-8")
    assert "--threads 4" not in text and "--timeout 120" not in text and "--graceful-timeout 30" not in text
    assert os.access(ENTRYPOINT, os.R_OK)
