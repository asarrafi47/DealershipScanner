"""deploy/railway/sync-vault-to-railway.sh refuses an unset TRUSTED_PROXY_HOPS (P3.7).

The script used to set ``TRUSTED_PROXY_HOPS="${TRUSTED_PROXY_HOPS:-1}"``, which silently
reset prod web (Cloudflare, then Railway's edge: 2 hops since 1.4.4) to 1. It now
requires the variable as a positive integer and exits 1 before its first railway call.
It also requires RAILWAY_SERVICE, and every railway call it makes passes ``--service``,
so nothing lands on the checkout's linked service (scanner-nightly in the main checkout).

Every test runs the real script with a fake ``railway`` on PATH that only records its
argv. PATH holds that fake directory plus the system directories, HOME is a fresh
directory and the environment is built from scratch, so a real Railway CLI, login or
vault token can never be reached.
"""

from __future__ import annotations

import re
import stat
import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT = REPO_ROOT / "deploy" / "railway" / "sync-vault-to-railway.sh"
README = REPO_ROOT / "deploy" / "railway" / "README.md"
SYSTEM_PATH = "/usr/bin:/bin:/usr/sbin:/sbin"

FAKE_RAILWAY = """#!/bin/sh
# Fake railway CLI for test_sync_vault_guard.py: record argv, print nothing.
{
  printf 'CALL'
  for a in "$@"; do printf '\\t%s' "$a"; done
  printf '\\n'
} >> "$FAKE_RAILWAY_LOG"
exit 0
"""


@pytest.fixture
def sandbox(tmp_path):
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    railway = fake_bin / "railway"
    railway.write_text(FAKE_RAILWAY, encoding="utf-8")
    railway.chmod(railway.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
    home = tmp_path / "home"
    home.mkdir()
    return tmp_path, fake_bin, home


def _run(sandbox, *args: str, env_extra: dict | None = None, with_railway: bool = True):
    tmp_path, fake_bin, home = sandbox
    log = tmp_path / "railway.log"
    env = {
        "PATH": f"{fake_bin}:{SYSTEM_PATH}" if with_railway else SYSTEM_PATH,
        "HOME": str(home),
        "FAKE_RAILWAY_LOG": str(log),
    }
    env.update(env_extra or {})
    proc = subprocess.run([str(SCRIPT), *args], cwd=tmp_path, env=env, capture_output=True, text=True, timeout=60)
    calls = []
    if log.exists():
        calls = [line.split("\t")[1:] for line in log.read_text(encoding="utf-8").splitlines()]
    return proc, calls


def test_script_parses():
    proc = subprocess.run(["bash", "-n", str(SCRIPT)], capture_output=True, text=True, timeout=30)
    assert proc.returncode == 0, proc.stderr


@pytest.mark.parametrize("env_extra", [{}, {"TRUSTED_PROXY_HOPS": ""}], ids=["unset", "empty"])
def test_unset_hops_refuses_before_any_railway_call(sandbox, env_extra):
    proc, calls = _run(sandbox, env_extra={"RAILWAY_SERVICE": "web", **env_extra})
    assert proc.returncode == 1, proc.stdout + proc.stderr
    assert "TRUSTED_PROXY_HOPS is not set" in proc.stderr
    assert "Nothing was changed on Railway" in proc.stderr
    assert "uses 2" in proc.stderr  # the prod rule is in the message
    assert calls == []


@pytest.mark.parametrize("value", ["0", "02", "-1", "two", "1.5", " 2", "2 ", "2\n"])
def test_malformed_hops_refuses_before_any_railway_call(sandbox, value):
    proc, calls = _run(sandbox, env_extra={"RAILWAY_SERVICE": "web", "TRUSTED_PROXY_HOPS": value})
    assert proc.returncode == 1, proc.stdout + proc.stderr
    assert "TRUSTED_PROXY_HOPS must be a positive integer" in proc.stderr
    assert calls == []


def test_guard_runs_before_the_cli_check(sandbox):
    """With no railway CLI at all, the refusal is still the hops guard: it is the first
    check after argument parsing, ahead of everything that touches Railway."""
    proc, calls = _run(sandbox, with_railway=False)
    assert proc.returncode == 1
    assert "TRUSTED_PROXY_HOPS is not set" in proc.stderr
    assert "Install Railway CLI" not in proc.stderr
    assert calls == []


@pytest.mark.parametrize("hops", ["1", "2", "3"])
def test_explicit_hops_is_the_value_set(sandbox, hops):
    proc, calls = _run(sandbox, env_extra={"RAILWAY_SERVICE": "web", "TRUSTED_PROXY_HOPS": hops})
    assert proc.returncode == 0, proc.stdout + proc.stderr
    hop_sets = [c for c in calls if any(a.startswith("TRUSTED_PROXY_HOPS=") for a in c)]
    assert hop_sets == [["variables", "set", f"TRUSTED_PROXY_HOPS={hops}", "--service", "web"]]
    assert f"TRUSTED_PROXY_HOPS={hops}" in proc.stdout
    # No vault token in the sandbox: nothing secret is set.
    assert not any(a.startswith("VAULT_TOKEN=") for c in calls for a in c)


@pytest.mark.parametrize("value", [None, "", "-s", "web --kv", "web;x", "$(id)"])
def test_missing_or_malformed_service_refuses_before_any_railway_call(sandbox, value):
    env_extra = {"TRUSTED_PROXY_HOPS": "2"}
    if value is not None:
        env_extra["RAILWAY_SERVICE"] = value
    proc, calls = _run(sandbox, env_extra=env_extra)
    assert proc.returncode == 1, proc.stdout + proc.stderr
    assert "RAILWAY_SERVICE" in proc.stderr
    assert "Nothing was changed on Railway" in proc.stderr
    assert calls == []


def test_every_railway_call_names_the_service(sandbox):
    proc, calls = _run(sandbox, env_extra={"RAILWAY_SERVICE": "web", "TRUSTED_PROXY_HOPS": "2"})
    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert calls, "expected the script to call railway"
    for call in calls:
        assert call[-2:] == ["--service", "web"], call
    deletes = [c[2] for c in calls if c[:2] == ["variables", "delete"]]
    assert deletes == ["INVENTORY_DB_PATH", "KMAC_VAULT_DISABLE", "SCANNER_VDP_IMAGE_DOWNLOAD_DIR"]


def test_mirror_sets_name_the_service_too():
    """The --mirror-secrets path sources the vault loader, so it is checked statically:
    every line that runs ``railway variables`` passes ${SVC_ARGS}."""
    runs = [
        line.strip()
        for line in SCRIPT.read_text(encoding="utf-8").splitlines()
        if re.match(r"\s*(?:if\s+!\s+)?railway variables\b", line)
    ]
    assert len(runs) >= 17, runs
    for line in runs:
        assert "${SVC_ARGS}" in line, line


def test_help_needs_no_hops_and_names_the_requirement(sandbox):
    proc, calls = _run(sandbox, "--help")
    assert proc.returncode == 0, proc.stderr
    assert "TRUSTED_PROXY_HOPS" in proc.stdout
    assert "RAILWAY_SERVICE" in proc.stdout
    assert calls == []


def test_source_has_no_hops_default_and_no_bare_railway_up_advice():
    text = SCRIPT.read_text(encoding="utf-8")
    assert not re.search(r"TRUSTED_PROXY_HOPS:-[^}]", text), "a default value for TRUSTED_PROXY_HOPS is back"
    assert "Deploy: railway up" not in text
    lines = text.splitlines()
    guard = max(
        next(i for i, line in enumerate(lines) if line.strip() == 'case "$hops" in'),
        next(i for i, line in enumerate(lines) if line.strip() == 'case "${RAILWAY_SERVICE:-}" in'),
    )
    first_call = next(i for i, line in enumerate(lines) if re.match(r"\s*(?:if\s+!\s+)?railway\s", line))
    assert guard < first_call


def test_readme_service_list_and_hops_rule():
    text = " ".join(README.read_text(encoding="utf-8").split())
    services = text[text.index("## Services") : text.index("## Prerequisites")]
    for name in ("`web`", "`scanner-nightly`", "`Postgres`"):
        assert name in services, name
    assert "`scanner-worker` and `scanner-scheduler` services were deleted" in services
    assert "2026-10-08 (P0A.4" in services
    hops = text[text.index("## 7. Client IP and proxy hops") : text.index("## Local vs Railway")]
    assert "`TRUST_PROXY_HEADERS=1` and `TRUSTED_PROXY_HOPS=2`" in hops
    assert "TRUSTED_PROXY_CIDRS=cloudflare" in hops
    assert "`1` / `1`" not in text  # the old hops default in the section 2 table
    # The users-DB-on-volume section stays (plan P3.7).
    assert "USERS_DB_PATH=/data/users.db" in text
    assert "DEALER_PORTAL_DB_PATH=/data/dealer_portal.db" in text
