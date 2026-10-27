"""P11A.1: a lock-refused scanner raises ScanLockHeld and the CLI exits 75."""
from __future__ import annotations

import asyncio
import subprocess
import sys
import textwrap

import pytest

from backend.scanner import scan_lock
from backend.scanner.constants import ROOT


def _hold_in_child(path):
    """A live process holding the flock on ``path``; returns (proc, pid)."""
    code = textwrap.dedent(
        f"""
        import sys, time
        from backend.scanner import scan_lock
        assert scan_lock.acquire_scan_lock({str(path)!r})
        print("held", flush=True)
        time.sleep(60)
        """
    )
    proc = subprocess.Popen([sys.executable, "-c", code], cwd=str(ROOT), stdout=subprocess.PIPE, text=True)
    assert proc.stdout.readline().strip() == "held"
    return proc


def test_exit_code_constant():
    assert scan_lock.EXIT_LOCK_HELD == 75


def test_main_raises_scan_lock_held_before_impl(monkeypatch, tmp_path, caplog):
    from backend.scanner import orchestrator

    lock = tmp_path / "s.lock"
    monkeypatch.setenv("SCANNER_LOCK_PATH", str(lock))
    called = []

    async def _impl(*a, **k):
        called.append(1)

    monkeypatch.setattr(orchestrator, "_main_impl", _impl)
    child = _hold_in_child(lock)
    try:
        with caplog.at_level("ERROR", logger="scanner"):
            with pytest.raises(scan_lock.ScanLockHeld) as ei:
                asyncio.run(orchestrator.main())
    finally:
        child.kill()
        child.wait()
    assert not called
    assert ei.value.holder == child.pid and ei.value.path == lock
    # older pipelines scrape this text during rollout
    assert "Scanner already running" in caplog.text and "refusing to start" in caplog.text


def test_cli_entry_exits_75(monkeypatch, tmp_path):
    from backend.scanner import cli

    async def _main(*a, **k):
        raise scan_lock.ScanLockHeld(123, tmp_path / "x.lock")

    monkeypatch.setattr(cli, "main", _main)
    monkeypatch.setattr(sys, "argv", ["scanner.py", "--dealer-id", "nobody"])
    monkeypatch.setattr(cli, "load_manifest", lambda *a, **k: [{"dealer_id": "nobody", "name": "n", "url": "https://x.test"}])
    with pytest.raises(SystemExit) as ei:
        cli.run_cli_entry()
    assert ei.value.code == 75


def test_env_change_after_import_takes_effect(monkeypatch, tmp_path):
    monkeypatch.setenv("SCANNER_LOCK_PATH", str(tmp_path / "a.lock"))
    assert scan_lock.default_lock_path() == tmp_path / "a.lock"
    assert scan_lock.LOCK_PATH == tmp_path / "a.lock"
    monkeypatch.setenv("SCANNER_LOCK_PATH", str(tmp_path / "b.lock"))
    assert scan_lock.LOCK_PATH == tmp_path / "b.lock"
    from backend.scanner.scan_lock import LOCK_PATH  # importable

    assert LOCK_PATH == tmp_path / "b.lock"


def test_permission_error_pid_counts_as_alive(monkeypatch):
    def _kill(pid, sig):
        raise PermissionError

    monkeypatch.setattr(scan_lock.os, "kill", _kill)
    assert scan_lock._pid_alive(4242) is True
