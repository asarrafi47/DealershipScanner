"""SCANNER_LOCK_PATH gives each pipeline shard its own scanner lock (2026-09-28)."""
from __future__ import annotations

import importlib
import os

import pytest


@pytest.fixture
def scan_lock(monkeypatch, tmp_path):
    monkeypatch.setenv("SCANNER_LOCK_PATH", str(tmp_path / "shard3.lock"))
    import backend.scanner.scan_lock as mod

    mod = importlib.reload(mod)
    yield mod
    monkeypatch.setenv("SCANNER_LOCK_PATH", "")
    importlib.reload(mod)


def test_env_names_the_lock_file(scan_lock, tmp_path):
    assert scan_lock.LOCK_PATH == tmp_path / "shard3.lock"
    assert scan_lock.default_lock_path() == tmp_path / "shard3.lock"


def test_relative_env_path_anchors_to_repo_root(monkeypatch):
    monkeypatch.setenv("SCANNER_LOCK_PATH", "workspace/scanner.shard1.lock")
    import backend.scanner.scan_lock as mod

    p = mod.default_lock_path()
    assert p.is_absolute()
    assert p.parts[-2:] == ("workspace", "scanner.shard1.lock")


def test_unset_env_keeps_the_machine_lock(monkeypatch):
    monkeypatch.setenv("SCANNER_LOCK_PATH", "")
    import backend.scanner.scan_lock as mod

    assert mod.default_lock_path().parts[-2:] == ("workspace", "scanner.lock")


def test_two_shards_hold_independent_locks(scan_lock, tmp_path):
    a, b = tmp_path / "a.lock", tmp_path / "b.lock"
    assert scan_lock.acquire_scan_lock(a)
    assert scan_lock.acquire_scan_lock(b)
    assert scan_lock.current_lock_holder(a) == os.getpid()
    assert scan_lock.current_lock_holder(b) == os.getpid()
    # the same lock cannot be taken twice while its holder lives
    assert scan_lock.acquire_scan_lock(a)  # same pid reclaims its own
    scan_lock.release_scan_lock(a)
    scan_lock.release_scan_lock(b)
    assert not a.exists() and not b.exists()


def test_pipeline_lock_file_follows_env(monkeypatch, tmp_path):
    monkeypatch.setenv("SCANNER_LOCK_PATH", str(tmp_path / "shard7.lock"))
    import backend.scripts.dealer_pipeline as dp

    dp = importlib.reload(dp)
    assert dp.LOCK_FILE == tmp_path / "shard7.lock"
    monkeypatch.setenv("SCANNER_LOCK_PATH", "")
    importlib.reload(dp)
