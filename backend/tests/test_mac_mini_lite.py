"""Mac Mini lite scanner profile (memory-safe local 92694 runs)."""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from backend.scanner import mac_mini_lite as mml


def test_apply_mac_mini_lite_env_sets_low_concurrency(monkeypatch):
    monkeypatch.delenv("SCANNER_MAX_DEALER_CONCURRENCY", raising=False)
    monkeypatch.delenv("SCANNER_MAX_VDP_CONCURRENCY", raising=False)
    mml.apply_mac_mini_lite_env(force=True)
    assert os.environ["SCANNER_MAX_DEALER_CONCURRENCY"] == "1"
    assert os.environ["SCANNER_MAX_VDP_CONCURRENCY"] == "1"
    assert os.environ["SCANNER_VDP_SPEC_GAP_MAX"] == "0"
    assert os.environ["SCANNER_SISTER_STORE_FILTER"] == "0"
    assert os.environ["SCANNER_VDP_DOWNLOAD_IMAGES"] == "0"
    assert os.environ["SCANNER_MAC_MINI_LITE"] == "1"


def test_manifest_allowed_for_92694():
    assert mml.manifest_allowed_for_mac_mini_lite("workspace/manifest_92694_25mi.json")
    assert not mml.manifest_allowed_for_mac_mini_lite("workspace/manifest_100mi.json")


def test_manifest_allowed_override(monkeypatch):
    monkeypatch.setenv(mml.ALLOW_ANY_MANIFEST_ENV, "1")
    assert mml.manifest_allowed_for_mac_mini_lite("workspace/manifest_100mi.json")


def test_validate_mac_mini_lite_scope_rejects_other_manifest(tmp_path, monkeypatch):
    mp = tmp_path / "manifest_other.json"
    mp.write_text("[]", encoding="utf-8")
    monkeypatch.delenv(mml.ALLOW_ANY_MANIFEST_ENV, raising=False)
    with pytest.raises(SystemExit):
        mml.validate_mac_mini_lite_scope(mp)


def test_validate_mac_mini_lite_scope_accepts_92694(tmp_path, monkeypatch):
    mp = tmp_path / "manifest_92694_25mi.json"
    mp.write_text("[]", encoding="utf-8")
    monkeypatch.delenv(mml.ALLOW_ANY_MANIFEST_ENV, raising=False)
    mml.validate_mac_mini_lite_scope(mp)


def test_bind_manifest_path_updates_constants(tmp_path, monkeypatch):
    mp = tmp_path / "manifest_92694_25mi.json"
    mp.write_text("[]", encoding="utf-8")
    import backend.scanner.constants as scanner_constants

    monkeypatch.setattr(scanner_constants, "MANIFEST_PATH", Path("dealers.json"))
    bound = mml.bind_manifest_path(mp)
    assert bound == mp.resolve()
    assert os.environ["DEALERS_MANIFEST_PATH"] == str(mp.resolve())
    assert scanner_constants.MANIFEST_PATH == mp.resolve()


def test_remaining_manifest_dealers_skips_db_present(tmp_path, monkeypatch):
    import json
    import sqlite3

    mp = tmp_path / "manifest_92694_25mi.json"
    mp.write_text(
        json.dumps(
            [
                {"name": "Done Dealer", "dealer_id": "done-com", "url": "https://a.com"},
                {"name": "Todo Dealer", "dealer_id": "todo-com", "url": "https://b.com"},
            ]
        ),
        encoding="utf-8",
    )
    db = tmp_path / "inventory.db"
    conn = sqlite3.connect(db)
    conn.execute(
        "CREATE TABLE cars (id INTEGER PRIMARY KEY, dealer_id TEXT, listing_active INTEGER DEFAULT 1)"
    )
    conn.execute("INSERT INTO cars (dealer_id) VALUES ('done-com')")
    conn.commit()
    conn.close()
    monkeypatch.setattr(mml, "default_manifest_path", lambda: mp)
    monkeypatch.setattr(mml, "dealers_with_inventory_rows", lambda: {"done-com"})
    remaining = mml.remaining_manifest_dealers(mp)
    assert [d["dealer_id"] for d in remaining] == ["todo-com"]


def test_scan_lab_spawns_mac_mini_runner(monkeypatch, tmp_path):
    from backend.dev import scan_lab as sl

    mp = tmp_path / "manifest_92694_25mi.json"
    mp.write_text("[]", encoding="utf-8")
    captured: dict[str, object] = {}

    class FakeProc:
        stdout = iter([])

        def wait(self):
            return 0

    def fake_popen(cmd, **kwargs):
        captured["cmd"] = cmd
        captured["env"] = kwargs.get("env")
        return FakeProc()

    monkeypatch.setattr(subprocess := __import__("subprocess"), "Popen", fake_popen)
    sl._run_manifest_scan_job("job1", mp)
    cmd = captured["cmd"]
    assert "scanner_mac_mini.py" in str(cmd)
    assert "scanner.py" not in str(cmd)
    env = captured["env"]
    assert env["SCANNER_MAX_DEALER_CONCURRENCY"] == "1"
    assert env["SCANNER_MAC_MINI_LITE"] == "1"
