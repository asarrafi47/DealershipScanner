"""P16.3: the optional dead-MFA scrub is a dry run by default and clears seeds on apply."""
from __future__ import annotations

import sqlite3

import pytest

from backend.scripts import scrub_dead_mfa_secrets as scr


def _make(path, with_dead_cols=True):
    conn = sqlite3.connect(path)
    extra = ", totp_secret TEXT, mfa_phone TEXT, totp_enabled INTEGER DEFAULT 0" if with_dead_cols else ""
    conn.execute(f"CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT{extra})")
    if with_dead_cols:
        conn.execute("INSERT INTO users (email, totp_secret, mfa_phone, totp_enabled) VALUES ('a@x', 'SEED', '+15550001', 1)")
        conn.execute("INSERT INTO users (email, totp_secret, mfa_phone, totp_enabled) VALUES ('b@x', '', NULL, 0)")
    else:
        conn.execute("INSERT INTO users (email) VALUES ('a@x')")
    conn.commit()
    conn.close()


def _target(path):
    return [("users.db", lambda: sqlite3.connect(path), ("users",), lambda p: sqlite3.connect(p))]


def _counts(path):
    conn = sqlite3.connect(path)
    try:
        return scr.count_dead(conn, ("users",))
    finally:
        conn.close()


def test_dry_run_writes_nothing(tmp_path, capsys):
    db = tmp_path / "u.db"
    _make(str(db))
    assert scr.run(False, tmp_path / "bk", _target(str(db))) == 0
    assert _counts(str(db)) == {"users.totp_secret": 1, "users.mfa_phone": 1, "users.totp_enabled": 1}
    assert not (tmp_path / "bk").exists()
    assert "dry run" in capsys.readouterr().out


def test_apply_backs_up_then_clears(tmp_path):
    db = tmp_path / "u.db"
    _make(str(db))
    assert scr.run(True, tmp_path / "bk", _target(str(db))) == 0
    assert scr.total(_counts(str(db))) == 0
    copies = list((tmp_path / "bk").glob("*/u.db"))
    assert len(copies) == 1
    assert scr.total(_counts(str(copies[0]))) == 3  # the backup still holds the seeds
    conn = sqlite3.connect(str(db))
    assert conn.execute("SELECT count(*) FROM users").fetchone()[0] == 2  # rows kept
    conn.close()


def test_columns_already_dropped_are_skipped(tmp_path, capsys):
    db = tmp_path / "u.db"
    _make(str(db), with_dead_cols=False)
    assert scr.run(True, tmp_path / "bk", _target(str(db))) == 0
    assert _counts(str(db)) == {
        "users.totp_secret": "absent",
        "users.mfa_phone": "absent",
        "users.totp_enabled": "absent",
    }
    assert "nothing to scrub" in capsys.readouterr().out
    assert not (tmp_path / "bk").exists()


def test_failed_backup_stops_before_scrub(tmp_path):
    db = tmp_path / "u.db"
    _make(str(db))

    def bad_opener(p):
        raise RuntimeError("wrong key")

    tgt = [("users.db", lambda: sqlite3.connect(str(db)), ("users",), bad_opener)]
    with pytest.raises(RuntimeError):
        scr.run(True, tmp_path / "bk", tgt)
    assert scr.total(_counts(str(db))) == 3


def test_purge_backup(tmp_path):
    d = tmp_path / "bk" / "ts"
    d.mkdir(parents=True)
    (d / "u.db").write_text("x")
    assert scr.purge(d) == 0
    assert not d.exists()
    assert scr.purge(d) == 2
