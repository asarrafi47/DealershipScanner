"""Tests for /dev admin DB bootstrap password sync behavior."""

from __future__ import annotations

import importlib

import pytest

from backend.db.admin_users_db import authenticate_admin, init_admin_db, save_dev_admin_user
from backend.db.password_hash import hash_password


@pytest.fixture()
def dev_admin_db(tmp_path, monkeypatch):
    monkeypatch.setenv("DEV_USERS_DB_PATH", str(tmp_path / "dev_users.db"))
    monkeypatch.setenv("ALLOW_UNENCRYPTED_USER_DB", "1")
    monkeypatch.setenv("FLASK_ENV", "development")
    monkeypatch.setenv("ADMIN_USERNAME", "admin")
    monkeypatch.setenv("ADMIN_EMAIL", "admin@localhost")
    init_admin_db()
    return tmp_path / "dev_users.db"


def test_init_admin_db_does_not_overwrite_existing_password(dev_admin_db, monkeypatch) -> None:
    monkeypatch.setenv("ADMIN_PASSWORD", "vault-default-password")
    save_dev_admin_user("other", "other@localhost", "unused")
    conn_path = dev_admin_db
    import sqlite3

    conn = sqlite3.connect(str(conn_path))
    conn.execute(
        "UPDATE admin_users SET password = ? WHERE username = ?",
        (hash_password("user-chosen-password"), "admin"),
    )
    conn.commit()
    conn.close()

    init_admin_db()
    assert authenticate_admin("admin", "user-chosen-password") is not None
    assert authenticate_admin("admin", "vault-default-password") is None


def test_init_admin_db_force_sync_overwrites_password(dev_admin_db, monkeypatch) -> None:
    monkeypatch.setenv("ADMIN_PASSWORD", "vault-default-password")
    monkeypatch.setenv("BOOTSTRAP_FORCE_ADMIN_PASSWORD", "1")
    import sqlite3

    conn = sqlite3.connect(str(dev_admin_db))
    conn.execute(
        "UPDATE admin_users SET password = ? WHERE username = ?",
        (hash_password("user-chosen-password"), "admin"),
    )
    conn.commit()
    conn.close()

    init_admin_db()
    assert authenticate_admin("admin", "vault-default-password") is not None
    assert authenticate_admin("admin", "user-chosen-password") is None


def test_main_reload_does_not_reset_dev_admin_password(dev_admin_db, monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("ADMIN_PASSWORD", "vault-default-password")
    monkeypatch.setenv("USERS_DB_PATH", str(tmp_path / "users.db"))
    monkeypatch.setenv("DEV_USERS_DB_PATH", str(dev_admin_db))
    import sqlite3

    conn = sqlite3.connect(str(dev_admin_db))
    conn.execute(
        "UPDATE admin_users SET password = ? WHERE username = ?",
        (hash_password("user-chosen-password"), "admin"),
    )
    conn.commit()
    conn.close()

    import backend.main as main

    importlib.reload(main)
    assert authenticate_admin("admin", "user-chosen-password") is not None
