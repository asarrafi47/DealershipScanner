"""Tests for site-admin bootstrap password sync behavior."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

from backend.db.users_db import (
    admin_reset_user_password,
    authenticate_app_user,
    get_conn,
    init_users_db,
    list_users_for_admin,
)
from backend.utils.roles import ROLE_ADMIN

ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture()
def users_db(tmp_path, monkeypatch):
    db_path = tmp_path / "users.db"
    monkeypatch.setenv("USERS_DB_PATH", str(db_path))
    monkeypatch.setenv("ALLOW_UNENCRYPTED_USER_DB", "1")
    monkeypatch.setenv("APP_ADMIN_USERNAMES", "asarrafi")
    monkeypatch.setenv("APP_ADMIN_EMAILS", "asarrafi@sarraficars.com")
    init_users_db()
    return db_path


def _asarrafi_id() -> int:
    for row in list_users_for_admin():
        if (row.get("username") or "").lower() == "asarrafi":
            return int(row["id"])
    raise AssertionError("asarrafi user missing")


def _run_bootstrap(
    db_path: Path,
    *,
    admin_password: str,
    force: str | None = None,
    production: bool = False,
) -> str:
    env = {
        **{k: v for k, v in __import__("os").environ.items()},
        "PYTHONPATH": str(ROOT),
        "USERS_DB_PATH": str(db_path),
        "ALLOW_UNENCRYPTED_USER_DB": "1",
        "APP_ADMIN_USERNAMES": "asarrafi",
        "APP_ADMIN_EMAILS": "asarrafi@sarraficars.com",
        "ADMIN_PASSWORD": admin_password,
        "FLASK_ENV": "production" if production else "development",
    }
    if force is not None:
        env["BOOTSTRAP_FORCE_ADMIN_PASSWORD"] = force
    proc = subprocess.run(
        [sys.executable, str(ROOT / "scripts/bootstrap_site_admin.py")],
        capture_output=True,
        text=True,
        env=env,
        check=False,
    )
    assert proc.returncode == 0, proc.stderr or proc.stdout
    return proc.stdout


def test_bootstrap_does_not_overwrite_existing_password(users_db) -> None:
    err = admin_reset_user_password(
        _asarrafi_id(),
        "user-chosen-password",
        min_password_len=8,
    )
    assert err is None
    out = _run_bootstrap(users_db, admin_password="vault-default-password")
    assert "password unchanged" in out
    assert authenticate_app_user("asarrafi", "user-chosen-password") is not None
    assert authenticate_app_user("asarrafi", "vault-default-password") is None


def test_bootstrap_force_sync_overwrites_password(users_db) -> None:
    err = admin_reset_user_password(
        _asarrafi_id(),
        "user-chosen-password",
        min_password_len=8,
    )
    assert err is None
    out = _run_bootstrap(
        users_db,
        admin_password="vault-default-password",
        force="1",
    )
    assert "password synced for asarrafi" in out
    assert authenticate_app_user("asarrafi", "vault-default-password") is not None
    assert authenticate_app_user("asarrafi", "user-chosen-password") is None


def test_bootstrap_sets_password_for_new_admin(users_db) -> None:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM users WHERE lower(username) = lower(?)", ("asarrafi",))
    conn.commit()
    conn.close()

    out = _run_bootstrap(
        users_db,
        admin_password="initial-admin-password",
        production=True,
    )
    assert "created admin user asarrafi" in out
    assert "password set for new admin asarrafi" in out
    user = authenticate_app_user("asarrafi", "initial-admin-password")
    assert user is not None
    assert user.get("role") == ROLE_ADMIN
