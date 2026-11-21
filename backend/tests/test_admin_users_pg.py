"""N37.9: /dev operator accounts on Postgres (public.admin_users, V029), ``pg`` tier.

DEV_USERS_DB_BACKEND=pg routes admin_users_db to Postgres; the SQLite default is
covered by test_init_admin_db.py. The env-seeded break-glass admin must log in
with an empty table.
"""

from __future__ import annotations

import pytest

from backend.db import admin_users_db as adb

pytestmark = pytest.mark.pg


@pytest.fixture()
def pg_admin(pg_db, monkeypatch):
    monkeypatch.setenv("DEV_USERS_DB_BACKEND", "pg")
    monkeypatch.setenv("FLASK_ENV", "development")
    monkeypatch.setenv("ADMIN_USERNAME", "admin")
    monkeypatch.setenv("ADMIN_EMAIL", "admin@localhost")
    monkeypatch.setenv("ADMIN_PASSWORD", "env-admin-password")
    monkeypatch.setenv("BOOTSTRAP_FORCE_ADMIN_PASSWORD", "")
    return pg_db


def _count(pg) -> int:
    with pg.connect() as conn:
        return conn.execute("SELECT count(*) FROM admin_users").fetchone()[0]


def test_init_seeds_env_admin_once_and_login_works(pg_admin) -> None:
    adb.init_admin_db()
    adb.init_admin_db()
    assert _count(pg_admin) == 1
    got = adb.authenticate_admin("ADMIN", "env-admin-password")
    assert got is not None and got[1] == "admin"
    assert adb.authenticate_admin("admin@localhost", "env-admin-password") is not None
    assert adb.authenticate_admin("admin", "wrong") is None


def test_init_does_not_overwrite_changed_password_unless_forced(pg_admin, monkeypatch) -> None:
    adb.init_admin_db()
    with pg_admin.connect() as conn:
        conn.execute("UPDATE admin_users SET password = %s", (adb.hash_password("chosen"),))
        conn.commit()
    adb.init_admin_db()
    assert adb.authenticate_admin("admin", "chosen") is not None
    monkeypatch.setenv("BOOTSTRAP_FORCE_ADMIN_PASSWORD", "1")
    adb.init_admin_db()
    assert adb.authenticate_admin("admin", "env-admin-password") is not None
    assert adb.authenticate_admin("admin", "chosen") is None


def test_break_glass_admin_logs_in_with_empty_table(pg_admin) -> None:
    assert _count(pg_admin) == 0
    got = adb.authenticate_admin("admin", "env-admin-password")
    assert got is not None
    assert _count(pg_admin) == 1
    assert adb.get_admin_totp(got[0]) == {"enabled": False, "secret": ""}
    # Wrong password and other users never seed anything.
    assert adb.authenticate_admin("admin", "nope") is None
    assert adb.authenticate_admin("someone", "env-admin-password") is None
    assert _count(pg_admin) == 1


def test_break_glass_does_not_override_existing_row(pg_admin) -> None:
    adb.init_admin_db()
    with pg_admin.connect() as conn:
        conn.execute("UPDATE admin_users SET password = %s", (adb.hash_password("chosen"),))
        conn.commit()
    assert adb.authenticate_admin("admin", "env-admin-password") is None


def test_save_duplicate_raises_admin_user_exists_case_insensitive(pg_admin) -> None:
    adb.save_dev_admin_user("Op", "op@example.com", "pw-one-two")
    with pytest.raises(adb.AdminUserExists):
        adb.save_dev_admin_user("op", "other@example.com", "pw")
    with pytest.raises(adb.AdminUserExists):
        adb.save_dev_admin_user("op2", "OP@example.com", "pw")
    assert adb.authenticate_admin("op@example.com", "pw-one-two") is not None


def test_totp_round_trip(pg_admin) -> None:
    uid, _ = adb.authenticate_admin("admin", "env-admin-password")
    assert adb.set_admin_totp(uid, secret="ab cd", enabled=True)
    assert adb.get_admin_totp(uid) == {"enabled": True, "secret": "ABCD"}
    assert adb.set_admin_totp(999999, secret="X", enabled=True) is False
    assert adb.get_admin_totp(999999) is None


def test_production_without_admin_password_refuses_seed(pg_admin, monkeypatch) -> None:
    monkeypatch.setenv("FLASK_ENV", "production")
    monkeypatch.setenv("ADMIN_PASSWORD", "")
    with pytest.raises(RuntimeError):
        adb.init_admin_db()
