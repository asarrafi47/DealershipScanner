"""Tests for site-admin user management (D1)."""

from __future__ import annotations

import importlib

import pytest

from backend.db.users_db import (
    admin_create_user,
    admin_reset_user_password,
    admin_update_user,
    authenticate_app_user,
    delete_user_by_id,
    get_user_admin_record,
    init_users_db,
    list_users_for_admin,
    save_user,
    set_user_is_active,
    sync_env_admin_user_row,
)
from backend.utils.roles import ROLE_ADMIN, ROLE_GENERAL


@pytest.fixture()
def users_db(tmp_path, monkeypatch):
    db_path = tmp_path / "users.db"
    monkeypatch.setenv("USERS_DB_PATH", str(db_path))
    init_users_db()
    return db_path


def _fresh_app(monkeypatch, tmp_path):
    monkeypatch.setenv("FLASK_ENV", "development")
    monkeypatch.setenv("MFA_DELIVERY_MODE", "log")
    monkeypatch.setenv("USERS_DB_PATH", str(tmp_path / "users_test.db"))
    monkeypatch.setenv("DEV_USERS_DB_PATH", str(tmp_path / "dev_users_test.db"))
    monkeypatch.setenv("INVENTORY_DB_PATH", str(tmp_path / "inv_test.db"))
    monkeypatch.delenv("INVENTORY_DATABASE_URL", raising=False)
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.delenv("USERS_DB_ENCRYPTION_KEY", raising=False)
    monkeypatch.delenv("DEV_USERS_DB_ENCRYPTION_KEY", raising=False)
    # inventory_db.DB_PATH is resolved once at import time, so the env var above has no
    # effect on it post-import — patch the module attribute directly.
    from backend.db import inventory_db as inv_db

    monkeypatch.setattr(inv_db, "DB_PATH", str(tmp_path / "inv_test.db"))
    import backend.main as main

    importlib.reload(main)
    return main.app


def test_list_users_for_admin(users_db) -> None:
    save_user("alice", "alice@example.com", "password123", role="general_user")
    save_user("bob", "bob@example.com", "password123", role="dealer_staff")
    rows = list_users_for_admin()
    assert len(rows) >= 2
    emails = {r["email"] for r in rows}
    assert "alice@example.com" in emails
    alice = next(r for r in rows if r["email"] == "alice@example.com")
    assert "plan_label" in alice
    assert "login_methods" in alice
    assert "saved_count" in alice
    assert "scope_label" in alice


def test_suspend_blocks_login(users_db) -> None:
    uid = save_user("susie", "susie@example.com", "password123", role="general_user")
    assert authenticate_app_user("susie@example.com", "password123")
    assert set_user_is_active(uid, active=False)
    assert authenticate_app_user("susie@example.com", "password123") is None
    assert set_user_is_active(uid, active=True)
    assert authenticate_app_user("susie@example.com", "password123")


def test_admin_create_user(users_db) -> None:
    uid, err = admin_create_user(
        username="newbie",
        email="newbie@example.com",
        password="password123",
        role="general_user",
        dealer_id="toyota-cleveland",
    )
    assert err is None
    assert uid and uid > 0
    record = get_user_admin_record(uid)
    assert record is not None
    assert record["username"] == "newbie"
    assert record["email"] == "newbie@example.com"
    assert record["role"] == "general_user"
    assert record.get("dealer_id") == "toyota-cleveland"
    assert authenticate_app_user("newbie@example.com", "password123")


def test_admin_create_user_rejects_invalid_role(users_db) -> None:
    uid, err = admin_create_user(
        username="badrole",
        email="badrole@example.com",
        password="password123",
        role="superuser",
    )
    assert uid is None
    assert err == "Invalid role."


def test_admin_update_user_role(users_db) -> None:
    admin_id = save_user("siteadmin", "siteadmin@example.com", "password123", role=ROLE_ADMIN)
    uid = save_user("member", "member@example.com", "password123", role=ROLE_GENERAL)
    err = admin_update_user(
        uid,
        username="member",
        email="member@example.com",
        role="dealership_admin",
        actor_user_id=admin_id,
    )
    assert err is None
    record = get_user_admin_record(uid)
    assert record is not None
    assert record["role"] == "dealership_admin"


def test_admin_reset_user_password(users_db) -> None:
    uid = save_user("resetme", "resetme@example.com", "oldpassword1", role=ROLE_GENERAL)
    assert authenticate_app_user("resetme@example.com", "oldpassword1")
    err = admin_reset_user_password(uid, "newpassword2", min_password_len=8)
    assert err is None
    assert authenticate_app_user("resetme@example.com", "oldpassword1") is None
    assert authenticate_app_user("resetme@example.com", "newpassword2")


def test_delete_user_by_id(users_db) -> None:
    uid = save_user("gone", "gone@example.com", "password123", role=ROLE_GENERAL)
    ok, err = delete_user_by_id(uid)
    assert ok is True
    assert err is None
    assert get_user_admin_record(uid) is None


def test_delete_env_admin_blocked(users_db, monkeypatch) -> None:
    monkeypatch.setenv("APP_ADMIN_EMAILS", "protected@example.com")
    uid = save_user("protected", "protected@example.com", "password123", role=ROLE_GENERAL)
    sync_env_admin_user_row(uid)
    ok, err = delete_user_by_id(uid)
    assert ok is False
    assert err and "env-configured" in err.lower()


def test_admin_cannot_demote_env_admin(users_db, monkeypatch) -> None:
    monkeypatch.setenv("APP_ADMIN_EMAILS", "protected@example.com")
    actor = save_user("actor", "actor@example.com", "password123", role=ROLE_ADMIN)
    uid = save_user("protected", "protected@example.com", "password123", role=ROLE_ADMIN)
    sync_env_admin_user_row(uid)
    err = admin_update_user(
        uid,
        username="protected",
        email="protected@example.com",
        role=ROLE_GENERAL,
        actor_user_id=actor,
    )
    assert err and "admin role" in err.lower()
    record = get_user_admin_record(uid)
    assert record is not None
    assert record["role"] == ROLE_ADMIN


def test_admin_users_hub_create_via_http(monkeypatch, tmp_path) -> None:
    app = _fresh_app(monkeypatch, tmp_path)
    save_user("hubadmin", "hubadmin@example.com", "long-enough-password", role=ROLE_ADMIN)
    client = app.test_client()
    with client:
        client.get("/login")
        from flask import session

        csrf = session.get("_csrf_token")
        client.post(
            "/login",
            data={
                "csrf_token": csrf,
                "login": "hubadmin@example.com",
                "password": "long-enough-password",
            },
            follow_redirects=True,
        )
        r_list = client.get("/admin/users")
        assert r_list.status_code == 200
        assert b"App users" in r_list.data
        assert b"New user" in r_list.data

        r_new = client.get("/admin/users/new")
        assert r_new.status_code == 200
        csrf = session.get("_csrf_token")
        r_create = client.post(
            "/admin/users/new",
            data={
                "csrf_token": csrf,
                "action": "save",
                "username": "created",
                "email": "created@example.com",
                "password": "long-enough-password",
                "role": "general_user",
            },
            follow_redirects=False,
        )
        assert r_create.status_code in (302, 303)
        assert "/admin/users/" in (r_create.headers.get("Location") or "")
        assert authenticate_app_user("created@example.com", "long-enough-password")
