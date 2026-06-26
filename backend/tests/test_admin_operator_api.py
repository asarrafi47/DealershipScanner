"""Site-admin operator API routes (app session, role=admin)."""

from __future__ import annotations

import importlib

import pytest

from backend.db.users_db import init_users_db, save_user
from backend.utils.roles import ROLE_ADMIN


def _fresh_app(monkeypatch, tmp_path):
    monkeypatch.setenv("FLASK_ENV", "development")
    monkeypatch.setenv("USERS_DB_PATH", str(tmp_path / "users_test.db"))
    monkeypatch.setenv("DEV_USERS_DB_PATH", str(tmp_path / "dev_users_test.db"))
    monkeypatch.setenv("INVENTORY_DB_PATH", str(tmp_path / "inv_test.db"))
    monkeypatch.delenv("INVENTORY_DATABASE_URL", raising=False)
    monkeypatch.delenv("DATABASE_URL", raising=False)
    import backend.main as main

    importlib.reload(main)
    return main.app


def _login_admin(client, tmp_path):
    init_users_db()
    save_user(
        username="siteadmin",
        email="siteadmin@example.com",
        password="SecurePass123!",
        role=ROLE_ADMIN,
    )
    client.get("/login")
    from flask import session

    csrf = session.get("_csrf_token")
    client.post(
        "/login",
        data={
            "csrf_token": csrf,
            "login": "siteadmin@example.com",
            "password": "SecurePass123!",
        },
        follow_redirects=True,
    )


def test_operator_incomplete_cars_forbidden_without_admin(monkeypatch, tmp_path):
    app = _fresh_app(monkeypatch, tmp_path)
    client = app.test_client()
    rv = client.get("/api/admin/operator/incomplete-cars")
    assert rv.status_code == 403


def test_operator_incomplete_cars_ok_for_site_admin(monkeypatch, tmp_path):
    app = _fresh_app(monkeypatch, tmp_path)
    client = app.test_client()
    with client:
        _login_admin(client, tmp_path)
        rv = client.get("/api/admin/operator/incomplete-cars")
    assert rv.status_code == 200
    data = rv.get_json()
    assert data["ok"] is True
    assert "cars" in data
    assert "issues_summary" in data


def test_admin_data_quality_page_requires_site_admin(monkeypatch, tmp_path):
    app = _fresh_app(monkeypatch, tmp_path)
    init_users_db()
    save_user(
        username="member",
        email="member@example.com",
        password="SecurePass123!",
        role="general_user",
    )
    client = app.test_client()
    with client:
        client.get("/login")
        from flask import session

        csrf = session.get("_csrf_token")
        client.post(
            "/login",
            data={
                "csrf_token": csrf,
                "login": "member@example.com",
                "password": "SecurePass123!",
            },
            follow_redirects=True,
        )
        rv = client.get("/admin/data-quality", follow_redirects=False)
    assert rv.status_code == 302


def test_admin_data_quality_page_ok_for_site_admin(monkeypatch, tmp_path):
    app = _fresh_app(monkeypatch, tmp_path)
    monkeypatch.setattr(
        "backend.dealer.admin.data_quality_hub.get_dealership_issue_stats",
        lambda limit=25: [],
    )
    client = app.test_client()
    with client:
        _login_admin(client, tmp_path)
        rv = client.get("/admin/data-quality")
    assert rv.status_code == 200
    assert b"Incomplete listings" in rv.data
    assert b"Dealerships with issues" in rv.data


def test_admin_scanner_ops_page_ok_for_site_admin(monkeypatch, tmp_path):
    app = _fresh_app(monkeypatch, tmp_path)
    client = app.test_client()
    with client:
        _login_admin(client, tmp_path)
        rv = client.get("/admin/scanner-ops")
    assert rv.status_code == 200
    assert b"Bulk magic URLs" in rv.data
