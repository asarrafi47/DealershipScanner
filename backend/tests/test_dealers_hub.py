"""Tests for site-admin dealer onboarding hub at ``/admin/dealers``."""

from __future__ import annotations

import importlib

from backend.db.users_db import save_user
from backend.utils.ip_rate_limit import clear_rate_limit_state
from backend.utils.roles import ROLE_ADMIN


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
    import backend.main as main

    importlib.reload(main)
    return main.app


def _login_site_admin(client) -> str:
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
    return session.get("_csrf_token") or ""


def test_dealers_hub_post_rejects_javascript_url(monkeypatch, tmp_path) -> None:
    clear_rate_limit_state()
    app = _fresh_app(monkeypatch, tmp_path)
    save_user("hubadmin", "hubadmin@example.com", "long-enough-password", role=ROLE_ADMIN)
    monkeypatch.setattr(
        "backend.dealer.admin.onboard_api.is_inventory_postgres",
        lambda: True,
    )
    enqueue_calls: list[dict] = []

    def _track_enqueue(**kwargs):
        enqueue_calls.append(kwargs)
        return 99

    monkeypatch.setattr("backend.dealer.admin.onboard_api.enqueue_job", _track_enqueue)

    client = app.test_client()
    with client:
        csrf = _login_site_admin(client)
        rv = client.post(
            "/admin/dealers",
            data={
                "csrf_token": csrf,
                "dealer_id": "evil-dealer",
                "url": "javascript:alert(1)",
            },
            follow_redirects=True,
        )
    assert rv.status_code == 200
    assert not enqueue_calls
    assert b"valid http or https" in rv.data.lower()


def test_dealers_hub_post_enqueues_valid_url(monkeypatch, tmp_path) -> None:
    clear_rate_limit_state()
    app = _fresh_app(monkeypatch, tmp_path)
    save_user("hubadmin", "hubadmin@example.com", "long-enough-password", role=ROLE_ADMIN)
    monkeypatch.setattr(
        "backend.dealer.admin.onboard_api.is_inventory_postgres",
        lambda: True,
    )
    enqueue_calls: list[dict] = []

    def _track_enqueue(**kwargs):
        enqueue_calls.append(kwargs)
        return 42

    monkeypatch.setattr("backend.dealer.admin.onboard_api.enqueue_job", _track_enqueue)
    monkeypatch.setattr(
        "backend.scanner.job_queue.list_dealer_scan_registry",
        lambda **kwargs: [],
    )
    monkeypatch.setattr(
        "backend.scanner.job_queue.list_recent_jobs",
        lambda **kwargs: [],
    )

    client = app.test_client()
    with client:
        csrf = _login_site_admin(client)
        rv = client.post(
            "/admin/dealers",
            data={
                "csrf_token": csrf,
                "url": "https://www.example-dealer.com/inventory",
                "name": "Example Dealer",
            },
            follow_redirects=True,
        )
    assert rv.status_code == 200
    assert len(enqueue_calls) == 1
    assert enqueue_calls[0]["job_type"] == "onboard"
    assert enqueue_calls[0]["payload"]["url"] == "https://www.example-dealer.com/inventory"
    assert enqueue_calls[0]["payload"]["name"] == "Example Dealer"
    assert b"Queued onboard job #42" in rv.data
