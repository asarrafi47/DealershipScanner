"""Premium gate and shape for listings dealership picker API."""

from __future__ import annotations

import importlib


def _fresh_app(monkeypatch, tmp_path):
    monkeypatch.setenv("FLASK_ENV", "development")
    monkeypatch.setenv("USERS_DB_PATH", str(tmp_path / "users_test.db"))
    monkeypatch.setenv("DEV_USERS_DB_PATH", str(tmp_path / "dev_users_test.db"))
    monkeypatch.delenv("USERS_DB_ENCRYPTION_KEY", raising=False)
    import backend.main as main

    importlib.reload(main)
    return main.app


def test_nearby_dealers_open_when_billing_disabled(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("BILLING_STRIPE_ENABLED", "0")
    app = _fresh_app(monkeypatch, tmp_path)
    with app.test_client() as client:
        rv = client.get("/api/nearby-dealers?zip_code=28173&radius=25")
    assert rv.status_code == 200
    data = rv.get_json()
    assert data is not None
    assert "dealers" in data


def test_nearby_dealers_premium_required_when_billing_enabled(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("BILLING_STRIPE_ENABLED", "1")
    app = _fresh_app(monkeypatch, tmp_path)
    from backend.db.users_db import save_user
    from backend.utils.roles import ROLE_GENERAL

    save_user("u1", "u1@example.com", "long-enough-password", role=ROLE_GENERAL)
    client = app.test_client()
    with client:
        client.get("/login")
        from flask import session

        csrf = session.get("_csrf_token")
        assert csrf
        client.post(
            "/login",
            data={
                "csrf_token": csrf,
                "login": "u1@example.com",
                "password": "long-enough-password",
            },
            follow_redirects=False,
        )
        rv = client.get("/api/nearby-dealers?zip_code=28173&radius=25")
    assert rv.status_code == 403
    data = rv.get_json()
    assert data is not None
    assert data.get("error") == "premium_required"
    assert data.get("upgrade_plan_id") == "research"
    assert data.get("upgrade_plan_name") == "Research"
    assert data.get("dealers") == []


def test_listings_page_includes_dealer_picker_markup(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("BILLING_STRIPE_ENABLED", "0")
    app = _fresh_app(monkeypatch, tmp_path)
    with app.test_client() as client:
        rv = client.get(
            "/listings?zip_code=28173&radius=25&dealer_registry_id=1&dealer_registry_id=2"
        )
    assert rv.status_code == 200
    html = rv.get_data(as_text=True)
    assert "dealer-filter-group" in html
    assert "ds-listings-paid-access" in html
