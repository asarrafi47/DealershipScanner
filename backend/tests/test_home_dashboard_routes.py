"""Home (/home) and Dashboard (/dashboard) must serve different pages."""
from __future__ import annotations

import os
import tempfile
from pathlib import Path

import pytest


def _fresh_app(monkeypatch, tmp_path: Path):
    monkeypatch.setenv("USERS_DB_PATH", str(tmp_path / "users.db"))
    monkeypatch.setenv("INVENTORY_DB_PATH", str(tmp_path / "inventory.db"))
    monkeypatch.setenv("BILLING_STRIPE_ENABLED", "0")
    from backend.db.inventory_db import init_inventory_db
    from backend.db.users_db import init_users_db, save_user
    from backend.main import app

    init_users_db()
    init_inventory_db()
    app.config["TESTING"] = True
    return app, save_user


@pytest.fixture
def client(monkeypatch):
    with tempfile.TemporaryDirectory() as td:
        app, save_user = _fresh_app(monkeypatch, Path(td))
        c = app.test_client()
        uid = save_user("routeuser", "route@example.com", "longpassword123", role="general", org_id=None)
        with c.session_transaction() as sess:
            sess["user_id"] = uid
            sess["username"] = "routeuser"
            sess["mfa_ok"] = True
        yield c


def test_root_redirects_signed_in_user_to_home(client):
    r = client.get("/", follow_redirects=False)
    assert r.status_code in (302, 303)
    assert r.headers["Location"].endswith("/home")


def test_home_and_dashboard_are_different_pages(client):
    home = client.get("/home")
    dash = client.get("/dashboard")
    assert home.status_code == 200
    assert dash.status_code == 200
    home_html = home.data.decode()
    dash_html = dash.data.decode()
    assert "<title>Home —" in home_html
    assert "<title>Dashboard —" in dash_html
    assert "reco-track" in home_html or "saved-section" in home_html or "find-cars-card--banner" in home_html
    assert "hub-page" in dash_html
    assert "dash-hub-stats" in dash_html
    assert home_html != dash_html
