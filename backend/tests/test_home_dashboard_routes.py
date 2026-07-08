"""Home (/home) and Dashboard (/dashboard) must serve different pages."""
from __future__ import annotations

import tempfile
from pathlib import Path

import pytest


def _fresh_app(monkeypatch, tmp_path: Path):
    monkeypatch.setenv("USERS_DB_PATH", str(tmp_path / "users.db"))
    monkeypatch.setenv("INVENTORY_DB_PATH", str(tmp_path / "inventory.db"))
    monkeypatch.setenv("BILLING_STRIPE_ENABLED", "0")
    monkeypatch.setenv("DATABASE_URL", "")
    monkeypatch.setenv("INVENTORY_DATABASE_URL", "")
    monkeypatch.setattr(
        "backend.db.inventory_pg.is_inventory_postgres",
        lambda: False,
    )
    from backend.db import inventory_db
    from backend.db.inventory_db import init_inventory_db
    from backend.db.users_db import init_users_db, save_user
    from backend.main import app

    # inventory_db.DB_PATH is resolved once at import time, so the env var above has no
    # effect on it post-import — patch the module attribute directly (matches the pattern
    # in root conftest.py's _isolate_listings_env fixture) or this test silently reuses
    # whatever real sqlite file DB_PATH already pointed to.
    monkeypatch.setattr(inventory_db, "DB_PATH", str(tmp_path / "inventory.db"))

    init_users_db()
    init_inventory_db()
    app.config["TESTING"] = True
    return app, save_user


def _insert_test_car() -> int:
    from backend.db.inventory_db import get_conn

    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO cars (
                vin, title, year, make, model, trim, price, mileage,
                image_url, dealer_name, dealer_url, dealer_id, scraped_at,
                fuel_type, transmission, drivetrain,
                exterior_color, interior_color, gallery, listing_active
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "1HGBH41JXMN109186",
                "2022 Honda Civic Sport",
                2022,
                "Honda",
                "Civic",
                "Sport",
                24500,
                12000,
                "https://example.com/civic.jpg",
                "Test Dealer",
                "https://dealer.test/",
                "t-dealer",
                "2026-01-01T00:00:00Z",
                "Gas",
                "Automatic",
                "FWD",
                "Blue",
                "Black",
                "[]",
                1,
            ),
        )
        conn.commit()
        row = cur.execute("SELECT id FROM cars WHERE vin = ?", ("1HGBH41JXMN109186",)).fetchone()
        return int(row[0])
    finally:
        conn.close()


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
        yield c, Path(td)


def test_root_redirects_signed_in_user_to_home(client):
    c, _ = client
    r = c.get("/", follow_redirects=False)
    assert r.status_code in (302, 303)
    assert r.headers["Location"].endswith("/home")


def test_home_and_dashboard_are_different_pages(client):
    c, _ = client
    home = c.get("/home")
    dash = c.get("/dashboard")
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


def test_home_shows_recently_viewed_section(client):
    c, _td = client
    car_id = _insert_test_car()
    with c.session_transaction() as sess:
        uid = sess["user_id"]
    from backend.db.user_history_db import record_car_view

    record_car_view(int(uid), car_id)
    home = c.get("/home")
    html = home.data.decode()
    assert home.status_code == 200
    assert 'id="viewed-section"' in html
    assert "Recently viewed" in html
    assert "2022 Honda Civic Sport" in html
