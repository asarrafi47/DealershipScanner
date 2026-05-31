"""Compare page spec builder and route tests."""

from __future__ import annotations


def test_parse_compare_car_ids_dedupes_and_limits():
    from backend.utils.compare_specs import parse_compare_car_ids

    assert parse_compare_car_ids("1,2,1,3,4,5") == [1, 2, 3, 4]
    assert parse_compare_car_ids("abc,0,-1") == []


def test_build_compare_context_marks_diffs():
    from backend.utils.compare_specs import build_compare_context

    raw = [
        {
            "id": 101,
            "title": "2020 Toyota Camry LE",
            "make": "Toyota",
            "model": "Camry",
            "trim": "LE",
            "year": 2020,
            "price": 20000,
            "mileage": 30000,
            "fuel_type": "Gas",
            "transmission": "Automatic",
            "drivetrain": "FWD",
            "body_style": "Sedan",
            "condition": "Used",
            "exterior_color": "White",
            "interior_color": "Black",
            "vin": "1FAFP404X1F123456",
            "dealer_name": "Test Dealer",
            "image_url": "https://example.com/a.jpg",
            "gallery": "[]",
            "packages": "{}",
        },
        {
            "id": 102,
            "title": "2021 Toyota Camry SE",
            "make": "Toyota",
            "model": "Camry",
            "trim": "SE",
            "year": 2021,
            "price": 24000,
            "mileage": 12000,
            "fuel_type": "Gas",
            "transmission": "Automatic",
            "drivetrain": "FWD",
            "body_style": "Sedan",
            "condition": "Used",
            "exterior_color": "Blue",
            "interior_color": "Gray",
            "vin": "1FAFP404X1F654321",
            "dealer_name": "Test Dealer",
            "image_url": "https://example.com/b.jpg",
            "gallery": "[]",
            "packages": "{}",
        },
    ]
    ctx = build_compare_context(raw)
    assert len(ctx["cars"]) == 2
    assert len(ctx["compare_rows"]) >= 10
    price_row = next(r for r in ctx["compare_rows"] if r["key"] == "price")
    assert price_row["differs"] is True
    make_row = next(r for r in ctx["compare_rows"] if r["key"] == "make")
    assert make_row["differs"] is False


def test_compare_page_renders_spec_rows(monkeypatch, tmp_path):
    """Compare UI requires an app session (SEC-013)."""
    monkeypatch.setenv("USERS_DB_PATH", str(tmp_path / "users.db"))
    monkeypatch.setenv("INVENTORY_DB_PATH", str(tmp_path / "inventory.db"))
    monkeypatch.setenv("BILLING_STRIPE_ENABLED", "0")
    from backend.db.inventory_db import init_inventory_db
    from backend.db.users_db import init_users_db, save_user
    from backend.main import app

    init_users_db()
    init_inventory_db()
    app.config["TESTING"] = True
    uid = save_user("compareuser", "compare@example.com", "longpassword123", role="general", org_id=None)

    client = app.test_client()
    with client.session_transaction() as sess:
        sess["user_id"] = uid
        sess["username"] = "compareuser"
        sess["mfa_ok"] = True
    r = client.get("/compare?ids=1")
    assert r.status_code == 200
    body = r.data.decode("utf-8", errors="replace")
    assert "Compare vehicles" in body
    if "No vehicles to compare" not in body:
        assert "compare-table__row" in body
        assert "Engine" in body or "Price" in body
