"""GET /api/saved-cars for native Saved tab."""

from __future__ import annotations


def test_saved_cars_requires_login(monkeypatch, tmp_path, app_factory) -> None:
    app = app_factory().app
    with app.test_client() as client:
        rv = client.get("/api/saved-cars")
    assert rv.status_code == 401


def test_saved_cars_returns_saved_list(monkeypatch, tmp_path, app_factory) -> None:
    app = app_factory().app
    from backend.db.users_db import save_user
    from backend.utils.roles import ROLE_GENERAL

    uid = save_user("saved1", "saved1@example.com", "long-enough-password", role=ROLE_GENERAL)
    sample = {
        "id": 101,
        "vin": "1C4RJFBG0MC123456",
        "make": "Jeep",
        "model": "Grand Cherokee",
        "year": 2021,
        "price": 32999,
        "mileage": 41000,
        "active": 1,
    }
    monkeypatch.setattr("backend.routes.listings_api.get_saved_car_ids", lambda _uid: [101] if int(_uid) == int(uid) else [])
    monkeypatch.setattr("backend.routes.listings_api.get_cars_by_ids", lambda ids: [sample] if 101 in ids else [])
    monkeypatch.setattr(
        "backend.routes.listings_api.serialize_cars_for_listings_grid",
        lambda cars: [
            {"id": c["id"], "make": c["make"], "model": c["model"], "year": c["year"]}
            for c in cars
        ],
    )

    with app.test_client() as client:
        with client.session_transaction() as sess:
            sess["user_id"] = int(uid)
        rv = client.get("/api/saved-cars")
    assert rv.status_code == 200
    body = rv.get_json()
    assert body.get("ok") is True
    assert len(body["cars"]) == 1
    assert body["cars"][0]["make"] == "Jeep"
