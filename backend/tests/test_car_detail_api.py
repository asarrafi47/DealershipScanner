"""GET /api/cars/<id> aggregate JSON for native clients."""

from __future__ import annotations

from backend.main import app


def _sample_car(car_id: int = 42) -> dict:
    return {
        "id": car_id,
        "vin": "1C4RJFBG0MC123456",
        "make": "Jeep",
        "model": "Grand Cherokee",
        "year": 2021,
        "trim": "Limited",
        "price": 32999,
        "mileage": 41000,
        "dealer_id": "demo-dealer",
        "dealership_registry_id": None,
        "source_url": "https://example.com/vdp/1",
        "photo_urls_json": '["https://example.com/photo1.jpg"]',
        "packages_json": "[]",
        "active": 1,
    }


def test_api_car_detail_not_found(monkeypatch) -> None:
    monkeypatch.setattr("backend.main.get_car_by_id", lambda *_a, **_k: None)
    with app.test_client() as client:
        rv = client.get("/api/cars/99999999")
    assert rv.status_code == 404
    assert rv.get_json().get("error") == "not_found"


def test_api_car_detail_ok(monkeypatch) -> None:
    car = _sample_car(42)
    monkeypatch.setattr("backend.main.get_car_by_id", lambda cid, **kw: car if cid == 42 else None)
    monkeypatch.setattr(
        "backend.main.prepare_car_detail_context",
        lambda _raw: {
            "verified_specs": {},
            "gallery_images": ["https://example.com/photo1.jpg"],
            "listing_packages_sections": [],
        },
    )
    with app.test_client() as client:
        rv = client.get("/api/cars/42")
    assert rv.status_code == 200
    body = rv.get_json()
    assert body.get("ok") is True
    assert body["car"]["make"] == "Jeep"
    assert body["car"]["model"] == "Grand Cherokee"
    assert body["gallery_images"] == ["https://example.com/photo1.jpg"]
    assert body["logged_in"] is False
    assert body["market_intel"] is None


def test_api_car_detail_market_intel_when_paid(monkeypatch) -> None:
    car = _sample_car(7)
    monkeypatch.setattr("backend.main.get_car_by_id", lambda cid, **kw: car if cid == 7 else None)
    monkeypatch.setattr("backend.main._session_has_paid_access", lambda: True)
    monkeypatch.setattr(
        "backend.main.prepare_car_detail_context",
        lambda _raw: {"verified_specs": {}, "gallery_images": []},
    )
    monkeypatch.setattr(
        "backend.main.listings_geo_kwargs_from_session",
        lambda _sess: {"zip_code": "92694", "radius_miles": 50},
    )
    monkeypatch.setattr(
        "backend.utils.market_price.market_price_for_car",
        lambda *_a, **_k: {"avg_price": 31000, "sample_count": 12},
    )
    with app.test_client() as client:
        rv = client.get("/api/cars/7")
    assert rv.status_code == 200
    body = rv.get_json()
    assert body["market_intel"]["avg_price"] == 31000
