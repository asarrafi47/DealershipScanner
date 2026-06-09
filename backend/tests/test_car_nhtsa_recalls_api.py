"""GET /api/cars/<id>/nhtsa-recalls JSON for inline VDP recall lookup."""

from __future__ import annotations

from backend.main import app
from backend.tests.test_car_detail_api import _sample_car


def test_api_car_nhtsa_recalls_not_found(monkeypatch) -> None:
    monkeypatch.setattr("backend.main.get_car_by_id", lambda *_a, **_k: None)
    with app.test_client() as client:
        rv = client.get("/api/cars/99999999/nhtsa-recalls")
    assert rv.status_code == 404
    assert rv.get_json().get("error") == "not_found"


def test_api_car_nhtsa_recalls_ok(monkeypatch) -> None:
    car = _sample_car(42)
    monkeypatch.setattr("backend.main.get_car_by_id", lambda cid, **kw: car if cid == 42 else None)
    monkeypatch.setattr(
        "backend.main._nhtsa_recalls_lookup_payload",
        lambda **_kw: (
            {
                "ok": True,
                "vin": car["vin"],
                "recalls": [{"campaign": "24V123", "component": "AIR BAGS", "summary": "Example."}],
                "vehicle_label": "2021 Jeep Grand Cherokee",
            },
            200,
        ),
    )
    with app.test_client() as client:
        rv = client.get("/api/cars/42/nhtsa-recalls")
    assert rv.status_code == 200
    body = rv.get_json()
    assert body.get("ok") is True
    assert body["vin"] == car["vin"]
    assert len(body["recalls"]) == 1


def test_api_nhtsa_recalls_query_ok(monkeypatch) -> None:
    monkeypatch.setattr(
        "backend.main._nhtsa_recalls_lookup_payload",
        lambda **_kw: (
            {
                "ok": True,
                "vin": "1C4RJFBG0MC123456",
                "recalls": [],
                "vehicle_label": "2021 Jeep Grand Cherokee",
            },
            200,
        ),
    )
    with app.test_client() as client:
        rv = client.get(
            "/api/nhtsa-recalls?vin=1C4RJFBG0MC123456&make=Jeep&model=Grand%20Cherokee&year=2021"
        )
    assert rv.status_code == 200
    assert rv.get_json().get("ok") is True
