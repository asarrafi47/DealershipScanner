"""Branded HTML 404 for public routes."""

from __future__ import annotations

from backend.main import app


def test_car_not_found_renders_branded_page() -> None:
    with app.test_client() as c:
        r = c.get("/car/99999999")
    assert r.status_code == 404
    body = r.get_data(as_text=True)
    assert "Page not found" in body
    assert "Sarrafi Cars" in body
    assert "Browse inventory" in body


def test_api_not_found_returns_json() -> None:
    with app.test_client() as c:
        r = c.get("/api/nope")
    assert r.status_code == 404
    assert r.is_json
    assert r.get_json().get("error") == "not_found"
