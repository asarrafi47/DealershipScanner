"""Flask app exposes every route in backend.mobile.contract (iOS API surface)."""

from __future__ import annotations

import pytest

from backend.mobile.contract import MOBILE_API_ROUTES, MobileRoute


def _rule_keys(app) -> set[tuple[str, str]]:
    out: set[tuple[str, str]] = set()
    for rule in app.url_map.iter_rules():
        if rule.endpoint == "static":
            continue
        methods = {m for m in rule.methods if m not in ("HEAD", "OPTIONS")}
        for method in methods:
            out.add((method, rule.rule))
    return out


@pytest.mark.parametrize("route", MOBILE_API_ROUTES, ids=lambda r: f"{r.method} {r.path}")
def test_mobile_route_registered(route: MobileRoute, monkeypatch, tmp_path, app_factory) -> None:
    app = app_factory().app
    keys = _rule_keys(app)
    assert (route.method, route.path) in keys, f"missing {route.method} {route.path}"


@pytest.mark.parametrize("route", MOBILE_API_ROUTES, ids=lambda r: r.endpoint)
def test_mobile_endpoint_name(route: MobileRoute, monkeypatch, tmp_path, app_factory) -> None:
    app = app_factory().app
    endpoints = {rule.endpoint for rule in app.url_map.iter_rules()}
    assert route.endpoint in endpoints


def test_contract_matches_ios_doc_route_count() -> None:
    """Guardrail: ios/docs/API_CONTRACT.md table should list the same routes."""
    assert len(MOBILE_API_ROUTES) == 15


# ── area-scoped listings (contract v2) ──────────────────────────────────


@pytest.fixture
def area_client(sqlite_inventory, monkeypatch):
    import json as _json

    from backend.db.repositories import grid_cards_repo as gc
    from backend.db.repositories import listings_repo as lr
    from backend.routes import listings_api

    lr.clear_inventory_listings_cache()
    gc.reset_grid_cards_state()
    listings_api.clear_cars_scope_cache()
    sqlite_inventory.add_cars(
        [
            {
                "title": "2022 Toyota Camry",
                "year": 2022,
                "make": "Toyota",
                "model": "Camry",
                "price": 20000,
                "gallery": _json.dumps([]),
                "dealer_url": "https://near.test",
                "dealer_id": "near-test",
            }
        ]
    )
    from backend.main import app

    app.config["TESTING"] = True
    with app.test_client() as c:
        yield c
    lr.clear_inventory_listings_cache()


def _body(resp):
    import gzip as _gzip
    import json as _json

    raw = resp.get_data()
    if resp.headers.get("Content-Encoding") == "gzip":
        raw = _gzip.decompress(raw)
    return _json.loads(raw)


def test_listings_cars_without_an_area_is_the_documented_no_area_shape(area_client) -> None:
    from backend.mobile.contract import LISTINGS_AREA_CONTRACT

    spec = LISTINGS_AREA_CONTRACT["routes"]["GET /api/listings/cars"]["no_area"]
    r = area_client.get("/api/listings/cars")  # exactly what iOS fetchListings() sends
    assert r.status_code == spec["status"]
    body = r.get_json()
    assert set(body) == set(spec["keys"])
    assert body["error"] == "zip_required" and body["area_required"] is True
    assert body["api_version"] == LISTINGS_AREA_CONTRACT["version"]
    assert body["cars"] == []


def test_listings_cars_without_zip_uses_the_session_area(area_client) -> None:
    """iOS: POST /api/session/listings-geo, then GET /api/listings/cars (no params)."""
    from backend.mobile.contract import LISTINGS_AREA_CONTRACT

    with area_client.session_transaction() as sess:
        sess["listings_geo_zip"] = "92694"
        sess["listings_geo_radius_mi"] = 25.0
    r = area_client.get("/api/listings/cars")
    assert r.status_code == 200
    body = _body(r)
    spec = LISTINGS_AREA_CONTRACT["routes"]["GET /api/listings/cars"]
    assert set(body) == set(spec["ok_keys"])
    assert body["ok"] is True and body["zip"] == "92694" and body["radius"] == 25
    assert body["api_version"] == 2
    assert isinstance(body["cars"], list)
    # Derived from the cookie: never a shared-cache body.
    assert r.headers["Cache-Control"].startswith("private")


def test_listings_cars_accepts_the_documented_param_aliases(area_client) -> None:
    body = _body(area_client.get("/api/listings/cars?zip_code=92694&radius_miles=100"))
    assert body["zip"] == "92694" and body["radius"] == 100


def test_smart_search_without_an_area_is_the_documented_no_area_shape(area_client) -> None:
    from backend.mobile.contract import LISTINGS_AREA_CONTRACT

    csrf = "mobile-contract-test-csrf-token-32chars"
    with area_client.session_transaction() as sess:
        sess["_csrf_token"] = csrf
    spec = LISTINGS_AREA_CONTRACT["routes"]["POST /api/search/smart"]["no_area"]
    r = area_client.post("/api/search/smart", json={"query": "camry"}, headers={"X-CSRF-Token": csrf})
    assert r.status_code == spec["status"]
    body = r.get_json()
    assert set(body) == set(spec["keys"])
    assert body["results"] == [] and body["api_version"] == 2
