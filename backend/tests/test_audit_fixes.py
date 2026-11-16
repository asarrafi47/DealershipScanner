"""Regression tests for May 2026 audit fixes (premium gates, chat limits, sticker preview)."""

from __future__ import annotations

import pytest

from backend.routes import cars_pages as _cars_pages_routes


def _make_app(app_factory, tmp_path, **env):
    production = env.pop("FLASK_ENV", "development") == "production"
    return app_factory(
        production=production,
        ALLOW_DEFAULT_APP_USER="0",
        **env,
    )


def test_sticker_preview_requires_premium_when_billing_on(monkeypatch: pytest.MonkeyPatch, tmp_path, app_factory) -> None:
    main = _make_app(app_factory, tmp_path, BILLING_STRIPE_ENABLED="1")
    car = {
        "id": 7,
        "vin": "1C6RRFFG4NN401203",
        "make": "Ram",
        "inactive": 0,
        "gallery": [],
    }
    monkeypatch.setattr(_cars_pages_routes, "get_car_by_id", lambda *_a, **_k: car)

    from pathlib import Path

    fake_pdf = tmp_path / "sticker.pdf"
    fake_pdf.write_bytes(b"%PDF-1.4")
    fake_png = tmp_path / "sticker.png"
    fake_png.write_bytes(b"\x89PNG\r\n\x1a\n")

    def _local_path(**_k):
        return fake_pdf

    def _ensure_png(_path):
        return fake_png

    monkeypatch.setattr(
        "backend.enrichment.window_sticker_service.window_sticker_local_path",
        _local_path,
    )
    monkeypatch.setattr(
        "backend.enrichment.window_sticker_service.ensure_sticker_preview_png",
        _ensure_png,
    )

    client = main.app.test_client()
    rv = client.get("/car/7/window-sticker-preview.png")
    assert rv.status_code == 403


def test_sticker_preview_requires_login_in_production_when_billing_off(
    monkeypatch: pytest.MonkeyPatch, tmp_path, app_factory
) -> None:
    main = _make_app(app_factory, tmp_path, FLASK_ENV="production", BILLING_STRIPE_ENABLED="0")
    car = {
        "id": 7,
        "vin": "1C6RRFFG4NN401203",
        "make": "Ram",
        "inactive": 0,
        "gallery": [],
    }
    monkeypatch.setattr(_cars_pages_routes, "get_car_by_id", lambda *_a, **_k: car)

    from pathlib import Path

    fake_pdf = tmp_path / "sticker.pdf"
    fake_pdf.write_bytes(b"%PDF-1.4")
    fake_png = tmp_path / "sticker.png"
    fake_png.write_bytes(b"\x89PNG\r\n\x1a\n")

    monkeypatch.setattr(
        "backend.enrichment.window_sticker_service.window_sticker_visual_local_path",
        lambda *_a, **_k: fake_pdf,
    )
    monkeypatch.setattr(
        "backend.enrichment.window_sticker_service.ensure_window_sticker_for_car",
        lambda *_a, **_k: None,
    )
    monkeypatch.setattr(
        "backend.enrichment.window_sticker_service.ensure_sticker_preview_png",
        lambda _path: fake_png,
    )

    client = main.app.test_client()
    rv = client.get("/car/7/window-sticker-preview.png")
    assert rv.status_code == 403

    with client.session_transaction() as sess:
        sess["user_id"] = 99
    rv2 = client.get("/car/7/window-sticker-preview.png")
    assert rv2.status_code == 200


def test_car_chat_requires_login_in_production_when_billing_off(monkeypatch: pytest.MonkeyPatch, tmp_path, app_factory) -> None:
    main = _make_app(app_factory, tmp_path, FLASK_ENV="production", BILLING_STRIPE_ENABLED="0")
    monkeypatch.setattr(
        _cars_pages_routes,
        "get_car_by_id",
        lambda *_a, **_k: {"id": 1, "year": 2020, "make": "Test", "model": "Car", "inactive": 0},
    )
    from backend.utils.csrf import _SESSION_KEY

    tok = "t" * 32
    with main.app.test_client() as c:
        with c.session_transaction() as sess:
            sess[_SESSION_KEY] = tok
        rv = c.post(
            "/api/car/1/chat",
            json={"message": "hello"},
            headers={"X-CSRF-Token": tok, "Content-Type": "application/json"},
        )
        assert rv.status_code == 403
        body = rv.get_json()
        assert body is not None
        assert body.get("error") == "login_required"


# The three daily-limit tests that lived here (per user, per car, compare's own namespace) tested
# the in-memory daily buckets removed by card C3. Their replacements run against Postgres in
# test_chat_limits_routes.py: the 11th question in an hour, the per-car limit, and ONE shared
# allowance for the car page, compare chat and the sidebar (compare no longer has its own).


def test_car_page_hides_ask_ai_for_guests(monkeypatch: pytest.MonkeyPatch, tmp_path, app_factory) -> None:
    main = _make_app(app_factory, tmp_path, BILLING_STRIPE_ENABLED="1")
    car = {
        "id": 55,
        "vin": "1C6RRFFG4NN401299",
        "make": "Ram",
        "model": "1500",
        "year": 2024,
        "title": "2024 Ram 1500",
        "trim": "Big Horn",
        "price": 45000,
        "mileage": 10000,
        "dealer_id": "demo",
        "inactive": 0,
        "gallery": [],
        "history_highlights": [],
    }
    monkeypatch.setattr(_cars_pages_routes, "get_car_by_id", lambda *_a, **_k: car)
    monkeypatch.setattr(
        _cars_pages_routes,
        "prepare_car_detail_context",
        lambda _raw: {
            "verified_specs": {},
            "gallery_images": [],
            "packages_panel_has_content": False,
        },
    )

    with main.app.test_client() as client:
        rv = client.get("/car/55")

    assert rv.status_code == 200
    html = rv.get_data(as_text=True)
    assert "car-tab-ask" not in html
    assert "Ask AI" not in html
    assert "car_chat.js" not in html
