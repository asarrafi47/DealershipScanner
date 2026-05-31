"""Window sticker storage and premium packages API."""

from __future__ import annotations

from backend.enrichment.knowledge_engine import prepare_car_detail_context
from backend.enrichment.window_sticker_service import (
    _merge_packages,
    _sticker_storage_key,
    car_sticker_packages_need_analysis,
    window_sticker_has_visual,
    window_sticker_local_path,
    window_sticker_visual_local_path,
)
from backend.scanner.window_sticker import get_window_sticker_url


def test_merge_packages_sticker_options() -> None:
    merged = _merge_packages(
        '{"sticker_options": ["A"]}',
        {"sticker_options": ["B", "a"]},
    )
    import json

    data = json.loads(merged)
    assert "A" in data["sticker_options"]
    assert "B" in data["sticker_options"]


def test_prepare_car_detail_includes_sticker_options() -> None:
    car = {
        "gallery": [],
        "packages": (
            '{"sticker_options_priced": [{"name": "Premium Group", "price": 2595, '
            '"label": "Premium Group — $2,595"}], "sticker_exterior_color": "Pearl White"}'
        ),
    }
    ctx = prepare_car_detail_context(car)
    opts = ctx.get("listing_sticker_options") or []
    groups = ctx.get("listing_sticker_option_groups") or []
    sections = ctx.get("listing_sticker_option_sections") or {}
    assert any(
        (isinstance(o, dict) and o.get("name") == "Premium Group" and o.get("price") == 2595)
        for o in opts
    )
    assert groups
    assert sections.get("options") or sections.get("base")
    assert ctx.get("sticker_exterior_color") == "Pearl White"
    assert ctx.get("packages_panel_has_content") is True


def test_api_window_sticker_requires_premium(monkeypatch) -> None:
    from backend import main as main_mod

    monkeypatch.setattr(main_mod, "_billing_enabled", lambda: True)
    monkeypatch.setattr(main_mod, "_session_has_paid_access", lambda: False)
    with main_mod.app.test_client() as c:
        r = c.get("/api/cars/1/window-sticker")
    assert r.status_code == 403


def test_jeep_window_sticker_url() -> None:
    url = get_window_sticker_url("1C4RJHBG9SC340097")
    assert url is not None
    assert "jeep.com/hostd/windowsticker/getWindowStickerPdf.do" in url
    assert "1C4RJHBG9SC340097" in url


def test_sticker_storage_key_uses_dealer_slug() -> None:
    assert _sticker_storage_key(dealer_id="kefferjeep-com") == "kefferjeep-com"
    assert _sticker_storage_key(dealership_registry_id=42) == "42"


def test_window_sticker_local_path_finds_slug_stored_file(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(
        "backend.enrichment.window_sticker_service.CAR_WINDOW_STICKERS_DIR",
        tmp_path,
    )
    vin = "1C4RJHBG9SC340097"
    dest = tmp_path / "kefferjeep-com" / vin / "window_sticker.pdf"
    dest.parent.mkdir(parents=True)
    dest.write_bytes(b"%PDF-" + b"x" * 600)
    found = window_sticker_local_path(vin, dealer_id="kefferjeep-com")
    assert found == dest


def test_window_sticker_visual_local_path_ignores_txt(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(
        "backend.enrichment.window_sticker_service.CAR_WINDOW_STICKERS_DIR",
        tmp_path,
    )
    vin = "1C4RJHBG9SC340097"
    dest = tmp_path / "9" / vin / "window_sticker.txt"
    dest.parent.mkdir(parents=True)
    dest.write_text("Model: 2018 Ram 2500\nEngine: 6.7L Cummins\n" + ("Option line\n" * 30), encoding="utf-8")
    car = {"vin": vin, "dealer_id": "db-9", "dealership_registry_id": 9}
    assert window_sticker_local_path(vin, dealer_id="db-9", dealership_registry_id=9) == dest
    assert window_sticker_visual_local_path(vin, dealer_id="db-9", dealership_registry_id=9) is None
    assert window_sticker_has_visual(car) is False


def test_prepare_car_detail_skips_empty_trim_as_package() -> None:
    car = {
        "trim": "Summit Reserve",
        "gallery": [],
        "packages": '{"packages_normalized": [{"name": "Summit Reserve", "features": []}]}',
    }
    ctx = prepare_car_detail_context(car)
    assert (ctx.get("listing_packages_sections") or []) == []


def test_car_sticker_packages_need_analysis_when_pdf_only(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(
        "backend.enrichment.window_sticker_service.CAR_WINDOW_STICKERS_DIR",
        tmp_path,
    )
    vin = "1C4RJHBG9SC340097"
    dest = tmp_path / "9" / vin / "window_sticker.pdf"
    dest.parent.mkdir(parents=True)
    dest.write_bytes(b"%PDF-" + b"x" * 600)
    car = {"vin": vin, "dealer_id": "db-9", "dealership_registry_id": 9, "packages": None}
    assert car_sticker_packages_need_analysis(car) is True


def test_api_packages_ensure_requires_premium(monkeypatch) -> None:
    from backend import main as main_mod

    monkeypatch.setattr(main_mod, "_billing_enabled", lambda: True)
    monkeypatch.setattr(main_mod, "_session_has_paid_access", lambda: False)
    with main_mod.app.test_client() as c:
        r = c.post("/api/cars/1/packages/ensure", headers={"X-CSRF-Token": "test"})
    assert r.status_code == 403


def test_api_packages_ensure_csrf_passes_when_token_matches(monkeypatch) -> None:
    """Regression: ``if not validate_csrf_header()`` wrongly treated success (None) as failure."""
    from backend import main as main_mod

    monkeypatch.setattr(main_mod, "_billing_enabled", lambda: False)
    monkeypatch.setattr(main_mod, "_session_has_paid_access", lambda: True)
    monkeypatch.setattr(
        "backend.enrichment.window_sticker_service.ensure_window_sticker_for_car",
        lambda car_id, **_: {
            "ok": True,
            "window_sticker_available": False,
            "fetch_error": "oem_fetch_failed",
        },
    )
    monkeypatch.setattr(
        "backend.enrichment.listing_packages_service.ensure_listing_packages_for_car",
        lambda car_id, **_: {
            "ok": True,
            "window_sticker_available": False,
            "fetch_error": "oem_fetch_failed",
        },
    )
    monkeypatch.setattr(
        "backend.main.get_car_by_id",
        lambda car_id, **_: {
            "id": car_id,
            "vin": "1C4RJHBG9SC340097",
            "make": "Jeep",
            "model": "Grand Cherokee",
            "gallery": [],
            "packages": None,
        },
    )
    monkeypatch.setattr(
        "backend.enrichment.knowledge_engine.prepare_car_detail_context",
        lambda car: {"packages_panel_has_content": False, "listing_sticker_options": []},
    )
    monkeypatch.setattr(
        "backend.enrichment.window_sticker_service.sticker_panel_payload",
        lambda ctx, car=None: {"listing_sticker_options": []},
    )
    monkeypatch.setattr(
        "backend.enrichment.window_sticker_service.window_sticker_available",
        lambda car: False,
    )

    import secrets

    token = secrets.token_urlsafe(32)
    with main_mod.app.test_client() as c:
        with c.session_transaction() as sess:
            sess["_csrf_token"] = token
            sess["user_id"] = 1
        r = c.post(
            "/api/cars/42/packages/ensure",
            headers={"X-CSRF-Token": token},
        )
    assert r.status_code == 200
    body = r.get_json()
    assert body.get("ok") is True
