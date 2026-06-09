"""Per-listing packages orchestration (description + window sticker)."""

from __future__ import annotations

from backend.enrichment.listing_packages_service import (
    extract_description_from_listing_html,
    ensure_listing_description_for_car,
)
from backend.enrichment.window_sticker_service import _apply_vision_fallback_merged, _merge_packages


def test_extract_description_from_listing_html_class_block() -> None:
    html = """
    <html><body>
    <div class="vehicle-description">
    Premium Package includes navigation and heated seats.
    Driver Assistance Package with adaptive cruise control.
    </div></body></html>
    """
    text = extract_description_from_listing_html(html)
    assert "Premium Package" in text
    assert "Driver Assistance" in text


def test_merge_packages_preserves_existing_keys() -> None:
    merged = _merge_packages(
        '{"packages_normalized": [{"name": "AMG Line"}], "possible_packages": ["AMG Line"]}',
        {"observed_features": ["Panoramic sunroof"], "possible_packages": ["AMG Line"]},
    )
    import json

    data = json.loads(merged)
    assert data.get("packages_normalized")
    assert "Panoramic sunroof" in (data.get("observed_features") or [])


def test_apply_vision_fallback_merged(monkeypatch) -> None:
    from backend.enrichment import window_sticker_service as ws

    updates: list[dict] = []

    def _fake_update(car_id, fields):
        updates.append(fields)

    monkeypatch.setattr(ws, "update_car_row_partial", _fake_update)
    monkeypatch.setattr(
        ws,
        "_vision_fallback_packages",
        lambda car: {"observed_features": ["Roof rails"], "possible_packages": ["Sport Package"]},
    )
    monkeypatch.setattr(ws, "should_skip_photo_package_analysis", lambda car, packages=None: False)
    car = {"id": 1, "packages": '{"possible_packages": ["AMG Line Package"]}'}
    out: dict = {}
    assert _apply_vision_fallback_merged(1, car, out) is True
    assert out.get("vision_fallback") is True
    assert updates
    import json

    pkg = json.loads(updates[0]["packages"])
    assert "AMG Line Package" in (pkg.get("possible_packages") or [])
    assert "Roof rails" in (pkg.get("observed_features") or [])


def test_ensure_listing_description_skips_short_without_url(monkeypatch) -> None:
    row = {"id": 9, "description": "", "make": "Mercedes-Benz", "model": "S-Class", "year": 2022}
    monkeypatch.setattr(
        "backend.enrichment.listing_packages_service.get_car_by_id",
        lambda car_id, **_: row if car_id == 9 else None,
    )
    result = ensure_listing_description_for_car(9, row, refetch_if_missing=False)
    assert result.get("applied") is False
    assert result.get("reason") == "description_too_short"
