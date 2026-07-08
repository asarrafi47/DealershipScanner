"""Post-scan OEM window sticker stage."""

from __future__ import annotations

from backend.scanner.post_pipeline import (
    post_window_sticker_env_enabled,
    run_window_sticker_for_vins,
)


def test_post_window_sticker_env_default_on(monkeypatch) -> None:
    monkeypatch.delenv("SCANNER_POST_WINDOW_STICKER", raising=False)
    assert post_window_sticker_env_enabled() is True
    monkeypatch.setenv("SCANNER_POST_WINDOW_STICKER", "0")
    assert post_window_sticker_env_enabled() is False


def test_run_window_sticker_for_vins(monkeypatch) -> None:
    calls: list[int] = []

    def fake_get_car(vin: str):
        if vin == "1C4RJHBG9SC340097":
            # ``year`` required since 7d3c76e1c: OEM sticker auto-fetch is gated to
            # CDJR/Stellantis model year >= 2018 (cdjr_oem_window_sticker_eligible).
            return {"id": 99, "vin": vin, "dealer_id": "test-dealer-com", "year": 2025}
        return None

    def fake_available(car):
        return False

    def fake_ensure(car_id: int, **kwargs):
        calls.append(car_id)
        return {"window_sticker_available": True, "stored": True}

    monkeypatch.setattr("backend.db.inventory_db.get_car_by_vin", fake_get_car)
    monkeypatch.setattr(
        "backend.enrichment.window_sticker_service.window_sticker_available",
        fake_available,
    )
    monkeypatch.setattr(
        "backend.enrichment.window_sticker_service.ensure_window_sticker_for_car",
        fake_ensure,
    )
    monkeypatch.setattr(
        "backend.scanner.window_sticker.get_window_sticker_url",
        lambda vin: f"https://example.com/{vin}" if len(vin) == 17 else None,
    )

    stats = run_window_sticker_for_vins(["1C4RJHBG9SC340097", "SHORT"])
    assert stats["stored"] == 1
    assert stats["attempted"] == 1
    assert calls == [99]
