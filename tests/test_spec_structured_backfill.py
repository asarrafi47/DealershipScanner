"""Structured spec backfill (mocked vPIC)."""

from __future__ import annotations

import json

from backend import spec_structured_backfill as ssb


def _completeish_car(**overrides):
    base = {
        "id": 9901,
        "vin": "1HGBH41JXMN109185",
        "title": "2020 Honda Accord LX",
        "year": 2020,
        "make": "Honda",
        "model": "Accord",
        "trim": "LX",
        "price": 25500.0,
        "mileage": 12000,
        "image_url": "https://cdn.example/1.jpg",
        "exterior_color": "Black",
        "interior_color": "Gray",
        "fuel_type": "Gasoline",
        "transmission": "Automatic",
        "drivetrain": "FWD",
        "cylinders": 4,
        "body_style": None,
        "engine_description": None,
        "spec_source_json": None,
        "gallery": [],
    }
    base.update(overrides)
    return base


def test_apply_structured_backfill_vpic_body_style(monkeypatch) -> None:
    captured: list[tuple[int, dict]] = []

    monkeypatch.setattr(ssb, "get_car_by_id", lambda cid: dict(_completeish_car(id=cid)) if cid == 9901 else None)

    def capture_update(cid: int, fields: dict) -> None:
        captured.append((cid, fields))

    monkeypatch.setattr(ssb, "update_car_row_partial", capture_update)
    monkeypatch.setattr(ssb, "refresh_car_data_quality_score", lambda _cid: None)

    def fake_get(_url: str) -> dict:
        return {
            "Results": [
                {
                    "Make": "HONDA",
                    "Model": "Accord",
                    "ModelYear": "2020",
                    "BodyClass": "Sedan/Saloon",
                    "ErrorText": "",
                }
            ]
        }

    r = ssb.apply_structured_spec_backfill_for_car(
        9901,
        dry_run=False,
        get_json=fake_get,
        use_vpic_cache=False,
    )
    assert r.applied is True
    assert "body_style" in r.tier2_fields
    assert captured
    _cid, fields = captured[0]
    assert _cid == 9901
    assert fields.get("body_style") == "Sedan/Saloon"
    prov = json.loads(fields["spec_source_json"])
    assert prov["body_style"]["source"] == "nhtsa_vpic"


def test_apply_dry_run_no_db_writes(monkeypatch) -> None:
    monkeypatch.setattr(ssb, "get_car_by_id", lambda cid: dict(_completeish_car(id=cid)) if cid == 9902 else None)

    def no_write(*_a, **_k):
        raise AssertionError("no write")

    monkeypatch.setattr(ssb, "update_car_row_partial", no_write)
    monkeypatch.setattr(ssb, "refresh_car_data_quality_score", lambda *_a, **_k: None)

    def fake_get(_url: str) -> dict:
        return {
            "Results": [
                {
                    "Make": "HONDA",
                    "Model": "Accord",
                    "ModelYear": "2020",
                    "BodyClass": "Coupe",
                    "ErrorText": "",
                }
            ]
        }

    r = ssb.apply_structured_spec_backfill_for_car(9902, dry_run=True, get_json=fake_get, use_vpic_cache=False)
    assert r.skip_reason == "dry_run"
    assert r.has_pending_patch is True
    assert "body_style" in r.tier2_fields


def test_skip_already_complete(monkeypatch) -> None:
    car = _completeish_car(id=9903, body_style="Sedan", engine_description="2.0L")
    monkeypatch.setattr(ssb, "get_car_by_id", lambda cid: dict(car) if cid == 9903 else None)
    r = ssb.apply_structured_spec_backfill_for_car(9903, dry_run=False, get_json=lambda _u: {}, use_vpic_cache=False)
    assert r.skip_reason == "already_complete"
