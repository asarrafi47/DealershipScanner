"""Structured spec backfill (mocked vPIC)."""

from __future__ import annotations

import json

from backend.enrichment import spec_structured_backfill as ssb


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


# --------------------------------------------------------------------------
# vPIC trim corroboration guard (audit A.1 #9): a decoder trim the listing
# never names must not land on cars.trim, and the refusal must be recorded.
# --------------------------------------------------------------------------


def _vpic_car(cid: int, **overrides):
    """Row missing transmission (hot-path entry condition) with empty trim."""
    base = _completeish_car(
        id=cid,
        trim=None,
        transmission=None,
        body_style="Sedan",
        engine_description="1.5L I4",
        # No model token in title so tier-1 title inference cannot fill trim.
        title="2020 Certified Pre-Owned Vehicle",
        description="Great condition, one owner.",
        model_full_raw=None,
        source_url="https://dealer.example/inventory/123",
    )
    base.update(overrides)
    return base


def _wire(monkeypatch, car: dict, captured: list) -> None:
    cid = car["id"]
    monkeypatch.setattr(ssb, "get_car_by_id", lambda c: dict(car) if c == cid else None)
    monkeypatch.setattr(ssb, "update_car_row_partial", lambda c, fields: captured.append((c, fields)))
    monkeypatch.setattr(ssb, "refresh_car_data_quality_score", lambda _cid: None)
    # Isolate tier 2: tier 1 touches EPA/catalog lookups.
    monkeypatch.setattr(ssb, "collect_row_storage_repairs", lambda _raw: {})


def _vpic_response(**fields) -> dict:
    row = {"Make": "HONDA", "Model": "Accord", "ModelYear": "2020", "ErrorText": ""}
    row.update(fields)
    return {"Results": [row]}


def test_uncorroborated_vpic_trim_not_written_but_rejection_recorded(monkeypatch) -> None:
    captured: list[tuple[int, dict]] = []
    _wire(monkeypatch, _vpic_car(9910), captured)

    r = ssb.apply_structured_spec_backfill_for_car(
        9910,
        dry_run=False,
        get_json=lambda _u: _vpic_response(Trim="Touring", TransmissionStyle="Automatic"),
        use_vpic_cache=False,
    )
    assert r.applied is True
    # transmission backfill (the reason the row entered) is unaffected
    assert "transmission" in r.tier2_fields
    assert "trim" not in r.tier2_fields
    _cid, fields = captured[0]
    assert fields.get("transmission") == "Automatic"
    assert "trim" not in fields
    prov = json.loads(fields["spec_source_json"])
    assert prov["transmission"]["source"] == "nhtsa_vpic"
    assert "trim" not in prov
    rej = prov["trim_rejected"]
    assert rej["source"] == "nhtsa_vpic"
    assert rej["value"] == "Touring"
    assert rej["reason"] == "not_named_in_listing"


def test_corroborated_vpic_trim_written_with_provenance(monkeypatch) -> None:
    captured: list[tuple[int, dict]] = []
    car = _vpic_car(9911, title="2020 Honda Accord Touring for sale")
    _wire(monkeypatch, car, captured)

    r = ssb.apply_structured_spec_backfill_for_car(
        9911,
        dry_run=False,
        get_json=lambda _u: _vpic_response(Trim="Touring", TransmissionStyle="Automatic"),
        use_vpic_cache=False,
    )
    assert r.applied is True
    assert "trim" in r.tier2_fields
    _cid, fields = captured[0]
    assert fields.get("trim") == "Touring"
    prov = json.loads(fields["spec_source_json"])
    assert prov["trim"]["source"] == "nhtsa_vpic"
    assert "trim_rejected" not in prov


def test_multi_value_vpic_trim_refused_even_when_one_token_matches(monkeypatch) -> None:
    captured: list[tuple[int, dict]] = []
    car = _vpic_car(9912, title="2023 Fisker Ocean Wind")
    _wire(monkeypatch, car, captured)

    r = ssb.apply_structured_spec_backfill_for_car(
        9912,
        dry_run=False,
        get_json=lambda _u: _vpic_response(
            Trim="Light, Light Long Range, Wind", TransmissionStyle="Automatic"
        ),
        use_vpic_cache=False,
    )
    assert r.applied is True
    assert "trim" not in r.tier2_fields
    _cid, fields = captured[0]
    assert "trim" not in fields
    prov = json.loads(fields["spec_source_json"])
    rej = prov["trim_rejected"]
    assert rej["reason"] == "multi_value_list"
    assert rej["value"] == "Light, Light Long Range, Wind"


def test_trim_only_rejection_still_persists_provenance(monkeypatch) -> None:
    # Nothing else fillable: the refusal alone is still written to spec_source_json,
    # cars.trim is not.
    captured: list[tuple[int, dict]] = []
    car = _vpic_car(9913, transmission="Automatic")
    _wire(monkeypatch, car, captured)

    r = ssb.apply_structured_spec_backfill_for_car(
        9913,
        dry_run=False,
        get_json=lambda _u: _vpic_response(Trim="Touring"),
        use_vpic_cache=False,
    )
    assert r.applied is False
    assert r.skip_reason == "no_fillable_fields"
    assert captured
    _cid, fields = captured[0]
    assert set(fields.keys()) == {"spec_source_json"}
    prov = json.loads(fields["spec_source_json"])
    assert prov["trim_rejected"]["reason"] == "not_named_in_listing"


def test_overwrite_dealer_env_cannot_bypass_trim_guard(monkeypatch) -> None:
    captured: list[tuple[int, dict]] = []
    car = _vpic_car(9914, trim="LX")
    _wire(monkeypatch, car, captured)
    monkeypatch.setenv("SPEC_STRUCTURED_VPIC_OVERWRITE_DEALER", "1")

    r = ssb.apply_structured_spec_backfill_for_car(
        9914,
        dry_run=False,
        get_json=lambda _u: _vpic_response(Trim="Touring", TransmissionStyle="Automatic"),
        use_vpic_cache=False,
    )
    assert r.applied is True
    assert "trim" not in r.tier2_fields
    _cid, fields = captured[0]
    assert "trim" not in fields
    prov = json.loads(fields["spec_source_json"])
    assert prov["trim_rejected"]["reason"] == "not_named_in_listing"


def test_skip_already_complete(monkeypatch) -> None:
    car = _completeish_car(id=9903, body_style="Sedan", engine_description="2.0L")
    monkeypatch.setattr(ssb, "get_car_by_id", lambda cid: dict(car) if cid == 9903 else None)
    r = ssb.apply_structured_spec_backfill_for_car(9903, dry_run=False, get_json=lambda _u: {}, use_vpic_cache=False)
    assert r.skip_reason == "already_complete"
