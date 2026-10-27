"""Window-sticker column writes go through fact precedence (P8B.2).

Pure cases pin ``_car_fields_from_parsed_sticker`` (sticker proposals decided
against the stored row and the VIN decode); the ``pg`` case runs the real
``ensure_window_sticker_for_car`` entry point that ``routes/cars_pages`` calls.
"""
from __future__ import annotations

import json

import pytest

from backend.enrichment import window_sticker_service as wss
from backend.vehicle_facts import precedence as prec

VIN = "1C4RJFAG5NC100001"


@pytest.fixture
def authoritative(monkeypatch):
    monkeypatch.setattr("backend.scanner.window_sticker.oem_sticker_parsing_skip_claude", lambda v: True)


@pytest.fixture
def unverified(monkeypatch):
    monkeypatch.setattr("backend.scanner.window_sticker.oem_sticker_parsing_skip_claude", lambda v: False)


def _vpic(monkeypatch, drive=None, fuel=None, hybrid=False):
    flat = {}
    if drive:
        flat["DriveType"] = drive
    if fuel:
        flat["FuelTypePrimary"] = fuel
    if hybrid:
        flat["ElectrificationLevel"] = "PHEV (Plug-in Hybrid Electric Vehicle)"
    ctx = {"vpic": flat, "vpic_drive": drive} if flat else {}
    monkeypatch.setattr(wss, "_uncached_vpic_context", lambda vin: dict(ctx))


def _car(**kw):
    c = {"id": 1, "vin": VIN, "make": "Jeep", "model": "Grand Cherokee", "trim": "4xe", "title": "2022 Jeep Grand Cherokee 4xe"}
    c.update(kw)
    return c


def _prov(**fields):
    return json.dumps({f: {"source": s, "value": v, "fetched_at": "2026-10-01T00:00:00+00:00"}
                       for f, (s, v) in fields.items()})


def test_hybrid_sticker_never_downgrades_stored_phev(authoritative, monkeypatch):
    _vpic(monkeypatch)
    car = _car(fuel_type="Plug-In Hybrid", spec_source_json=_prov(fuel_type=(prec.NHTSA_VPIC, "Plug-In Hybrid")))
    fields, prov = wss._car_fields_from_parsed_sticker(car, {"fuel_type": "hybrid"})
    assert "fuel_type" not in fields
    assert prov["fuel_type"]["source"] == prec.NHTSA_VPIC  # stored entry survives
    assert prov["fuel_type"]["discrepancy"]["source"] == prec.STICKER
    assert prov["fuel_type"]["discrepancy"]["value"] == "Hybrid"


def test_sticker_transmission_over_feed_is_written(authoritative, monkeypatch):
    _vpic(monkeypatch)
    car = _car(transmission="Automatic", spec_source_json=_prov(transmission=(prec.DEALER_FEED, "Automatic")))
    fields, prov = wss._car_fields_from_parsed_sticker(car, {"transmission": "8-Speed Automatic"})
    assert fields["transmission"] == "8-Speed Automatic"
    assert prov["transmission"]["source"] == prec.STICKER
    assert prov["transmission"]["value"] == "8-Speed Automatic"


def test_sticker_rwd_with_vpic_4x2_is_written(authoritative, monkeypatch):
    _vpic(monkeypatch, drive="4x2")
    car = _car(drivetrain="AWD", spec_source_json=_prov(drivetrain=(prec.DEALER_FEED, "AWD")))
    fields, prov = wss._car_fields_from_parsed_sticker(car, {"drivetrain": "RWD"})
    assert fields["drivetrain"] == "RWD"
    assert prov["drivetrain"]["source"] == prec.STICKER


def test_sticker_rwd_with_vpic_4wd_is_not_written(authoritative, monkeypatch):
    _vpic(monkeypatch, drive="4WD/4-Wheel Drive/4x4")
    car = _car(drivetrain="4WD", spec_source_json=_prov(drivetrain=(prec.NHTSA_VPIC, "4WD")))
    fields, prov = wss._car_fields_from_parsed_sticker(car, {"drivetrain": "RWD"})
    assert "drivetrain" not in fields
    assert prov["drivetrain"]["discrepancy"]["value"] == "RWD"


def test_non_authoritative_oem_is_fill_only(unverified, monkeypatch):
    _vpic(monkeypatch)
    car = _car(transmission="Automatic", drivetrain=None, spec_source_json=_prov(transmission=(prec.DEALER_FEED, "Automatic")))
    fields, prov = wss._car_fields_from_parsed_sticker(
        car, {"transmission": "8-Speed Automatic", "drivetrain": "4WD", "exterior_color": "Bright White"})
    assert "transmission" not in fields           # column already set
    assert fields["drivetrain"] == "4WD" and fields["exterior_color"] == "Bright White"
    assert prov["drivetrain"]["source"] == prec.STICKER_UNVERIFIED
    assert prov["exterior_color"]["source"] == prec.STICKER_UNVERIFIED


def test_trim_heuristic_engine_is_fill_only_and_tagged(authoritative, monkeypatch):
    from backend.scanner.window_sticker import known_oem_engine_from_car

    _vpic(monkeypatch)
    base = dict(model="Wrangler", trim="Rubicon 392", title="2023 Jeep Wrangler Rubicon 392")
    parsed = dict(known_oem_engine_from_car(_car(**base)))
    assert parsed.get("engine_display")
    blank_fields, blank_prov = wss._car_fields_from_parsed_sticker(_car(**base), parsed)
    assert blank_fields["engine_description"] == "6.4L V8"
    assert blank_prov["engine_description"]["source"] == prec.TRIM_HEURISTIC
    assert blank_fields["cylinders"] == 8 and blank_prov["cylinders"]["source"] == prec.TRIM_HEURISTIC

    stored = _car(**base, engine_description="6.4L V8 HEMI", cylinders=8,
                  spec_source_json=_prov(engine_description=(prec.DEALER_FEED, "6.4L V8 HEMI")))
    fields, _ = wss._car_fields_from_parsed_sticker(stored, parsed)
    assert "engine_description" not in fields


def test_text_parsed_engine_outranks_feed(authoritative, monkeypatch):
    _vpic(monkeypatch)
    car = _car(engine_description="3.6L", spec_source_json=_prov(engine_description=(prec.DEALER_FEED, "3.6L")))
    fields, prov = wss._car_fields_from_parsed_sticker(car, {"engine_display": "3.6L V6 Pentastar"})
    assert fields["engine_description"] == "3.6L V6 Pentastar"
    assert prov["engine_description"]["source"] == prec.STICKER


@pytest.mark.pg
def test_cars_pages_entry_point_keeps_phev_and_writes_transmission(pg_db, tmp_path, monkeypatch, authoritative):
    pg_db.add_cars([{
        "dealer_id": "d1", "title": "2022 Jeep Grand Cherokee 4xe", "vin": VIN, "make": "Jeep",
        "model": "Grand Cherokee", "fuel_type": "Plug-In Hybrid", "transmission": "Automatic",
        "spec_source_json": _prov(fuel_type=(prec.NHTSA_VPIC, "Plug-In Hybrid"),
                                  transmission=(prec.DEALER_FEED, "Automatic")),
    }])
    with pg_db.connect() as c:
        cid = c.execute("SELECT max(id) FROM cars").fetchone()[0]
    txt = tmp_path / "sticker.txt"
    txt.write_text("monroney")
    monkeypatch.setattr(wss, "_ROOT", tmp_path)
    monkeypatch.setattr(wss, "window_sticker_local_path", lambda *a, **k: txt)
    monkeypatch.setattr(wss, "car_sticker_packages_need_analysis", lambda c: True)
    monkeypatch.setattr("backend.scanner.window_sticker.parse_sticker_from_text",
                        lambda t: {"fuel_type": "hybrid", "transmission": "8-Speed Automatic",
                                   "sticker_specs": {"Transmission": "8-Speed Automatic"}})
    monkeypatch.setattr("backend.scanner.window_sticker.should_auto_fetch_oem_window_sticker", lambda c: False)
    out = wss.ensure_window_sticker_for_car(cid, allow_vision_fallback=False)
    assert out["analyzed"]
    with pg_db.connect() as c:
        fuel, trans, prov = c.execute(
            "SELECT fuel_type, transmission, spec_source_json FROM cars WHERE id = %s", (cid,)).fetchone()
    prov = json.loads(prov)
    assert fuel == "Plug-In Hybrid" and prov["fuel_type"]["source"] == prec.NHTSA_VPIC
    assert prov["fuel_type"]["discrepancy"]["value"] == "Hybrid"
    assert trans == "8-Speed Automatic" and prov["transmission"]["source"] == prec.STICKER
    assert "window_sticker_pdf" in prov
