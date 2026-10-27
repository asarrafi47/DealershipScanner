"""EPA dictionary writer goes through fact precedence (P8B.5, D-PR3 b+d).

The catalog fills blanks; an engine correction needs vPIC or the dealer's own
title / trim to back it; ``--all`` refills only what the catalog may outrank; no
EPA colors, no CSV forced-induction label.
"""
from __future__ import annotations

import json

import pytest

from backend.dictionary import enrich_from_dictionary as efd
from backend.vehicle_facts import precedence as prec

VIN = "WAUGNAFC9HN000001"


def _a6(**kw):
    car = {
        "id": 11, "vin": VIN, "year": 2017, "make": "Audi", "model": "A6", "trim": "Premium Plus",
        "title": "2017 Audi A6 Premium Plus Sedan", "drivetrain": "AWD",
        "engine_l": 3.0, "cylinders": 4, "engine_description": "3.0L",
        "transmission": "8-Speed Automatic", "fuel_type": "Gasoline",
    }
    car.update(kw)
    return car


def _vpic(lit, cyl, drive=None):
    flat = {"DisplacementL": lit, "EngineCylinders": cyl}
    if drive:
        flat["DriveType"] = drive
    return efd.vpic_context_for(flat)


def _prov(**fields):
    return json.dumps({f: {"source": s, "value": v, "fetched_at": "2026-10-01T00:00:00+00:00"}
                       for f, (s, v) in fields.items()})


# --- engine correction: corroboration only -----------------------------------
def test_a6_corrected_when_the_dealer_title_states_four_cylinders():
    car = _a6(title="2017 Audi A6 Premium Plus Sedan TFSI four-cylinder engine")
    up = efd.enrich_car(car, use_vpic=False)
    assert up["engine_l"] == 2.0 and "2.0L I4" in up["engine_description"]
    assert up.ctx.get("corroborated") is True


def test_a6_corrected_when_vpic_decodes_two_liter_four():
    up = efd.enrich_car(_a6(), use_vpic=False, vpic_ctx=_vpic("2.0", "4"))
    assert up["engine_l"] == 2.0 and "2.0L I4" in up["engine_description"]


def test_a6_not_corrected_without_corroboration_and_discrepancy_recorded():
    up = efd.enrich_car(_a6(), use_vpic=False)
    # The picker follows the stored 3.0L to the 3.0 V6 row; none of it is applied.
    assert not {"engine_l", "engine_description", "cylinders"} & set(up)
    assert up.skipped >= 1
    disc = up.provenance["cylinders"]["discrepancy"]
    assert disc["source"] == prec.EPA_CATALOG and disc["value"] == 6
    assert "discrepancy" in up.provenance["engine_description"]


def test_vpic_naming_an_engine_the_catalog_lacks_vetoes_the_title():
    car = _a6(title="2017 Audi A6 Premium Plus Sedan TFSI four-cylinder engine")
    up = efd.enrich_car(car, use_vpic=False, vpic_ctx=_vpic("1.8", "4"))
    assert not {"engine_l", "engine_description", "cylinders"} & set(up)


def test_correction_follows_the_decode_when_the_catalog_has_that_engine():
    up = efd.enrich_car(_a6(), use_vpic=False, vpic_ctx=_vpic("3.0", "6"))
    assert up["cylinders"] == 6 and "3.0L V6" in up["engine_description"]


def test_title_that_contradicts_the_catalog_row_does_not_corroborate():
    row = {"displacement": "2.0", "cylinders": "4"}
    assert efd.engine_correction_corroborated({"title": "2017 Audi A6 3.0L V6"}, row, None) is None
    assert efd.engine_correction_corroborated({"title": "2017 Audi A6 2.0L"}, row, None) == "dealer_title"
    assert efd.engine_correction_corroborated({"title": "2017 Audi A6"}, row, None) is None
    assert efd.engine_correction_corroborated({"title": "x"}, row, {"DisplacementL": "2.0", "EngineCylinders": "4"}) == "vpic"


def test_no_engine_fields_proposed_on_a_battery_electric_vehicle():
    car = _a6(fuel_type="Electric", engine_l=None, cylinders=None, engine_description=None,
              title="2017 Audi A6 e-tron")
    up = efd.enrich_car(car, use_vpic=False)
    assert not {"engine_l", "cylinders", "engine_description"} & set(up)


# --- --all ---------------------------------------------------------------------
def test_fill_all_leaves_vpic_cylinders_alone(monkeypatch):
    rows = [{"Trim": "Base", "cylinders": "4", "displacement": "2.0", "drivetrainOptions": "Rear-Wheel Drive"}]
    monkeypatch.setattr(efd, "_load_epa_csv", lambda *a: rows)
    car = _a6(cylinders=6, engine_l=3.0, engine_description="3.0L V6", drivetrain="AWD",
              spec_source_json=_prov(cylinders=(prec.NHTSA_VPIC, 6), drivetrain=(prec.NHTSA_VPIC, "AWD")))
    up = efd.enrich_car(car, fill_all=True, use_vpic=False)
    assert "cylinders" not in up and "drivetrain" not in up
    assert up.skipped >= 2


def test_fill_all_refills_a_lower_ranked_value_but_not_an_unlabeled_one(monkeypatch):
    rows = [{"Trim": "Base", "drivetrainOptions": "Front-Wheel Drive", "mpg_city": "20", "mpg_highway": "30"}]
    monkeypatch.setattr(efd, "_load_epa_csv", lambda *a: rows)
    labeled = _a6(drivetrain="RWD", spec_source_json=_prov(drivetrain=(prec.MODEL_SPECS, "RWD")))
    assert efd.enrich_car(labeled, fill_all=True, use_vpic=False)["drivetrain"] == "FWD"
    unlabeled = _a6(drivetrain="RWD")
    assert "drivetrain" not in efd.enrich_car(unlabeled, fill_all=True, use_vpic=False)


# --- vPIC 4x2 -------------------------------------------------------------------
def test_blank_drivetrain_with_vpic_4x2_is_never_filled_all_wheel(monkeypatch):
    rows = [{"Trim": "Base", "drivetrainOptions": "All-Wheel Drive", "transmissionOptions": "Automatic"}]
    monkeypatch.setattr(efd, "_load_epa_csv", lambda *a: rows)
    car = _a6(drivetrain=None)
    up = efd.enrich_car(car, use_vpic=False, vpic_ctx=_vpic("2.0", "4", drive="4x2"))
    assert up.get("drivetrain") not in ("AWD", "4WD")
    # Without the decode the same row fills AWD.
    assert efd.enrich_car(car, use_vpic=False)["drivetrain"] == "AWD"


def test_most_common_row_fallback_skips_all_wheel_rows_under_a_two_wheel_decode():
    rows = [
        {"Trim": "A", "drivetrainOptions": "All-Wheel Drive", "transmissionOptions": "Auto"},
        {"Trim": "B", "drivetrainOptions": "All-Wheel Drive", "transmissionOptions": "Auto"},
        {"Trim": "C", "drivetrainOptions": "Rear-Wheel Drive", "transmissionOptions": "Manual"},
    ]
    assert efd._best_row(rows, "Unknown")["Trim"] == "A"
    assert efd._best_row(rows, "Unknown", two_wheel=True)["Trim"] == "C"


# --- colors and forced induction ------------------------------------------------
def test_no_exterior_color_and_no_forced_induction_from_the_catalog(monkeypatch):
    rows = [{"Trim": "Base", "exteriorColors": "Red", "forcedInduction": "Turbocharged",
             "transmissionOptions": "Automatic", "cylinders": "4", "displacement": "2.0"}]
    monkeypatch.setattr(efd, "_load_epa_csv", lambda *a: rows)
    car = _a6(exterior_color=None, forced_induction=None, engine_l=None, cylinders=None, engine_description=None,
              description="Exterior Color: Glacier White Metallic")
    up = efd.enrich_car(car, use_vpic=False)
    assert "exterior_color" not in up
    # The CSV label never fills; at most the resolver's last-resort heuristic does (P10C.4).
    assert up.provenance.get("forced_induction", {}).get("source") != "epa_dictionary"
    assert "exterior_color" not in efd.FILLABLE_FIELDS and "forced_induction" not in efd.FILLABLE_FIELDS
    assert up["cylinders"] == 4  # the rest of the catalog still fills blanks


def test_text_transmission_keeps_its_honest_source(monkeypatch):
    monkeypatch.setattr(efd, "_load_epa_csv", lambda *a: [])
    car = _a6(transmission=None, description="Engine with 8-Speed Automatic transmission")
    up = efd.enrich_car(car, use_vpic=False)
    assert [p for p in up.proposals if p[0] != "forced_induction"] == [
        ("transmission", up["transmission"], prec.DEALER_TEXT)]


# --- persistence ----------------------------------------------------------------
def test_apply_writes_columns_and_provenance_in_one_update(monkeypatch):
    calls = []
    import backend.db.repositories.cars_repo as repo

    monkeypatch.setattr(repo, "update_car_row_partial", lambda cid, f: calls.append((cid, dict(f))))
    car = _a6(title="2017 Audi A6 Premium Plus Sedan TFSI four-cylinder engine")
    up = efd.enrich_car(car, use_vpic=False)
    out = efd.apply_dictionary_updates(car, up)
    assert out["written"]
    fact_call = next(f for _, f in calls if "spec_source_json" in f)
    assert fact_call["engine_l"] == 2.0
    prov = json.loads(fact_call["spec_source_json"])
    assert prec.canonical_source(prov["engine_l"]["source"]) == prec.EPA_CATALOG
    assert any("mpg_city" in f for _, f in calls)  # plain columns follow in their own update


def test_apply_dry_run_writes_nothing(monkeypatch):
    calls = []
    import backend.db.repositories.cars_repo as repo

    monkeypatch.setattr(repo, "update_car_row_partial", lambda cid, f: calls.append(f))
    car = _a6()
    efd.apply_dictionary_updates(car, efd.enrich_car(car, use_vpic=False), dry_run=True)
    assert calls == []


def test_refused_correction_is_recorded_in_stored_provenance(monkeypatch):
    calls = []
    import backend.db.repositories.cars_repo as repo

    monkeypatch.setattr(repo, "update_car_row_partial", lambda cid, f: calls.append(dict(f)))
    car = _a6()
    efd.apply_dictionary_updates(car, efd.enrich_car(car, use_vpic=False))
    prov = json.loads(next(f for f in calls if "spec_source_json" in f)["spec_source_json"])
    assert prov["cylinders"]["discrepancy"]["value"] == 6
    assert next(f for f in calls if "spec_source_json" in f).get("cylinders") is None


@pytest.mark.pg
def test_post_scan_stage_end_to_end(pg_db):
    from backend.scanner.post_scan.pipeline import run_dictionary_enrich_for_vins

    pg_db.add_cars([{
        "dealer_id": "d1", "vin": VIN, "year": 2017, "make": "Audi", "model": "A6", "trim": "Premium Plus",
        "title": "2017 Audi A6 Premium Plus Sedan TFSI four-cylinder engine", "drivetrain": "AWD",
        "engine_l": 3.0, "cylinders": 4, "engine_description": "3.0L", "transmission": "8-Speed Automatic",
    }])
    stats = run_dictionary_enrich_for_vins([VIN])
    assert stats["updated"] == 1
    with pg_db.connect() as c:
        lit, prov = c.execute("SELECT engine_l, spec_source_json FROM cars WHERE vin = %s", (VIN,)).fetchone()
    assert float(lit) == 2.0
    assert prec.canonical_source(json.loads(prov)["engine_l"]["source"]) == prec.EPA_CATALOG
