"""Storage repair, repair scripts and recovery scripts write through fact precedence (P8B.7).

Every write goes out in one ``update_car_row_partial`` call carrying a provenance entry for
each column; a higher-ranked stored value is never replaced by a repair; and the recovery
scripts hold no raw fact ``UPDATE cars``.
"""
from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

from backend.vehicle_facts import precedence as prec
from backend.vehicle_facts.apply import apply_labeled_patch

REPO = Path(__file__).resolve().parents[2]
VIN = "1FTFW1E50NFA00001"


def _prov(**fields):
    return json.dumps({f: {"source": s, "value": v, "fetched_at": "2026-10-01T00:00:00+00:00"}
                       for f, (s, v) in fields.items()})


@pytest.fixture
def writes(monkeypatch):
    """Capture ``update_car_row_partial`` (the one write path ``apply_*`` use)."""
    calls: list[tuple[int, dict]] = []
    from backend.db.repositories import cars_repo

    monkeypatch.setattr(cars_repo, "update_car_row_partial", lambda cid, patch: calls.append((cid, dict(patch))))
    return calls


def _raw(**kw):
    r = {"id": 7, "vin": VIN, "year": 2022, "make": "Ford", "model": "F-150", "trim": "XLT",
         "title": "2022 Ford F-150 XLT 4WD", "condition": "Used", "mileage": 30000,
         "drivetrain": None, "spec_source_json": None}
    r.update(kw)
    return r


# --- apply_labeled_patch (recovery scripts) --------------------------------


def test_labeled_patch_records_a_source_for_every_column(writes):
    res = apply_labeled_patch(_raw(), {"drivetrain": "4WD", "price": 41000}, prec.RECIPE_HEAL, detail="heal_from_recipes")
    assert res.written and len(writes) == 1
    cid, patch = writes[0]
    prov = json.loads(patch["spec_source_json"])
    assert cid == 7 and patch["drivetrain"] == "4WD" and patch["price"] == 41000
    assert prov["drivetrain"]["source"] == "recipe_heal" and prov["price"]["source"] == "recipe_heal"
    assert prov["price"]["detail"] == "heal_from_recipes"


def test_labeled_patch_never_replaces_a_vpic_value(writes):
    raw = _raw(drivetrain="4WD", spec_source_json=_prov(drivetrain=(prec.NHTSA_VPIC, "4WD")))
    res = apply_labeled_patch(raw, {"drivetrain": "RWD"}, prec.RECIPE_HEAL)
    assert "drivetrain" not in res.applied and not writes
    assert res.kept and res.kept[0]["field"] == "drivetrain"


def test_labeled_patch_dry_run_writes_nothing(writes):
    res = apply_labeled_patch(_raw(), {"drivetrain": "4WD"}, prec.HTML_JSONLD, dry_run=True)
    assert res.applied == {"drivetrain": "4WD"} and not res.written and not writes


def test_labeled_patch_meta_columns_carry_no_provenance(writes):
    apply_labeled_patch(_raw(), {"drivetrain": "4WD"}, prec.DEALER_VDP, meta={"recovery_status": "recovered"})
    patch = writes[0][1]
    assert patch["recovery_status"] == "recovered"
    assert "recovery_status" not in json.loads(patch["spec_source_json"])


def test_labeled_patch_per_column_sources(writes):
    apply_labeled_patch(_raw(), {"drivetrain": "4WD", "transmission": "Automatic"}, prec.UNKNOWN,
                        sources={"drivetrain": prec.NHTSA_VPIC, "transmission": prec.DEALER_TEXT})
    prov = json.loads(writes[0][1]["spec_source_json"])
    assert prov["drivetrain"]["source"] == "nhtsa_vpic" and prov["transmission"]["source"] == "dealer_text_heuristic"


# --- storage repair (post-scan) ---------------------------------------------


def test_storage_repair_writes_once_with_provenance(writes):
    from backend.utils.inventory_repair import apply_row_storage_repair

    raw = _raw(condition=None, mileage=None, source_url="https://d.example/used-inventory/")
    res = apply_row_storage_repair(raw)
    assert res.written and len(writes) == 1
    patch = writes[0][1]
    prov = json.loads(patch["spec_source_json"])
    assert patch["condition"] == "Used"
    for col in patch:
        if col in ("spec_source_json", "interior_color_buckets"):
            continue
        assert col in prov and prov[col]["source"] in prec.SOURCES, col


def test_storage_repair_keeps_a_stored_vpic_drivetrain(writes):
    from backend.utils.inventory_repair import apply_row_storage_repair

    raw = _raw(drivetrain="RWD", title="2022 Ford F-150 XLT 4WD",
               spec_source_json=_prov(drivetrain=(prec.NHTSA_VPIC, "RWD")))
    res = apply_row_storage_repair(raw, dry_run=True)
    assert "drivetrain" not in res.applied or res.applied["drivetrain"] == "RWD"


def test_run_storage_repair_for_vins_goes_through_the_helper(monkeypatch, writes):
    from backend.db import inventory_db
    from backend.scanner.post_scan import pipeline

    raw = _raw(condition=None, mileage=None, source_url="https://d.example/used-inventory/")
    monkeypatch.setattr(inventory_db, "get_car_by_vin", lambda v: dict(raw))
    monkeypatch.setattr(inventory_db, "get_car_by_id", lambda *a, **k: None)
    monkeypatch.setattr(inventory_db, "refresh_car_data_quality_score", lambda cid: None)
    monkeypatch.setattr(inventory_db, "update_car_row_partial",
                        lambda *a, **k: pytest.fail("raw update_car_row_partial bypasses precedence"))
    stats = pipeline.run_storage_repair_for_vins([VIN])
    assert stats["rows_patched"] == 1 and stats["fields"].get("condition") == 1
    assert "spec_source_json" in writes[0][1]


# --- repair_inventory_fields -------------------------------------------------


def test_repair_inventory_fields_main_uses_precedence(monkeypatch, writes, capsys):
    from backend.db import inventory_db
    from backend.scripts import repair_inventory_fields as rif

    raw = _raw(condition=None, mileage=None, source_url="https://d.example/used-inventory/")

    class Cur:
        def execute(self, *a, **k): pass
        def fetchall(self): return [(7,)]

    class Conn:
        def cursor(self): return Cur()
        def commit(self): pass
        def close(self): pass

    monkeypatch.setattr(inventory_db, "get_conn", lambda: Conn())
    monkeypatch.setattr(inventory_db, "ensure_cars_table_columns", lambda cur: None)
    monkeypatch.setattr(inventory_db, "get_car_by_id", lambda *a, **k: dict(raw))
    monkeypatch.setattr(inventory_db, "refresh_car_data_quality_score", lambda cid: None)
    monkeypatch.setattr("sys.argv", ["repair_inventory_fields.py"])
    assert rif.main() == 0
    assert len(writes) == 1 and "spec_source_json" in writes[0][1]
    monkeypatch.setattr("sys.argv", ["repair_inventory_fields.py", "--dry-run"])
    writes.clear()
    assert rif.main() == 0 and not writes


# --- backfill_specs ----------------------------------------------------------


def test_backfill_specs_proposes_through_precedence(monkeypatch, writes):
    from backend.scripts import backfill_specs as bs

    car = {"id": 7, "vin": VIN, "year": 2022, "make": "Ford", "model": "F-150", "trim": "XLT",
           "title": "2022 Ford F-150 XLT 4WD", "transmission": None, "drivetrain": None, "cylinders": None}
    stored = _raw(drivetrain="4WD", spec_source_json=_prov(drivetrain=(prec.NHTSA_VPIC, "4WD")))
    monkeypatch.setattr(bs, "get_car_by_id", lambda cid: dict(stored))
    monkeypatch.setattr(bs, "looks_like_decode_vin", lambda v: False)
    monkeypatch.setattr(bs, "decode_trim_logic", lambda **k: {"drivetrain": "RWD", "cylinders": 6})
    monkeypatch.setattr(bs, "lookup_epa_aggregate", lambda **k: None)
    res = bs.backfill_single_car(car, ":memory:")
    assert res["errors"] == []
    patch = writes[0][1]
    assert "drivetrain" not in patch  # vPIC 4WD outranks the catalog's RWD
    prov = json.loads(patch["spec_source_json"])
    assert patch["cylinders"] == 6 and prov["cylinders"]["source"] == "epa_catalog"


def test_backfill_specs_dry_run_does_not_write(monkeypatch, writes):
    from backend.scripts import backfill_specs as bs

    car = {"id": 7, "vin": VIN, "year": 2022, "make": "Ford", "model": "F-150", "trim": "XLT",
           "title": "", "transmission": None, "drivetrain": None, "cylinders": None}
    monkeypatch.setattr(bs, "get_car_by_id", lambda cid: _raw())
    monkeypatch.setattr(bs, "looks_like_decode_vin", lambda v: False)
    monkeypatch.setattr(bs, "decode_trim_logic", lambda **k: {"cylinders": 6})
    monkeypatch.setattr(bs, "lookup_epa_aggregate", lambda **k: None)
    res = bs.backfill_single_car(car, ":memory:", dry_run=True)
    assert res["updates"] == {"cylinders": 6} and not writes


# --- recovery scripts: no raw fact UPDATE ------------------------------------

_RECOVERY = ("heal_from_recipes.py", "harvest_carscommerce.py", "harvest_html_jsonld.py",
             "recover_incomplete_listings.py", "backfill_specs.py", "repair_inventory_fields.py")


@pytest.mark.parametrize("name", _RECOVERY)
def test_recovery_scripts_hold_no_raw_fact_update(name):
    src = (REPO / "backend" / "scripts" / name).read_text()
    assert not re.search(r"UPDATE\s+cars\s+SET", src, re.I), name


def test_post_scan_storage_repair_has_no_direct_patch_write():
    src = (REPO / "backend" / "scanner" / "post_scan" / "pipeline.py").read_text()
    body = src[src.index("def run_storage_repair_for_vins"):src.index("def run_enrichment_for_car_ids")]
    assert "update_car_row_partial" not in body and "collect_row_storage_repairs" not in body
