"""The upsert never lets the classifier overwrite an evidence-backed label (P10C.7)."""
from __future__ import annotations

import json

import pytest

from backend.scanner import database as sdb
from backend.scanner.database import upsert_vehicles
from backend.scanner.upsert.guard import FACT_COLUMNS
from backend.scanner.upsert.rows import normalize_vehicle_row
from backend.scanner.upsert.sql import prepare_row

VIN = "1FTFW1E50NFA00001"
NA = "Naturally Aspirated"


def _prov(source, value):
    return json.dumps({"forced_induction": {"source": source, "value": value,
                                            "fetched_at": "2026-10-01T00:00:00+00:00"}})


def _stored(fi):
    row = dict.fromkeys(FACT_COLUMNS)
    row["forced_induction"] = fi
    return row


def _feed(**f):
    v = {"vin": VIN, "title": "2022 Ford F-150 XLT", "year": 2022, "make": "Ford", "model": "F-150",
         "trim": "XLT", "price": 40000, "dealer_id": "d-one", "dealer_name": "d-one"}
    v.update(f)
    return normalize_vehicle_row(v)


def test_catalog_backed_na_survives_a_feed_with_no_induction_text():
    # description markers rank below the catalog, so the stored NA stays
    row = prepare_row(_feed(description="Twin turbocharged V6"), _prov("epa_catalog", NA), _stored(NA))
    assert row.forced_induction is None  # COALESCE keeps the stored NA


def test_feed_text_turbo_wins_over_catalog():
    row = prepare_row(_feed(engine_description="2.7L V6 Turbocharged"), _prov("epa_catalog", NA), _stored(NA))
    assert row.forced_induction == "Turbocharged"


def test_label_without_provenance_is_replaced_as_before():
    row = prepare_row(_feed(description="Twin turbocharged V6"), None, _stored(NA))
    assert row.forced_induction is not None
    row = prepare_row(_feed(description="Twin turbocharged V6"), _prov("epa_catalog", "Turbocharged"), _stored(NA))  # stale entry
    assert row.forced_induction is not None


def test_heuristic_backed_stored_label_is_not_protected():
    row = prepare_row(_feed(description="Twin turbocharged V6"), _prov("trim_heuristic", NA), _stored(NA))
    assert row.forced_induction is not None


# --- the real statement on Postgres ------------------------------------------
@pytest.fixture()
def upsert_env(pg_db, monkeypatch):
    monkeypatch.setattr("backend.utils.project_env.load_project_dotenv", lambda *a, **k: None)
    monkeypatch.setattr(sdb, "_vin_owner_table_ready", False)
    monkeypatch.delenv("SCANNER_VIN_OWNER_GUARD_HOURS", raising=False)
    monkeypatch.setattr(
        "backend.enrichment.spec_structured_backfill.apply_structured_spec_backfill_for_car",
        lambda *a, **k: {},
    )
    monkeypatch.setattr("backend.catalog.linker.link_cars_by_vins", lambda vins: 0)
    return pg_db


def _car(**f):
    row = {"vin": VIN, "title": "2022 Ford F-150 XLT", "year": 2022, "make": "Ford", "model": "F-150",
           "trim": "XLT", "price": 40000, "mileage": 12000, "image_url": "https://img.example.test/a.jpg",
           "dealer_id": "d-one", "dealer_name": "d-one", "dealer_url": "https://d-one.example.test"}
    row.update(f)
    return row


@pytest.mark.pg
def test_cycle_repair_survives_rescan_and_feed_text_wins(upsert_env):
    assert upsert_vehicles([_car()], {}) == 1
    with upsert_env.connect() as conn:
        conn.execute("UPDATE cars SET forced_induction=%s, spec_source_json=%s WHERE vin=%s",
                     (NA, _prov("epa_catalog", NA), VIN))
        conn.commit()
    assert upsert_vehicles([_car(description="Twin turbocharged V6")], {}) == 1
    with upsert_env.connect() as conn:
        assert conn.execute("SELECT forced_induction FROM cars WHERE vin=%s", (VIN,)).fetchone() == (NA,)
    assert upsert_vehicles([_car(engine_description="2.7L V6 Turbocharged")], {}) == 1
    with upsert_env.connect() as conn:
        assert conn.execute("SELECT forced_induction FROM cars WHERE vin=%s", (VIN,)).fetchone() == ("Turbocharged",)
