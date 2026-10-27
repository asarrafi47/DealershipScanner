"""The scan upsert stops reverting higher-ranked stored facts (P8B.1, D-PR2 (a)).

Pure cases pin the arbitration in ``prepare_row`` (no database); the ``pg`` cases run
the real statement: a heal-written value survives a rescan whose feed disagrees, the
feed's claim lands in ``feed_said``, and stale provenance lets the feed win.
"""
from __future__ import annotations

import json

import pytest

from backend.scanner import database as sdb
from backend.scanner.database import upsert_vehicles
from backend.scanner.upsert.guard import FACT_COLUMNS, arbitrate_feed_facts
from backend.scanner.upsert.rows import normalize_vehicle_row
from backend.scanner.upsert.sql import prepare_row, upsert_params

VIN = "1N4BL4DV0PN300001"


def _prov(**fields) -> str:
    return json.dumps({f: {"source": s, "value": v, "fetched_at": "2026-10-01T00:00:00+00:00"}
                       for f, (s, v) in fields.items()})


def _stored(**cols) -> dict:
    row = dict.fromkeys(FACT_COLUMNS)
    row.update(cols)
    return row


def _feed(**fields) -> dict:
    v = {"vin": VIN, "title": "2023 Nissan Altima SV", "year": 2023, "make": "Nissan", "model": "Altima",
         "trim": "SV", "price": 30000, "dealer_id": "d-one", "dealer_name": "d-one"}
    v.update(fields)
    return normalize_vehicle_row(v)


def _bound(v, prior, stored):
    row = prepare_row(v, prior, stored)
    params = upsert_params(row, now="2026-10-02T00:00:00Z", price_history_json=None,
                           guard_on=False, guard_cutoff_iso="")
    return row, params


def test_vpic_drivetrain_survives_a_rescan_and_feed_said_is_recorded():
    prior = _prov(drivetrain=("nhtsa_vpic", "4WD"))
    row, params = _bound(_feed(drivetrain="RWD"), prior, _stored(drivetrain="4WD"))
    assert row.bound["drivetrain"] is None
    assert "RWD" not in params and row.v["drivetrain"] == "RWD"  # v is not mutated
    entry = json.loads(row.spec_source_json)["drivetrain"]
    assert (entry["source"], entry["value"], entry["feed_said"]) == ("nhtsa_vpic", "4WD", "RWD")
    assert entry["feed_seen_at"]


def test_phev_is_not_reverted_by_a_hybrid_feed_label():
    prior = _prov(fuel_type=("nhtsa_vpic", "Plug-in Hybrid"))
    row, _ = _bound(_feed(fuel_type="Hybrid"), prior, _stored(fuel_type="Plug-in Hybrid"))
    assert row.bound["fuel_type"] is None
    assert json.loads(row.spec_source_json)["fuel_type"]["feed_said"] == "Hybrid"


def test_vpic_cylinders_survive_and_sticker_transmission_survives():
    prior = _prov(cylinders=("nhtsa_vpic", 6), transmission=("oem_window_sticker", "8-Speed Automatic"))
    stored = _stored(cylinders=6, transmission="8-Speed Automatic")
    row, _ = _bound(_feed(cylinders=4, transmission="Automatic"), prior, stored)
    assert row.bound["cylinders"] is None and row.bound["transmission"] is None
    assert row.bound["transmission_type"] is None  # derived from the bound value, not the feed
    prov = json.loads(row.spec_source_json)
    assert prov["cylinders"]["feed_said"] == 4
    assert prov["transmission"]["feed_said"] == "Automatic"


def test_stored_tiebreak_rwd_keeps_against_a_feed_2wd():
    prior = _prov(drivetrain=("epa_catalog_tiebreak", "RWD"))
    row, _ = _bound(_feed(drivetrain="2WD"), prior, _stored(drivetrain="RWD"))
    assert row.bound["drivetrain"] is None


def test_stale_provenance_lets_the_feed_win():
    prior = _prov(drivetrain=("nhtsa_vpic", "4WD"))  # the column has since changed
    row, _ = _bound(_feed(drivetrain="RWD"), prior, _stored(drivetrain="FWD"))
    assert row.bound["drivetrain"] == "RWD"
    assert "feed_said" not in json.loads(row.spec_source_json or "{}").get("drivetrain", {})


def test_no_provenance_or_same_rank_behaves_as_today():
    row, _ = _bound(_feed(drivetrain="RWD"), None, _stored(drivetrain="4WD"))
    assert row.bound["drivetrain"] == "RWD"
    prior = _prov(drivetrain=("dealer_feed", "FWD"))  # the feed refreshing itself
    row, _ = _bound(_feed(drivetrain="RWD"), prior, _stored(drivetrain="FWD"))
    assert row.bound["drivetrain"] == "RWD"


def test_a_stated_better_value_still_overwrites_a_lower_rank():
    prior = _prov(engine_description=("nhtsa_vpic", "3.5L V6"))
    row, _ = _bound(_feed(engine_description="3.5L V6 EcoBoost"), prior,
                    _stored(engine_description="3.5L V6"))
    assert row.bound["engine_description"] == "3.5L V6 EcoBoost"


def test_a_nulled_engine_description_does_not_change_forced_induction():
    prior = _prov(engine_description=("oem_window_sticker", "2.0L Turbocharged"))
    stored = _stored(engine_description="2.0L Turbocharged")
    row, _ = _bound(_feed(engine_description="2.0L"), prior, stored)
    assert row.bound["engine_description"] is None
    # classified from the bound (post-arbitration) values, never the feed's text
    from backend.utils.forced_induction import classify_forced_induction_from_car_row
    assert row.forced_induction == classify_forced_induction_from_car_row(row.bound)


def test_data_quality_score_uses_the_unnulled_values():
    prior = _prov(drivetrain=("nhtsa_vpic", "4WD"), transmission=("oem_window_sticker", "CVT"))
    v = _feed(drivetrain="RWD", transmission="Automatic")
    with_guard, _ = _bound(v, prior, _stored(drivetrain="4WD", transmission="CVT"))
    without, _ = _bound(_feed(drivetrain="RWD", transmission="Automatic"), None, None)
    assert with_guard.data_quality_score == without.data_quality_score


def test_arbitration_needs_no_extra_query():
    v = _feed(drivetrain="RWD")
    assert arbitrate_feed_facts(v, None, _prov(drivetrain=("nhtsa_vpic", "4WD"))) == {}
    assert arbitrate_feed_facts(v, _stored(drivetrain="4WD"), None) == {}


# --- the real statement on Postgres ------------------------------------------
pg = pytest.mark.pg


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


def _car(**fields) -> dict:
    row = {"vin": VIN, "title": "2023 Nissan Altima SV", "year": 2023, "make": "Nissan", "model": "Altima",
           "trim": "SV", "price": 30000, "mileage": 12000, "image_url": "https://img.example.test/a.jpg",
           "transmission": "CVT", "drivetrain": "FWD", "fuel_type": "Gasoline",
           "dealer_id": "d-one", "dealer_name": "d-one", "dealer_url": "https://d-one.example.test"}
    row.update(fields)
    return row


def _read(env, cols: str):
    with env.connect() as conn:
        return conn.execute(f"SELECT {cols} FROM cars WHERE vin = %s", (VIN,)).fetchone()


def _heal(env, drivetrain: str, source: str = "nhtsa_vpic", column: str | None = None):
    with env.connect() as conn:
        conn.execute(
            "UPDATE cars SET drivetrain = %s, spec_source_json = %s WHERE vin = %s",
            (column or drivetrain, _prov(drivetrain=(source, drivetrain)), VIN),
        )
        conn.commit()


@pg
def test_rescan_keeps_the_vpic_drivetrain_and_records_feed_said(upsert_env):
    assert upsert_vehicles([_car()], {}) == 1
    _heal(upsert_env, "4WD")
    assert upsert_vehicles([_car(drivetrain="RWD")], {}) == 1
    drive, prov = _read(upsert_env, "drivetrain, spec_source_json")
    entry = (json.loads(prov) if isinstance(prov, str) else prov)["drivetrain"]
    assert drive == "4WD" and entry["source"] == "nhtsa_vpic" and entry["feed_said"] == "RWD"


@pg
def test_rescan_with_stale_provenance_lets_the_feed_win(upsert_env):
    assert upsert_vehicles([_car()], {}) == 1
    _heal(upsert_env, "4WD", column="FWD")
    assert upsert_vehicles([_car(drivetrain="RWD")], {}) == 1
    assert _read(upsert_env, "drivetrain") == ("RWD",)


@pg
def test_rescan_issues_no_extra_per_row_select(upsert_env, monkeypatch):
    assert upsert_vehicles([_car(), _car(vin="1N4BL4DV0PN300002")], {}) == 2
    seen: list[str] = []
    real = sdb.get_conn

    class Spy:
        def __init__(self, cur):
            self._c = cur

        def execute(self, sql, *a, **k):
            seen.append(" ".join(str(sql).split()))
            return self._c.execute(sql, *a, **k)

        def __getattr__(self, n):
            return getattr(self._c, n)

    class SpyConn:
        def __init__(self, conn):
            self._c = conn

        def cursor(self, *a, **k):
            return Spy(self._c.cursor(*a, **k))

        def __getattr__(self, n):
            return getattr(self._c, n)

    monkeypatch.setattr(sdb, "get_conn", lambda: SpyConn(real()))
    upsert_vehicles([_car(drivetrain="RWD"), _car(vin="1N4BL4DV0PN300002", drivetrain="RWD")], {})
    prefetch = [s for s in seen if s.startswith("SELECT vin, spec_source_json")]
    per_row = [s for s in seen if s.startswith("SELECT") and "WHERE vin = ?" in s]
    assert len(prefetch) == 1 and len(per_row) == 2  # one batch read; the price read is the old one per row
