"""P8A.3: ``apply_fact_proposals`` against the Postgres harness (``pg`` tier).

Only accepted fields are written, provenance carries ``value``, losing signals
are recorded as discrepancies, a dry run writes nothing, and a lower rank never
overwrites a higher one.
"""
from __future__ import annotations

import json

import pytest

from backend.vehicle_facts import precedence as prec
from backend.vehicle_facts.apply import apply_fact_proposals

pytestmark = pytest.mark.pg


def _seed(pg_db, **fields) -> int:
    pg_db.add_cars([{"dealer_id": "d1", "title": "2024 Test Car", **fields}])
    with pg_db.connect() as c:
        return c.execute("SELECT max(id) FROM cars").fetchone()[0]


def _row(pg_db, car_id):
    with pg_db.connect() as c:
        cur = c.execute("SELECT drivetrain, fuel_type, cylinders, spec_source_json FROM cars WHERE id = %s", (car_id,))
        d, f, cy, prov = cur.fetchone()
    return d, f, cy, (json.loads(prov) if prov else {})


def test_only_accepted_fields_written_with_value_provenance(pg_db):
    cid = _seed(pg_db, drivetrain="FWD", fuel_type=None)
    res = apply_fact_proposals(None, cid, [("drivetrain", "AWD"), ("fuel_type", "Gasoline")],
                               prec.NHTSA_VPIC, vpic={"drivetrain": "AWD"})
    d, f, _, prov = _row(pg_db, cid)
    assert res.written and d == "AWD" and f == "Gasoline"  # N15.5: vPIC plain Gasoline fills a blank fuel
    assert prov["drivetrain"]["source"] == prec.NHTSA_VPIC
    assert prov["drivetrain"]["value"] == "AWD"
    assert "fetched_at" in prov["drivetrain"]


def test_lower_rank_never_overwrites_higher_and_records_discrepancy(pg_db):
    cid = _seed(pg_db, drivetrain="AWD",
                spec_source_json=json.dumps({"drivetrain": {"source": prec.NHTSA_VPIC, "value": "AWD"}}))
    res = apply_fact_proposals(None, cid, [("drivetrain", "FWD")], prec.DEALER_FEED)
    d, _, _, prov = _row(pg_db, cid)
    assert d == "AWD" and not res.applied and res.kept
    assert res.discrepancies["drivetrain"]["value"] == "FWD"
    assert prov["drivetrain"]["source"] == prec.NHTSA_VPIC  # stored entry survives
    assert prov["drivetrain"]["discrepancy"]["source"] == prec.DEALER_FEED


def test_dry_run_writes_nothing(pg_db):
    cid = _seed(pg_db, drivetrain=None)
    res = apply_fact_proposals(None, cid, [("drivetrain", "RWD")], prec.DEALER_FEED, dry_run=True)
    d, _, _, prov = _row(pg_db, cid)
    assert res.applied == {"drivetrain": "RWD"} and not res.written
    assert d is None and prov == {}


def test_second_apply_proposes_nothing(pg_db):
    cid = _seed(pg_db, drivetrain=None)
    apply_fact_proposals(None, cid, [("drivetrain", "RWD")], prec.DEALER_FEED)
    again = apply_fact_proposals(None, cid, [("drivetrain", "RWD")], prec.DEALER_FEED)
    assert not again.applied and not again.written
