"""Golden test for ``backend.scanner.database.upsert_vehicles``.

Records what one upsert pipeline run does on a varied, realistic batch and
pins it, so the function can be split into named steps without any behavior
change:

* batch 1: ten real rows read (read-only) from the local Postgres ``cars``
  table on 2026-10-01 -- duplicate VIN, odd non-17-char VIN, "Electric" Ram
  and Wrangler, a Ram "Hybrid", JSON columns given as decoded lists/dicts and
  as raw strings -- plus synthetic edge cases: every gallery / spin / pano /
  highlights / packages / spec_source_json shape, price/mileage/msrp coercion,
  blank VIN, missing VIN, a rooftop reject, lot location, in-transit row.
* batch 2: sparse rescans of real VINs (keep-if-nonempty, price history),
  a guard refusal (fresh foreign owner), a guard pass (stale owner),
  "Unknown vehicle"/year 0/empty make, mileage 0 vs NULL.
* batch 3: the SQL backstop (prefetch predicate disabled) and a price change.

Recorded: return counts, the ``stats`` dict, every ``cars`` /
``vin_owner_conflicts`` / ``incomplete_listings`` row, the log lines of the
``backend.scanner.database`` logger, and the post-write step calls (vPIC
backfill and catalog linker are stubbed -- network; the model_specs
correction and incomplete-listing sync run for real on the tmp SQLite DB).

Timestamps written by the run are replaced by rank tokens (``<TS0>``,
``<TS1>`` ...) so equality between columns is still checked.

Regenerate (only when behavior is meant to change):
``UPSERT_GOLDEN_REGEN=1 .venv/bin/python -m pytest -q backend/tests/test_upsert_vehicles_golden.py``
"""

from __future__ import annotations

import json
import logging
import os
import re
import sqlite3
from decimal import Decimal
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

FIXTURES = Path(__file__).parent / "fixtures"
INPUTS = FIXTURES / "upsert_vehicles_golden_inputs.json"
EXPECTED = FIXTURES / "upsert_vehicles_golden_expected.json"

_TS_RE = re.compile(r"\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(?:\.\d+)?(?:Z|\+00:00)?")


def _rows(sql: str, get_conn=None) -> list[dict]:
    """Dict rows through the product's connection (``inventory_db.get_conn``), so the
    same reader serves the SQLite golden and its Postgres variant (P14A.6)."""
    if get_conn is None:
        from backend.db import inventory_db

        get_conn = inventory_db.get_conn
    conn = get_conn()
    conn.row_factory = sqlite3.Row  # any truthy factory: the wrapper maps it to dict_row on Postgres
    try:
        return [dict(r) for r in conn.execute(sql).fetchall()]
    finally:
        conn.close()


def _exec(sql: str, params: tuple = ()) -> None:
    from backend.db import inventory_db

    conn = inventory_db.get_conn()
    try:
        conn.execute(sql, params)
        conn.commit()
    finally:
        conn.close()


def _parse_ts(text: str) -> datetime:
    dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def _mask_timestamps(obj, started: datetime):
    """Stamps written during the run (at/after ``started``) become tokens:
    microsecond stamps (``scraped_at`` & co.) get rank tokens ``<TSn>`` so
    equality/order between columns is still pinned; second-resolution stamps
    (provenance ``fetched_at``) become ``<NOW_S>``. Older stamps (real data,
    seeded values) are kept literally."""
    text = json.dumps(obj, sort_keys=True, default=str)
    found = set(m.group(0) for m in _TS_RE.finditer(text))
    run_fine = sorted(t for t in found if "." in t and _parse_ts(t) >= started)
    run_coarse = [t for t in found if "." not in t and _parse_ts(t) >= started]
    repl = {t: f"<TS{i}>" for i, t in enumerate(run_fine)}
    repl.update({t: "<NOW_S>" for t in run_coarse})
    return json.loads(_TS_RE.sub(lambda m: repl.get(m.group(0), m.group(0)), text))


def _pg_normalize(obj):
    """Postgres row values in the SQLite golden's shapes (the documented normalization):
    ``Decimal`` -> int when integral else float; ``bool`` -> 0/1; ``datetime`` -> ISO text.
    Timestamp columns are TEXT on both backends, so the run's stamps reach
    ``_mask_timestamps`` as the same rank tokens."""
    if isinstance(obj, dict):
        return {k: _pg_normalize(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_pg_normalize(v) for v in obj]
    if isinstance(obj, Decimal):
        return int(obj) if obj == obj.to_integral_value() else float(obj)
    if isinstance(obj, bool):
        return int(obj)
    if isinstance(obj, datetime):
        return obj.isoformat()
    return obj


def _stub_post_write(monkeypatch):
    """Stub the network post-write steps and record the calls; shared by both backends."""
    from backend.scanner import database as sdb

    monkeypatch.setattr("backend.utils.project_env.load_project_dotenv", lambda *a, **k: None)
    monkeypatch.setattr(sdb, "_vin_owner_table_ready", False)
    monkeypatch.delenv("SCANNER_VIN_OWNER_GUARD_HOURS", raising=False)
    monkeypatch.delenv("SCANNER_IDLE_IN_TXN_TIMEOUT_MS", raising=False)

    calls: list = []

    def fake_backfill(car_id, **kw):
        calls.append(["spec_backfill", _vin_of(car_id), kw])
        return {}

    def fake_link(vins):
        calls.append(["link_cars_by_vins", list(vins)])
        return 0

    real_corrections = sdb.apply_model_specs_corrections

    def recording_corrections(*a, **kw):
        calls.append(["apply_model_specs_corrections", list(kw.get("vins") or [])])
        return real_corrections(*a, **kw)

    monkeypatch.setattr(
        "backend.enrichment.spec_structured_backfill.apply_structured_spec_backfill_for_car",
        fake_backfill,
    )
    monkeypatch.setattr("backend.catalog.linker.link_cars_by_vins", fake_link)
    monkeypatch.setattr(sdb, "apply_model_specs_corrections", recording_corrections)
    return calls


@pytest.fixture()
def golden_env(sqlite_inventory, monkeypatch):
    import backend.db.incomplete_listings_db as ild

    il_path = sqlite_inventory.path.parent / "incomplete_listings.db"
    monkeypatch.setattr(ild, "DB_PATH", str(il_path))
    return _stub_post_write(monkeypatch)


def _vin_of(car_id):
    from backend.db import inventory_db

    conn = inventory_db.get_conn()
    try:
        r = conn.execute("SELECT vin FROM cars WHERE id=?", (car_id,)).fetchone()
        return r[0] if r else None
    finally:
        conn.close()


def _run(calls: list, monkeypatch, caplog, *, pg: bool = False) -> dict:
    from backend.db import incomplete_listings_db as ild
    from backend.scanner import database as sdb
    from backend.scanner.database import upsert_vehicles

    started = datetime.now(timezone.utc).replace(microsecond=0) - timedelta(seconds=1)
    data = json.loads(INPUTS.read_text(encoding="utf-8"))
    monkeypatch.setenv("SCANNER_TRACE_VIN", data["trace_vin"])
    caplog.set_level(logging.DEBUG, logger="backend.scanner.database")

    # model_specs dictionary rows so the post-write correction has work to do.
    if not pg:  # the migration chain already created it on Postgres
        _exec(
            "CREATE TABLE IF NOT EXISTS model_specs (make TEXT NOT NULL, model TEXT NOT NULL, "
            "cylinders INTEGER, gears INTEGER, transmission TEXT, drivetrain TEXT, body_style TEXT, "
            "fuel_type TEXT, PRIMARY KEY (make, model))"
        )
    _exec(
        "INSERT OR REPLACE INTO model_specs (make, model, cylinders, gears, transmission, drivetrain, "
        "body_style, fuel_type) VALUES ('Dodge','Charger',8,8,'8-Speed Automatic','RWD','Sedan','Gasoline')"
    )

    out: dict = {"phases": []}
    for name in ("batch1", "batch2", "batch3"):
        if name == "batch2":
            # Make the stale-owner VIN old enough for the guard to let it move.
            _exec("UPDATE cars SET scraped_at='2020-01-01T00:00:00Z' WHERE vin=?", (data["stale_vin"],))
        if name == "batch3":
            monkeypatch.setattr(sdb, "_vin_owned_elsewhere", lambda *a, **k: False)
        calls.clear()
        caplog.clear()
        stats: dict = {"stale_key": "reset?"}
        count = upsert_vehicles(json.loads(json.dumps(data[name])), stats)
        logs = [
            [r.levelname, r.getMessage()]
            for r in caplog.records
            if r.name == "backend.scanner.database" and r.levelno >= logging.INFO
        ]
        if isinstance(stats.get("timing"), dict):
            stats["timing"] = _timing_shape(stats["timing"])
        out["phases"].append(
            {"name": name, "count": count, "stats": stats, "calls": list(calls), "logs": logs}
        )
    out["cars"] = [
        {k: v for k, v in r.items() if k != "id"}
        for r in _rows("SELECT * FROM cars ORDER BY vin")
    ]
    out["vin_owner_conflicts"] = _rows(
        "SELECT * FROM vin_owner_conflicts ORDER BY vin, owner_dealer_id, claimant_dealer_id"
    )
    out["incomplete_listings"] = [
        {k: v for k, v in r.items() if k not in ("car_id", "id", "updated_at")}
        for r in _rows("SELECT * FROM incomplete_listings ORDER BY vin", ild.get_conn)
    ]
    out["empty_call"] = upsert_vehicles([], {})
    if pg:
        out = _pg_normalize(out)
    return _mask_timestamps(out, started)


def _timing_shape(t: dict) -> dict:
    """P11A.7 ``stats['timing']`` with the clock readings masked: seconds -> "<t>", the
    environment's concurrency -> "<n>"; the counts (rows, selects, cars ...) stay exact."""
    out: dict = {}
    for k, v in t.items():
        if isinstance(v, dict):
            out[k] = _timing_shape(v)
        elif k == "dealer_concurrency":
            out[k] = "<n>"
        elif k.endswith("_s"):
            out[k] = "<t>"
        else:
            out[k] = v
    return out


def test_upsert_vehicles_golden(golden_env, monkeypatch, caplog):
    got = _run(golden_env, monkeypatch, caplog)
    if os.environ.get("UPSERT_GOLDEN_REGEN") == "1":
        EXPECTED.write_text(json.dumps(got, indent=1, sort_keys=True) + "\n", encoding="utf-8")
        pytest.skip("golden regenerated")
    expected = json.loads(EXPECTED.read_text(encoding="utf-8"))
    for key in ("phases", "vin_owner_conflicts", "incomplete_listings", "empty_call"):
        assert got[key] == expected[key], key
    assert [r["vin"] for r in got["cars"]] == [r["vin"] for r in expected["cars"]]
    for g, e in zip(got["cars"], expected["cars"]):
        assert g == e, g["vin"]


@pytest.fixture()
def golden_env_pg(pg_db, monkeypatch):
    import backend.db.incomplete_listings_db as ild

    ild.reset_incomplete_listings_schema_cache()
    return _stub_post_write(monkeypatch)


@pytest.mark.pg
def test_upsert_vehicles_golden_on_postgres(golden_env_pg, monkeypatch, caplog):
    """The same three batches on a migration-built Postgres (P14A.6); the expected
    JSON is read, never written."""
    from backend.db import inventory_pg

    assert inventory_pg.is_inventory_postgres()  # the run below must not fall back to SQLite
    got = _run(golden_env_pg, monkeypatch, caplog, pg=True)
    expected = json.loads(EXPECTED.read_text(encoding="utf-8"))
    for key in ("phases", "vin_owner_conflicts", "incomplete_listings", "empty_call"):
        assert got[key] == expected[key], key
    assert [r["vin"] for r in got["cars"]] == [r["vin"] for r in expected["cars"]]
    for g, e in zip(got["cars"], expected["cars"]):
        assert g == e, g["vin"]
