"""
Catalog linker safety (P1A.3): a catalog that cannot be read must never clear
or move a link, while a car that the readable catalog cannot resolve still has
its stale link cleared.

Both writers are covered: the incremental ``link_cars_by_vins`` (scanner
post-upsert hook) and the batch ``link_cars_to_catalog.py``.
"""
from __future__ import annotations

import logging
import sqlite3
from dataclasses import dataclass, field

import pytest

from backend.catalog import linker
from backend.catalog.resolver import _CANDIDATE_COLS
from backend.db.inventory_compat import InventoryConnection
from backend.scripts import link_cars_to_catalog as script

_CAR_COLS = (
    "id", "vin", "dealer_id", "listing_active", "year", "make", "model", "trim",
    "cylinders", "engine_l", "engine_description", "drivetrain", "fuel_type", "title",
    "epa_master_id", "epa_match_confidence", "epa_match_method",
)

TUNDRA_VIN = "5TFLA5DB0TX000001"
F150_VIN = "1FTFW1E80TFA00002"
SILVERADO_EV_VIN = "1GC40ZEL0TZ000003"


def _car(id_, vin, year, make, model, trim, cyl, engine, drive, fuel, link, conf=0.9, method="old"):
    return (id_, vin, "d1", 1, year, make, model, trim, cyl, None, engine, drive, fuel,
            f"{year} {make} {model} {trim}", link, conf, method)


def _epa(id_, year, make, model, trim, cyl, disp, drive, fuel, atv=None):
    return (id_, year, make, model, trim, cyl, disp, None, drive, fuel, None, None, atv)


@dataclass
class _Db:
    raw: sqlite3.Connection
    statements: list[str] = field(default_factory=list)


@pytest.fixture
def db(monkeypatch):
    raw = sqlite3.connect(":memory:")
    raw.execute(f"CREATE TABLE cars ({', '.join(_CAR_COLS)})")
    # Plain "id INTEGER" (not the rowid alias): heap order differs from id order.
    raw.execute(f"CREATE TABLE epa_master ({', '.join(_CANDIDATE_COLS)})")
    raw.executemany(
        f"INSERT INTO epa_master VALUES ({', '.join('?' * len(_CANDIDATE_COLS))})",
        [
            # Identical twins, the higher id stored first.
            _epa(70001, 2026, "Toyota", "Tundra", "4WD", 6, 3.4, "Four-Wheel Drive", "Regular Gasoline"),
            _epa(19946, 2026, "Toyota", "Tundra", "4WD", 6, 3.4, "Four-Wheel Drive", "Regular Gasoline"),
            _epa(19296, 2026, "Ford", "F150", "Pickup 2WD", 6, 2.7, "Rear-Wheel Drive", "Regular Gasoline"),
            _epa(31000, 2026, "Chevrolet", "Silverado", "4WD", 8, 6.2, "Four-Wheel Drive", "Regular Gasoline"),
        ],
    )
    raw.executemany(
        f"INSERT INTO cars VALUES ({', '.join('?' * len(_CAR_COLS))})",
        [
            # Stored link is stale (70001): a real resolve moves it to 19946.
            _car(1, TUNDRA_VIN, 2026, "Toyota", "TUNDRA", "SR5", 6, "3.4L V6", "4WD", "Gasoline", 70001),
            _car(2, F150_VIN, 2026, "Ford", "F150", "STX", 6, "2.7L V6 EcoBoost", "RWD", "Gasoline", 19296),
            # Only gas Silverado rows exist: the EV can never resolve.
            _car(3, SILVERADO_EV_VIN, 2026, "Chevrolet", "Silverado EV", "RST", 0,
                 "Dual Electric Motors", "4WD", "Electric", 31000, 0.5, "stale"),
        ],
    )
    raw.commit()
    out = _Db(raw)
    raw.set_trace_callback(out.statements.append)

    monkeypatch.setattr("backend.enrichment.knowledge_engine.prime_vpic_cache", lambda vins: None)
    monkeypatch.setattr("backend.enrichment.knowledge_engine.lookup_vpic_from_cache", lambda vin: {})
    yield out
    raw.close()


class _FlakyCursor:
    """Wraps the real cursor; the catalog read for one make fails like a live
    connection can (lock, I/O error, dropped connection)."""

    def __init__(self, inner, fail_make: str) -> None:
        self._inner, self._fail_make = inner, fail_make

    def execute(self, sql, params=()):
        if "FROM epa_master" in sql and str(params[1]).lower() == self._fail_make:
            raise sqlite3.OperationalError("database is locked")
        self._inner.execute(sql, params)
        return self

    def __getattr__(self, name):
        return getattr(self._inner, name)


class _FlakyConn(InventoryConnection):
    def __init__(self, raw, fail_make: str | None) -> None:
        # shared=True: close() only rolls back, so the in-memory DB survives.
        super().__init__(raw, backend="sqlite", shared=True)
        self._fail_make = fail_make

    def cursor(self, row_factory=None):
        cur = super().cursor(row_factory)
        return _FlakyCursor(cur, self._fail_make) if self._fail_make else cur


def _links(db: _Db) -> dict[int, tuple]:
    return {
        r[0]: tuple(r[1:])
        for r in db.raw.execute(
            "SELECT id, epa_master_id, epa_match_confidence, epa_match_method FROM cars ORDER BY id"
        ).fetchall()
    }


def _updates(db: _Db) -> list[str]:
    return [s for s in db.statements if s.lstrip().upper().startswith("UPDATE")]


def test_catalog_error_never_clears_links(db, monkeypatch, caplog) -> None:
    before = _links(db)
    db.statements.clear()
    monkeypatch.setattr(linker, "get_conn", lambda: _FlakyConn(db.raw, fail_make="ford"))
    with caplog.at_level(logging.WARNING, logger=linker.__name__):
        n = linker.link_cars_by_vins([TUNDRA_VIN, F150_VIN, SILVERADO_EV_VIN])
    assert n == 0
    # The Toyota resolved (and would move 70001 -> 19946) before the Ford read
    # failed: the batch is abandoned whole, nothing was even attempted.
    assert _updates(db) == []
    assert _links(db) == before
    assert any("catalog unavailable" in r.getMessage() for r in caplog.records)


def test_unresolvable_car_link_cleared(db, monkeypatch) -> None:
    monkeypatch.setattr(linker, "get_conn", lambda: _FlakyConn(db.raw, fail_make=None))
    n = linker.link_cars_by_vins([TUNDRA_VIN, F150_VIN, SILVERADO_EV_VIN])
    links = _links(db)
    # The EV has gas-only candidates: catalog readable, no match -> cleared.
    assert links[3] == (None, None, None)
    # Twins: the lowest id wins whatever the heap order.
    assert links[1][0] == 19946
    assert links[2][0] == 19296
    assert n == 2  # Tundra moved + Silverado EV cleared; the F150 kept its id


def test_link_fleet_catalog_error_exits_nonzero_before_update(db, monkeypatch, capsys) -> None:
    before = _links(db)
    monkeypatch.setattr(script, "get_conn", lambda: _FlakyConn(db.raw, fail_make="ford"))
    monkeypatch.setattr(script, "ensure_link_columns", lambda: None)
    monkeypatch.setattr(script, "seed_model_generations", lambda: 0)
    for argv in (["link_cars_to_catalog.py"], ["link_cars_to_catalog.py", "--dry-run"]):
        db.statements.clear()
        monkeypatch.setattr("sys.argv", argv)
        with pytest.raises(SystemExit) as ei:
            script.main()
        assert ei.value.code == 2, argv
        assert _updates(db) == [], argv
        assert _links(db) == before, argv
        assert "catalog unavailable" in capsys.readouterr().err


def test_link_fleet_writes_when_catalog_readable(db, monkeypatch) -> None:
    monkeypatch.setattr(script, "get_conn", lambda: _FlakyConn(db.raw, fail_make=None))
    out = script.link_fleet(dry_run=False, only_missing=False)
    assert out["stats"]["linked"] == 2 and out["stats"]["unlinked"] == 1
    links = _links(db)
    assert links[1][0] == 19946
    # The batch linker never clears: the unresolvable EV keeps its old link.
    assert links[3][0] == 31000
