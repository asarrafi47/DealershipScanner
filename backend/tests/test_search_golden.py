"""Golden pins for ``search_cars`` and ``_build_filter_options_uncached`` (refactor guard).

Recorded 2026-10-01 before the two functions were split into named steps
(``backend/db/repositories/search/`` and ``backend/db/repositories/facets/``).
The split is a pure refactor, so every recorded value must still match:

* 400 generated ``search_cars`` parameter combinations covering every filter
  family (make/model/trim, country, vehicle_or, scalar columns, paint families,
  interior buckets, displacement, year/price/mileage, CPO/condition, ZIP radius,
  registry ids, candidate ids, VIN, equipment needles, trim needles, hidden
  dealers, flagged/incomplete toggles, limit edge cases) plus the six real
  ``user_search_history`` entries, translated by hand.
* For each: result ids in order, count, a sha256 of the full returned dicts,
  ``distance_miles`` values, and the exact SQL text + params of every statement
  the call issued against ``cars`` / ``dealerships`` / ``dealer_geopoints``.
* The full facet dict from ``_build_filter_options_uncached``.

The SQLite side runs against ``fixtures/search_golden_inventory.json.gz``: 984
real rows (30 per dealer from 32 dealers, 392 inactive, plus the 24 active rows
with ``packages_normalized``) READ from local Postgres, with a handful
flagged ``marked_for_review`` here so the flag path has rows.

The Postgres side (``SEARCH_GOLDEN_PG=1``, needs local Postgres) replays the same
combinations read-only (``default_transaction_read_only=on``) against the live
local database and compares with ``fixtures/search_golden_pg.json.gz``; the local
data drifts after a scan, so it is opt-in and is a same-day refactor check.

Regenerate (only when behavior is meant to change):
``SEARCH_GOLDEN_REGEN=1 .venv/bin/python -m pytest backend/tests/test_search_golden.py``
(add ``SEARCH_GOLDEN_PG=1`` for the Postgres file).
"""
from __future__ import annotations

import gzip
import hashlib
import json
import os
import random
import sqlite3
from pathlib import Path
from typing import Any

import pytest

FIXTURES = Path(__file__).parent / "fixtures"
INVENTORY_FIXTURE = FIXTURES / "search_golden_inventory.json.gz"
SQLITE_GOLDEN = FIXTURES / "search_golden_sqlite.json.gz"
PG_GOLDEN = FIXTURES / "search_golden_pg.json.gz"

REGEN = os.environ.get("SEARCH_GOLDEN_REGEN") == "1"
RUN_PG = os.environ.get("SEARCH_GOLDEN_PG") == "1"
PG_URL = "postgresql:///cars?host=/tmp"

# Fixed ZIP centroids so the radius path never depends on pgeocode data.
ZIP_COORDS = {
    "37421": (35.0329, -85.1557),   # Chattanooga
    "92694": (33.5539, -117.6640),  # Ladera Ranch (real search log)
    "28202": (35.2271, -80.8431),   # Charlotte
    "30301": (33.7490, -84.3880),   # Atlanta
    "78701": (30.2672, -97.7431),   # Austin
    "10001": (40.7506, -73.9972),   # New York
}
UNKNOWN_ZIP = "00000"

FLAG_EVERY = 37  # every 37th sampled row is flagged marked_for_review (SQLite only)

_RECORD_TABLES = ("FROM cars", "FROM dealerships", "dealer_geopoints")


# ---------------------------------------------------------------------------
# SQL recording: wrap base_repo.get_conn so every db_conn() (wherever imported)
# goes through a proxy that logs execute() calls.
# ---------------------------------------------------------------------------


class _RecCursor:
    def __init__(self, real: Any, log: list) -> None:
        object.__setattr__(self, "_real", real)
        object.__setattr__(self, "_log", log)

    def execute(self, sql, params=None):
        self._log.append((sql, list(params) if params is not None else None))
        if params is None:
            return self._real.execute(sql)
        return self._real.execute(sql, params)

    def __getattr__(self, name):
        return getattr(self._real, name)

    def __setattr__(self, name, value):
        setattr(self._real, name, value)

    def __iter__(self):
        return iter(self._real)


class _RecConn:
    def __init__(self, real: Any, log: list) -> None:
        object.__setattr__(self, "_real", real)
        object.__setattr__(self, "_log", log)

    def execute(self, sql, params=None):
        self._log.append((sql, list(params) if params is not None else None))
        if params is None:
            return self._real.execute(sql)
        return self._real.execute(sql, params)

    def cursor(self, *a, **k):
        return _RecCursor(self._real.cursor(*a, **k), self._log)

    def __getattr__(self, name):
        return getattr(self._real, name)

    def __setattr__(self, name, value):
        setattr(self._real, name, value)


def _install_recorder(monkeypatch) -> list:
    from backend.db.repositories import base_repo

    log: list = []
    real_get_conn = base_repo.get_conn
    monkeypatch.setattr(base_repo, "get_conn", lambda: _RecConn(real_get_conn(), log))
    return log


def _relevant_sql(log: list) -> list:
    out = []
    for sql, params in log:
        if any(t in sql for t in _RECORD_TABLES):
            out.append([" ".join(sql.split()), _jsonable(params)])
    return out


def _jsonable(v):
    return json.loads(json.dumps(v, default=str))


def _digest(obj) -> str:
    return hashlib.sha256(
        json.dumps(obj, default=str, sort_keys=True).encode("utf-8")
    ).hexdigest()


# ---------------------------------------------------------------------------
# Environment
# ---------------------------------------------------------------------------


def _load_inventory() -> dict:
    with gzip.open(INVENTORY_FIXTURE, "rt") as f:
        return json.load(f)


def _common_env(monkeypatch) -> None:
    monkeypatch.setenv("LISTINGS_INCLUDE_INCOMPLETE_CARS", "1")
    monkeypatch.setattr(
        "backend.db.geo.zip_to_coords", lambda z: ZIP_COORDS.get(str(z).strip())
    )
    from backend.db.repositories import data_quality_repo

    monkeypatch.setattr(data_quality_repo, "_incomplete_index_snapshot_cache", None)


def _seed_sqlite(path: Path, inv: dict) -> None:
    from backend.db import inventory_db

    inventory_db.init_inventory_db()
    conn = sqlite3.connect(str(path))
    try:
        cols = {r[1] for r in conn.execute("PRAGMA table_info(cars)")}
        for i, car in enumerate(inv["cars"]):
            row = {k: v for k, v in car.items() if k in cols}
            if i % FLAG_EVERY == 5:
                row["marked_for_review"] = 1
            keys = sorted(row)
            conn.execute(
                f"INSERT INTO cars ({', '.join(keys)}) VALUES ({', '.join('?' * len(keys))})",
                [row[k] for k in keys],
            )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS dealer_geopoints (dealer_url TEXT PRIMARY KEY, "
            "dealer_name TEXT, lat REAL, lon REAL, zip_code TEXT, city TEXT, state TEXT, "
            "geocode_source TEXT, geocoded_at TEXT)"
        )
        conn.execute("DROP TABLE IF EXISTS dealerships")
        conn.execute(
            "CREATE TABLE dealerships (id INTEGER PRIMARY KEY, name TEXT, website_url TEXT, "
            "dealer_website_url TEXT, duplicate_of_id INTEGER, city TEXT, state TEXT, latitude REAL, "
            "longitude REAL, "
            "zip_code TEXT, is_active INTEGER)"
        )
        for g in inv["dealer_geopoints"]:
            keys = sorted(g)
            conn.execute(
                f"INSERT OR REPLACE INTO dealer_geopoints ({', '.join(keys)}) "
                f"VALUES ({', '.join('?' * len(keys))})",
                [g[k] for k in keys],
            )
        for d in inv["dealerships"]:
            keys = sorted(d)
            conn.execute(
                f"INSERT INTO dealerships ({', '.join(keys)}) VALUES ({', '.join('?' * len(keys))})",
                [d[k] for k in keys],
            )
        conn.commit()
    finally:
        conn.close()


@pytest.fixture
def sqlite_inventory(monkeypatch, tmp_path):
    from backend.db import inventory_db

    db = tmp_path / "inventory.db"
    monkeypatch.setattr("backend.db.inventory_pg.is_inventory_postgres", lambda: False)
    monkeypatch.setattr(inventory_db, "DB_PATH", str(db))
    monkeypatch.setenv("INVENTORY_DB_PATH", str(db))
    inv = _load_inventory()
    _seed_sqlite(db, inv)
    _common_env(monkeypatch)
    return inv


@pytest.fixture
def pg_inventory(monkeypatch):
    if not RUN_PG:
        pytest.skip("SEARCH_GOLDEN_PG=1 not set (needs local Postgres)")
    monkeypatch.setenv("INVENTORY_DATABASE_URL", PG_URL)
    monkeypatch.setenv("PGOPTIONS", "-c default_transaction_read_only=on")
    monkeypatch.delenv("INVENTORY_SQLITE_TESTS", raising=False)
    _common_env(monkeypatch)
    from backend.db.inventory_pg import is_inventory_postgres

    if not is_inventory_postgres():
        pytest.skip("inventory is not on Postgres")
    from backend.db.repositories.base_repo import get_conn

    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("SHOW default_transaction_read_only")
        row = cur.fetchone()
        val = row[0] if not isinstance(row, dict) else next(iter(row.values()))
        assert str(val).lower() == "on", "Postgres golden must run read-only"
    finally:
        conn.close()
    return _load_inventory()


# ---------------------------------------------------------------------------
# Parameter combinations
# ---------------------------------------------------------------------------


def _vals(cars, col, active_only=True):
    out = []
    for c in cars:
        if active_only and c.get("listing_active") == 0:
            continue
        v = c.get(col)
        if v is None or str(v).strip() == "":
            continue
        if v not in out:
            out.append(v)
    return sorted(out, key=lambda x: str(x))


def build_combos(inv: dict) -> list[dict]:
    """400 kwargs dicts. Deterministic for a given inventory fixture."""
    rnd = random.Random(20261001)
    cars = inv["cars"]
    makes = _vals(cars, "make")
    mm = sorted({(c["make"], c["model"]) for c in cars if c.get("make") and c.get("model")})
    trims = _vals(cars, "trim")
    fuels = _vals(cars, "fuel_type")
    cyls = _vals(cars, "cylinders")
    trans = _vals(cars, "transmission")
    drives = _vals(cars, "drivetrain")
    fis = _vals(cars, "forced_induction")
    bodies = _vals(cars, "body_style")
    dealer_ids = _vals(cars, "dealer_id")
    reg_ids = [int(x) for x in _vals(cars, "dealership_registry_id")]
    vins = _vals(cars, "vin")
    ids = sorted(int(c["id"]) for c in cars)
    families = ["red", "blue", "black", "white", "gray", "silver", "green", "brown",
                "tan", "beige", "orange", "yellow", "gold", "purple", "bogus"]
    countries = ["Germany", "Japan", "USA", "South Korea", "UK", "Italy", "Sweden", "Atlantis"]
    pkg_words = ["navigation", "sunroof", "heated", "premium", "tow", "leather",
                 "technology", "convenience", "adaptive cruise", "100%_odd", "carplay"]
    trim_words = ["sport", "limited", "xle", "lt", "premium", "touring", "s", "ex"]

    def pick(seq, k_lo=1, k_hi=3):
        k = min(len(seq), rnd.randint(k_lo, k_hi))
        return rnd.sample(seq, k)

    def mutate_case(s):
        s = str(s)
        return rnd.choice([s, s.upper(), s.lower(), f"  {s} "])

    gens = {
        "makes": lambda: {"makes": [mutate_case(m) for m in pick(makes)]},
        "make_model": lambda: (lambda p: {"makes": [p[0]], "models": [mutate_case(p[1])]})(rnd.choice(mm)),
        "trims": lambda: (lambda c: {"makes": [c["make"]], "trims": [mutate_case(c["trim"])]})(
            rnd.choice([c for c in cars if c.get("trim") and c.get("make")])),
        "countries": lambda: {"countries": pick(countries, 1, 2)},
        "countries_makes": lambda: {"countries": pick(countries, 1, 2), "makes": [mutate_case(m) for m in pick(makes, 1, 3)]},
        "vehicle_or": lambda: {"vehicle_or": [
            rnd.choice([
                {"make": p[0], "models": [p[1]]},
                {"make": p[0], "model": p[1]},
                {"make": p[0], "trim_contains": rnd.choice(trim_words)},
                {"models": p[1]},
                "not-a-dict",
                {},
            ]) for p in rnd.sample(mm, rnd.randint(1, 4))
        ], **({"makes": [rnd.choice(makes)]} if rnd.random() < 0.3 else {})},
        "fuel": lambda: {"fuel_types": pick(fuels)},
        "cylinders": lambda: {"cylinders": [str(c) if rnd.random() < 0.5 else c for c in pick(cyls)]},
        "transmissions": lambda: {"transmissions": pick(trans)},
        "drivetrains": lambda: {"drivetrains": pick(drives)},
        "forced": lambda: {"forced_inductions": pick(fis)} if fis else {"forced_inductions": ["Turbo"]},
        "body": lambda: {"body_styles": [mutate_case(b) for b in pick(bodies)]},
        "ext_colors": lambda: {"exterior_colors": pick(families, 1, 3)},
        "int_colors": lambda: {"interior_colors": pick(families, 1, 3)},
        "int_buckets": lambda: {"interior_color_bucket_filters": pick(families, 1, 3)},
        "int_both": lambda: {"interior_colors": pick(families, 1, 2), "interior_color_bucket_filters": pick(families, 1, 2)},
        "displacement": lambda: rnd.choice([
            {"engine_displacement_l_min": rnd.choice([1.5, 2.0, 2.5, 3.0])},
            {"engine_displacement_l_max": rnd.choice([2.0, 3.5, 5.0])},
            {"engine_displacement_l_min": 2.0, "engine_displacement_l_max": 3.6},
            {"engine_displacement_l_min": "abc"},
            {"engine_displacement_l_min": "2.5", "engine_displacement_l_max": "x"},
        ]),
        "year": lambda: rnd.choice([
            {"min_year": rnd.choice([2015, 2020, 2023, 2025])},
            {"max_year": rnd.choice([2018, 2022, 2026])},
            {"min_year": 2021, "max_year": "2024"},
        ]),
        "price": lambda: {"max_price": rnd.choice([0, 15000, 30000, "45000", 80000.5, "abc", None])},
        "mileage": lambda: {"max_mileage": rnd.choice([0, 10, 5000, "30000.7", 100000, "zz"])},
        "cpo": lambda: {"cpo_only": rnd.choice([True, 1, False])},
        "condition": lambda: {"inventory_condition": rnd.choice(["new", "pre_owned", "cpo", " NEW ", "", "junk"])},
        "zip": lambda: {"zip_code": rnd.choice(list(ZIP_COORDS) + [UNKNOWN_ZIP]),
                        "radius_miles": rnd.choice([10, 25, 50, 100, 250, 2500, "75"])},
        "zip_no_radius": lambda: {"zip_code": rnd.choice(list(ZIP_COORDS)), "radius_miles": rnd.choice([None, 0])},
        "registry": lambda: {"dealership_registry_id": rnd.choice(reg_ids + ["abc", -3, 0, str(reg_ids[0])])},
        "registry_list": lambda: {"dealer_registry_ids": pick(reg_ids, 1, 3) + rnd.choice([[], ["x"], [0], [reg_ids[0]]]),
                                  **({"dealership_registry_id": rnd.choice(reg_ids)} if rnd.random() < 0.4 else {})},
        "candidates": lambda: {"candidate_ids": pick(ids, 1, 40) + rnd.choice([[], ["x"], [-1], [None], ["17"]])},
        "vin": lambda: {"vin": rnd.choice([
            rnd.choice(vins), rnd.choice(vins).lower(), " ".join(rnd.choice(vins)[i:i + 4] for i in range(0, 17, 4)),
            "SHORTVIN", "", rnd.choice(vins) + "XYZW"])},
        "pkg_single": lambda: {"packages_json_contains": rnd.choice(pkg_words)},
        "pkg_list": lambda: {"packages_json_contains_list": pick(pkg_words, 1, 3) + rnd.choice([[], [""], ["  NAVIGATION "]])},
        "pkg_all": lambda: {"packages_json_contains_all": pick(pkg_words, 1, 3)},
        "pkg_mix": lambda: {"packages_json_contains": rnd.choice(pkg_words),
                            "packages_json_contains_list": pick(pkg_words, 1, 2),
                            **({"packages_json_contains_all": pick(pkg_words, 1, 2)} if rnd.random() < 0.5 else {})},
        "trim_contains": lambda: {"trim_contains": rnd.choice(trim_words + ["x" * 150, "   "])},
        "trim_list": lambda: {"trim_contains_list": pick(trim_words, 1, 3) + rnd.choice([[], [" "]]),
                              **({"trim_contains": "ignored"} if rnd.random() < 0.5 else {})},
        "exclude": lambda: {"exclude_dealer_ids": [d.upper() if rnd.random() < 0.3 else d for d in pick(dealer_ids, 1, 4)] + rnd.choice([[], [""], [None]])},
        "incomplete": lambda: {"include_incomplete": rnd.choice([True, False, None])},
        "flagged": lambda: {"include_flagged": rnd.choice([True, False])},
        "limit": lambda: {"limit": rnd.choice([1, 3, 7, 25, 0, -1, "x", "12", 600])},
    }
    names = sorted(gens)
    combos: list[dict] = [{}, {"limit": None}, {"include_flagged": True, "limit": None}]
    # Real user_search_history rows (local Postgres, 2026-10-01), translated.
    combos += [
        {"makes": ["Toyota"], "models": ["RAV4"], "fuel_types": ["Hybrid"], "max_price": 35000},
        {"makes": ["Ford"], "models": ["F-150"], "zip_code": "92694", "radius_miles": 50},
        {"radius_miles": "50"},
        {"radius_miles": "50", "zip_code": "37421"},
    ]
    for name in names:  # each family alone, three draws
        for _ in range(3):
            combos.append(gens[name]())
    while len(combos) < 400:
        k = rnd.choice([2, 2, 3, 3, 4])
        kw: dict = {}
        for name in rnd.sample(names, k):
            kw.update(gens[name]())
        combos.append(kw)
    return combos[:400]


def _pg_safe(kw: dict) -> dict:
    """The fleet is 214k rows on Postgres: never hydrate it unbounded."""
    if "limit" in kw and kw["limit"] is None:
        selective = any(kw.get(k) for k in ("vin", "candidate_ids", "models", "dealership_registry_id",
                                             "dealer_registry_ids", "vehicle_or", "trims"))
        if not selective:
            kw = dict(kw)
            kw["limit"] = 50
    # The read-only connection cannot build the incomplete index, so
    # ``include_incomplete=False`` runs the per-row completeness fallback; paired
    # with a rare Python-side post-filter that hydrates the whole 20k id scan
    # (110 s for one combo). Those combos run with incomplete rows included.
    post = ("exterior_colors", "interior_colors", "interior_color_bucket_filters",
            "engine_displacement_l_min", "engine_displacement_l_max")
    if kw.get("include_incomplete") is False and any(kw.get(k) for k in post):
        kw = dict(kw)
        kw["include_incomplete"] = True
    return kw


# ---------------------------------------------------------------------------
# Recording
# ---------------------------------------------------------------------------


def _stable_sql(kw: dict, log: list) -> list:
    """``countries`` without ``makes`` binds ``list(set_of_makes)``: its order follows
    the per-process string hash seed, so compare those params as a multiset."""
    sql = _relevant_sql(log)
    if kw.get("countries") and not kw.get("makes"):
        sql = [[text, sorted(p, key=lambda v: (type(v).__name__, str(v))) if p else p]
               for text, p in sql]
    return sql


def _record_search(kw: dict, log: list) -> dict:
    from backend.db.repositories.search_repo import search_cars

    del log[:]
    try:
        rows = search_cars(**kw)
    except Exception as exc:  # pragma: no cover - recorded, must stay identical
        return {"error": f"{type(exc).__name__}: {exc}", "sql": _stable_sql(kw, log)}
    return {
        "count": len(rows),
        "ids": [int(r["id"]) for r in rows],
        "distance": [r.get("distance_miles") for r in rows] if any("distance_miles" in r for r in rows) else None,
        "rows_sha256": _digest(rows),
        "sql": _stable_sql(kw, log),
    }


def _record_facets(log: list, *, full: bool) -> dict:
    from backend.db.repositories.listings_repo import _build_filter_options_uncached

    del log[:]
    facets = _build_filter_options_uncached()
    out = {
        "sha256": _digest(facets),
        "sizes": {k: len(v) for k, v in facets.items() if hasattr(v, "__len__")},
        "sql": _relevant_sql(log),
    }
    if full:
        out["facets"] = _jsonable(facets)
    return out


def _compare(golden: dict, actual: dict) -> None:
    assert len(golden["combos"]) == len(actual["combos"])
    mism = []
    for i, (g, a) in enumerate(zip(golden["searches"], actual["searches"])):
        if g != a:
            keys = [k for k in set(g) | set(a) if g.get(k) != a.get(k)]
            mism.append((i, golden["combos"][i], keys))
    assert not mism, f"{len(mism)} search_cars mismatches, first: {mism[:5]}"
    assert golden["facets"] == actual["facets"], "facet golden mismatch"


def _run_all(combos: list[dict], log: list, *, full_facets: bool) -> dict:
    return {
        "combos": _jsonable(combos),
        "searches": [_record_search(kw, log) for kw in combos],
        "facets": _record_facets(log, full=full_facets),
    }


def _write(path: Path, data: dict) -> None:
    with gzip.open(path, "wt") as f:
        json.dump(data, f, sort_keys=True, indent=0, default=str)


def _read(path: Path) -> dict:
    with gzip.open(path, "rt") as f:
        return json.load(f)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_combos_are_400_and_stable(sqlite_inventory):
    combos = build_combos(sqlite_inventory)
    assert len(combos) == 400
    if not REGEN:
        assert _jsonable(combos) == _read(SQLITE_GOLDEN)["combos"]


@pytest.mark.parametrize("chunk", range(4))
def test_search_cars_golden_sqlite(sqlite_inventory, monkeypatch, chunk):
    log = _install_recorder(monkeypatch)
    combos = build_combos(sqlite_inventory)
    lo, hi = chunk * 100, (chunk + 1) * 100
    actual = [_record_search(kw, log) for kw in combos[lo:hi]]
    if REGEN:
        path = FIXTURES / f".search_golden_sqlite_part{chunk}.json"
        path.write_text(json.dumps(actual, default=str))
        return
    golden = _read(SQLITE_GOLDEN)
    mism = [(lo + i, combos[lo + i], [k for k in g if g.get(k) != a.get(k)])
            for i, (g, a) in enumerate(zip(golden["searches"][lo:hi], actual)) if g != a]
    assert not mism, f"{len(mism)} mismatches, first: {mism[:3]}"


def test_filter_options_golden_sqlite(sqlite_inventory, monkeypatch):
    log = _install_recorder(monkeypatch)
    facets = _record_facets(log, full=True)
    combos = build_combos(sqlite_inventory)
    if REGEN:
        parts = []
        for chunk in range(4):
            p = FIXTURES / f".search_golden_sqlite_part{chunk}.json"
            parts.extend(json.loads(p.read_text()))
            p.unlink()
        _write(SQLITE_GOLDEN, {"combos": _jsonable(combos), "searches": parts, "facets": facets})
        return
    golden = _read(SQLITE_GOLDEN)
    assert golden["facets"]["sql"] == facets["sql"]
    assert golden["facets"]["facets"] == facets["facets"]
    assert golden["facets"]["sha256"] == facets["sha256"]


@pytest.mark.parametrize("chunk", range(16))
def test_search_cars_golden_postgres(pg_inventory, monkeypatch, chunk):
    log = _install_recorder(monkeypatch)
    combos = [_pg_safe(kw) for kw in build_combos(pg_inventory)]
    lo, hi = chunk * 25, (chunk + 1) * 25
    actual = [_record_search(kw, log) for kw in combos[lo:hi]]
    if REGEN:
        (FIXTURES / f".search_golden_pg_part{chunk}.json").write_text(json.dumps(actual, default=str))
        return
    golden = _read(PG_GOLDEN)
    mism = [(lo + i, combos[lo + i], [k for k in g if g.get(k) != a.get(k)])
            for i, (g, a) in enumerate(zip(golden["searches"][lo:hi], actual)) if g != a]
    assert not mism, f"{len(mism)} mismatches, first: {mism[:3]}"


def test_filter_options_golden_postgres(pg_inventory, monkeypatch):
    log = _install_recorder(monkeypatch)
    facets = _record_facets(log, full=False)
    combos = [_pg_safe(kw) for kw in build_combos(pg_inventory)]
    if REGEN:
        parts = []
        for chunk in range(16):
            p = FIXTURES / f".search_golden_pg_part{chunk}.json"
            parts.extend(json.loads(p.read_text()))
            p.unlink()
        _write(PG_GOLDEN, {"combos": _jsonable(combos), "searches": parts, "facets": facets})
        return
    golden = _read(PG_GOLDEN)
    if os.environ.get("SEARCH_GOLDEN_PG_FACETS_REGEN") == "1":
        # Facets read the whole live inventory, so any local scan changes them;
        # refresh only this part (searches stay pinned).
        golden["facets"] = facets
        _write(PG_GOLDEN, golden)
        return
    assert golden["facets"] == facets
