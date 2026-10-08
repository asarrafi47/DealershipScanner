"""Golden pin for :func:`backend.enrichment.knowledge_engine_specs.merge_verified_specs`.

Written BEFORE the function was split into ``backend.enrichment.verified_specs``
(monolith audit 2026-10-01, phase 5) so the split could be proven a pure
refactor: the full 35-key dict is recorded per car, for both values of
``include_extended_specs``, and must not move.

Inputs (``fixtures/merge_verified_specs_golden.json.gz``):

* ``live_cars`` — 2,000 active cars (``listing_removed_at IS NULL``, ordered by
  ``md5(id)``) READ from local Postgres under ``default_transaction_read_only``,
  with the heavy text columns dropped (gallery, description, … — measured to
  change no output, alone or together) plus the vPIC decode each VIN resolved
  to at capture time.
* ``FIXTURE_CARS`` below — hand-built rows that hit the branches the sample may
  not (BEV labels, fuel cell, Wrangler, Charger Daytona, ``--`` placeholders,
  linked catalog rows, BMW fuel_type in the decode title, …), each with an
  optional stubbed vPIC decode.

Expectations:

* ``hermetic`` — computed under the suite's normal SQLite-tests environment
  with vPIC replayed from the fixture, and with the dictionary catalog DB built
  from the tracked dictionary tree into ``tmp_path`` (see
  :func:`_use_catalog_built_from_tree`). Runs in any checkout, including a
  clean one (CI) that has no gitignored ``backend/dictionary/index``.
* ``live`` — computed against local Postgres (read-only). Opt in with
  ``MERGE_SPECS_GOLDEN_LIVE_DB=1``.

Re-record (only when a behavior change is INTENDED):
``MERGE_SPECS_GOLDEN_RECORD=1`` on the same test runs. Capture fresh inputs with
``PYTHONPATH=. .venv/bin/python -m backend.tests.test_merge_verified_specs_golden capture``.
"""

from __future__ import annotations

import gzip
import json
import math
import os
import sys
from pathlib import Path
from typing import Any

import pytest

FIXTURE = Path(__file__).parent / "fixtures" / "merge_verified_specs_golden.json.gz"
LIVE = os.environ.get("MERGE_SPECS_GOLDEN_LIVE_DB") == "1"
RECORD = os.environ.get("MERGE_SPECS_GOLDEN_RECORD") == "1"
LIVE_DB_URL = os.environ.get("MERGE_SPECS_GOLDEN_DB_URL") or "postgresql:///cars?host=/tmp"
READ_ONLY_PGOPTIONS = "-c default_transaction_read_only=on"

#: Columns dropped from captured rows (measured: no output changes without them).
HEAVY_COLUMNS = (
    "gallery", "description", "spin_frames", "interior_pano", "history_highlights",
    "internal_notes", "image_url", "carfax_url", "dealer_url", "source_url",
    "price_provenance_json", "spec_source_json", "recovery_notes", "interior_color_buckets",
)

#: The returned dict's keys, IN ORDER (the order is observable to json.dumps callers).
RETURN_KEYS: tuple[str, ...] = (
    "cylinders",
    "cylinders_display",
    "cylinders_verified",
    "catalog_link_rejected",
    "epa_fuzzy_rejected",
    "drivetrain",
    "vpic_electrification",
    "drivetrain_display",
    "drivetrain_verified",
    "gears",
    "transmission_display",
    "fuel_type_hint",
    "sources",
    "dealer_cylinders",
    "master_engine_string",
    "fuel_economy_display",
    "epa_displacement",
    "vpic_engine_l",
    "body_style_display",
    "epa_fuel_type",
    "epa_engine_description",
    "epa_city08",
    "epa_highway08",
    "horsepower",
    "torque_lb_ft",
    "zero_to_60_sec",
    "fuel_tank_gal",
    "ev_range_miles",
    "curb_weight_lb",
    "battery_kwh",
    "tow_capacity_lb",
    "epa_master_id",
    "generation_code",
    "generation_years",
    "generation_notes",
)

_STICKER_EV = json.dumps({"window_sticker": {"fuel_type": "Electric", "mpg_city": 104, "mpg_highway": 89}})

#: (car, vpic-or-None). vpic None means "no decode" (``{}``).
FIXTURE_CARS: list[tuple[dict[str, Any], dict[str, Any] | None]] = [
    ({"make": "Ford", "model": "F-150", "year": 2018, "trim": "XLT", "title": "Used 2018 Ford F-150 XLT",
      "transmission": None, "drivetrain": "4WD", "cylinders": 6, "fuel_type": "Gasoline", "body_style": None}, None),
    ({"make": "Ford", "model": "F-150", "year": 2020, "trim": "Lariat",
      "title": "2020 Ford F-150 Lariat Electronic Ten-Speed Automatic", "transmission": "Automatic",
      "drivetrain": "4x4", "cylinders": None, "fuel_type": "Gasoline"}, None),
    ({"make": "Ford", "model": "F-150", "year": 2005, "trim": "XL", "title": "2005 Ford F-150 XL",
      "transmission": None, "drivetrain": None, "cylinders": None, "fuel_type": "Gasoline"}, None),
    ({"make": "Toyota", "model": "Camry", "year": 2020, "trim": "LE", "title": "Used 2020 Toyota Camry LE",
      "drivetrain": "--", "transmission": "--", "cylinders": None, "fuel_type": None, "body_style": None}, None),
    ({"make": "BMW", "model": "i4", "year": 2023, "trim": "eDrive40", "title": "2023 BMW i4 eDrive40",
      "fuel_type": "Electric", "cylinders": 0, "drivetrain": "RWD", "transmission": "Single-speed automatic",
      "mpg_city": None, "mpg_highway": None}, None),
    ({"make": "BMW", "model": "X2", "year": 2020, "trim": "xDrive28i", "title": "2020 BMW X2 xDrive28i",
      "fuel_type": "Gasoline", "cylinders": 4, "drivetrain": "AWD", "transmission": "8-Speed Automatic",
      "mpg_city": None, "mpg_highway": None}, None),
    ({"make": "BMW", "model": "X5", "year": 2021, "trim": "M", "title": "2021 BMW X5 M",
      "fuel_type": "Gasoline", "cylinders": 6, "drivetrain": "AWD", "transmission": "Automatic",
      "engine_description": "4.4L V8 Twin Turbo", "vin": "5YMJU0C09M9E00001"},
     {"cylinders": 8, "engine_l": 4.4, "drivetrain": "AWD", "electrification": None}),
    ({"make": "BMW", "model": "X5", "year": 2022, "trim": "xDrive45e", "title": "2022 BMW X5 xDrive45e",
      "fuel_type": "Plug-In Hybrid", "cylinders": 6, "drivetrain": "AWD", "transmission": None,
      "vin": "5UXTA6C09N9K00002"},
     {"cylinders": 6, "engine_l": 3.0, "drivetrain": "AWD", "electrification": "phev", "fuel_type": "Gasoline"}),
    ({"make": "Chevrolet", "model": "Silverado 1500", "year": 2026, "trim": "RST",
      "title": "2026 Chevrolet Silverado 1500 RST", "cylinders": 8, "engine_l": None,
      "engine_description": "2.7L I4 L3B Turbo", "drivetrain": "4WD", "transmission": "Automatic",
      "fuel_type": "Gasoline", "body_style": "Truck", "vin": "1GCUDEED0TZ000003"},
     {"cylinders": 4, "engine_l": 2.7}),
    ({"make": "Chevrolet", "model": "Silverado 1500", "year": 2026, "trim": "RST",
      "title": "2026 Chevrolet Silverado 1500 RST", "cylinders": 4,
      "engine_description": "2.7L I4 L3B Turbo", "drivetrain": "4WD", "transmission": "Automatic",
      "fuel_type": "Gasoline", "body_style": "Truck", "vin": "1GCUDEED0TZ000004"},
     {"cylinders": 8, "drivetrain": "RWD"}),
    ({"make": "Chevrolet", "model": "Silverado 1500", "year": 2026, "trim": "RST",
      "title": "2026 Chevrolet Silverado 1500 RST", "cylinders": None, "epa_master_id": 77,
      "engine_description": "2.7L I4 L3B Turbo", "drivetrain": "4WD", "transmission": "Automatic",
      "fuel_type": "Gasoline", "body_style": "Truck"}, None),
    ({"make": "Jeep", "model": "Wrangler", "trim": "Rubicon", "title": "2024 Jeep Wrangler Rubicon",
      "body_style": "Convertible", "vin": ""}, None),
    ({"make": "Jeep", "model": "Grand Cherokee", "year": 2022, "trim": "Limited 4xe",
      "title": "2022 Jeep Grand Cherokee Limited 4xe", "fuel_type": "Electric", "drivetrain": "4WD",
      "vin": "1C4RJYB60N8000005"},
     {"electrification": "phev", "cylinders": 4, "drivetrain": "4WD", "engine_l": 2.0}),
    ({"make": "Mercedes-Benz", "model": "SLK", "year": 2001, "trim": "Kompressor", "fuel_type": "Gasoline"}, None),
    ({"make": "Mercedes-Benz", "model": "E-Class", "year": 2008, "trim": "E 350", "title": "2008 Mercedes-Benz E 350",
      "fuel_type": "Gasoline", "drivetrain": "RWD", "transmission": "7-Speed Automatic"}, None),
    ({"make": "Toyota", "model": "Mirai", "year": 2021, "trim": "XLE", "title": "2021 Toyota Mirai XLE",
      "fuel_type": "Hydrogen", "cylinders": None, "drivetrain": "RWD", "transmission": None}, None),
    ({"make": "Lexus", "model": "GX", "year": 2024, "trim": "550 Premium", "title": "2024 Lexus GX 550 Premium",
      "fuel_type": "Electric", "cylinders": 6, "engine_description": "3.4L V6 Twin Turbo",
      "drivetrain": "4WD", "transmission": "10-Speed Automatic"}, None),
    ({"make": "Tesla", "model": "Model 3", "year": 2022, "trim": "Long Range", "title": "2022 Tesla Model 3 Long Range",
      "fuel_type": "Electric", "cylinders": None, "drivetrain": "AWD", "transmission": None,
      "vin": "5YJ3E1EB0NF000006"},
     {"electrification": "ev", "drivetrain": "AWD", "body_style": "Sedan/Saloon"}),
    ({"make": "Dodge", "model": "Charger", "year": 2024, "trim": "Daytona Scat Pack",
      "title": "2024 Dodge Charger Daytona Scat Pack", "fuel_type": None, "cylinders": None,
      "transmission": None, "packages": _STICKER_EV}, None),
    ({"make": "Hyundai", "model": "Ioniq 5", "year": 2023, "trim": "SEL", "title": "2023 Hyundai Ioniq 5 SEL",
      "fuel_type": None, "transmission": "N/A", "packages": _STICKER_EV}, None),
    ({"make": "Infiniti", "model": "Q70L", "year": 2015, "trim": "Sedan V-6 cyl", "title": "2015 INFINITI Q70L",
      "fuel_type": "Gasoline", "cylinders": None}, None),
    ({"make": "BMW", "model": "i8", "year": 2016, "trim": "", "title": "2016 BMW i8",
      "fuel_type": "Premium Unleaded", "cylinders": None}, None),
    ({"make": "Hummer", "model": "H2", "year": 2006, "trim": "Base", "title": "2006 Hummer H2",
      "fuel_type": "Gasoline", "cylinders": None}, None),
    ({"make": "Ford", "model": "Transit", "year": 2019, "trim": "250 Base", "title": "2019 Ford Transit 250",
      "transmission": None, "fuel_type": "Gasoline", "body_style": None}, None),
    ({"make": "Ford", "model": "Transit", "year": 2021, "trim": "250 Base", "title": "2021 Ford Transit 250",
      "transmission": None, "fuel_type": "Gasoline", "body_style": None}, None),
    ({"make": "Honda", "model": "Civic", "year": 2019, "trim": "EX", "title": "2019 Honda Civic EX",
      "transmission": "CVT", "fuel_type": "Gasoline", "body_style": "N/A", "mpg_city": 32, "mpg_highway": 42,
      "vin": "2HGFC2F70KH000007"},
     {"body_style": "Sedan/Saloon", "transmission": "Continuously Variable Transmission (CVT)",
      "drivetrain": "FWD", "cylinders": 4, "engine_l": 2.0}),
    ({"make": "Subaru", "model": "Outback", "year": 2020, "trim": "Premium", "title": "2020 Subaru Outback Premium",
      "drivetrain": "AWD", "transmission": "Automatic", "fuel_type": "Gasoline", "cylinders": "4",
      "vin": "4S4BTACC0L3000008"},
     {"drivetrain": "AWD", "cylinders": 4, "electrification": None}),
    ({"make": "Audi", "model": "Q5", "year": 2021, "trim": "Premium Plus 45 TFSI quattro",
      "title": "2021 Audi Q5 Premium Plus 45 TFSI quattro", "drivetrain": "FWD", "cylinders": 0,
      "fuel_type": "Gasoline", "vin": "WA1BAAFY0M2000009"},
     {"drivetrain": "AWD/All-Wheel Drive", "cylinders": 4, "engine_l": 2.0}),
    ({"make": "Mercedes-Benz", "model": "GLE", "year": 2021, "trim": "GLE 350 4MATIC",
      "title": "2021 Mercedes-Benz GLE 350 4MATIC SUV", "drivetrain": "", "transmission": "", "fuel_type": "",
      "cylinders": "", "body_style": ""}, None),
    ({"make": "Ram", "model": "1500", "year": 2023, "trim": "Big Horn", "title": "2023 Ram 1500 Big Horn",
      "drivetrain": "4x2", "fuel_type": "Gasoline", "vin": "1C6RREFT0PN000010"},
     {"drivetrain": "4x2", "cylinders": 8, "engine_l": 5.7}),
    ({"make": "Porsche", "model": "Taycan", "year": 2022, "trim": "4S", "title": "2022 Porsche Taycan 4S",
      "fuel_type": "Electricity", "drivetrain": "AWD", "transmission": "2-Speed Automatic"}, None),
    ({"make": "Toyota", "model": "RAV4", "year": 2023, "trim": "XSE Hybrid", "title": "2023 Toyota RAV4 Hybrid XSE",
      "fuel_type": "Hybrid", "drivetrain": "AWD", "transmission": "CVT", "vin": "JTMEWRFV0PD000011"},
     {"electrification": "hybrid", "cylinders": 4, "drivetrain": "AWD"}),
    ({}, None),
    ({"make": None, "model": None, "year": "not-a-year", "trim": None, "title": None, "cylinders": "abc"}, None),
]


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _normalize(obj: Any) -> Any:
    if isinstance(obj, float):
        if math.isnan(obj):
            return "NaN"
        return round(obj, 6)
    if isinstance(obj, dict):
        return {str(k): _normalize(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_normalize(v) for v in obj]
    if obj is None or isinstance(obj, (bool, int, str)):
        return obj
    return str(obj)


def _clear_caches() -> None:
    """Drop every in-process memo under ``backend.*`` so earlier tests cannot leak in."""
    import functools

    for name, mod in list(sys.modules.items()):
        if not name.startswith("backend.") or mod is None:
            continue
        for attr in list(vars(mod).values()):
            if isinstance(attr, functools._lru_cache_wrapper):
                try:
                    attr.cache_clear()
                except Exception:
                    pass
    try:
        from backend.enrichment import knowledge_engine as ke

        ke._VPIC_MEMO.clear()
    except Exception:
        pass


def _use_catalog_built_from_tree(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Path:
    """Point the EPA/options file lookups at a catalog DB built from the dictionary tree.

    ``find_epa_csv`` ranks candidates from ``index/dictionary_catalog.db`` and
    only falls back to a filename glob when that DB is absent. The DB is
    gitignored (.gitignore:110), so a clean checkout has none, and there the
    glob resolves car ``live:1239391`` (2026 Audi A5) to a different EPA file
    whose first matching row is the mild hybrid: 2 golden mismatches on CI run
    37850535029. That fallback is the path prod takes (prod has never had the
    DB) and is filed as a product finding in
    docs/monolith_audit_2026_10_01/tests.md, not changed here.

    The DB is a pure function of the tracked tree (``build_manifest_entries`` +
    ``rebuild_catalog_db``, what ``build_dictionary_manifest.py`` runs minus the
    derived-file enrichment, whose columns no lookup reads), so this golden
    builds its own copy in ``tmp_path`` instead of reading whatever index the
    machine happens to hold. Measured 2026-10-08: the tree build has the same
    14,598 entries as the MBP's on-disk index, and every recorded car matches
    with it, in the main checkout and in a clean worktree alike. Only
    ``dictionary_catalog``'s own copies of the paths are redirected; the real
    index is never opened for writing (the session guard in conftest would
    refuse it anyway).
    """
    from backend.enrichment import dictionary_catalog as dc

    db_path = tmp_path / "dictionary_catalog.db"
    monkeypatch.setattr(dc, "INDEX_DIR", tmp_path)
    monkeypatch.setattr(dc, "CATALOG_DB_PATH", db_path)
    built = dc.rebuild_catalog_db(dc.build_manifest_entries())
    assert built == db_path and db_path.is_file()
    dc.invalidate_catalog_cache()
    return db_path


def _load() -> dict[str, Any]:
    if not FIXTURE.is_file():
        pytest.skip("merge_verified_specs golden fixture not present")
    with gzip.open(FIXTURE, "rt", encoding="utf-8") as fh:
        return json.load(fh)


def _save(data: dict[str, Any]) -> None:
    raw = json.dumps(data, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    with gzip.GzipFile(FIXTURE, "wb", mtime=0) as fh:
        fh.write(raw)


def _all_inputs(data: dict[str, Any]) -> list[tuple[str, dict[str, Any], dict[str, Any] | None]]:
    out: list[tuple[str, dict[str, Any], dict[str, Any] | None]] = []
    for i, (car, vpic) in enumerate(FIXTURE_CARS):
        out.append((f"fixture:{i}", car, vpic))
    for entry in data["live_cars"]:
        out.append((f"live:{entry['car'].get('id')}", entry["car"], entry.get("vpic")))
    return out


def _run_all(data: dict[str, Any], *, replay_vpic: bool) -> dict[str, dict[str, Any]]:
    from backend.enrichment import knowledge_engine as ke
    from backend.enrichment.knowledge_engine_specs import merge_verified_specs

    inputs = _all_inputs(data)
    results: dict[str, dict[str, Any]] = {"True": {}, "False": {}}
    original = ke.lookup_vpic_from_cache
    vpic_by_vin = {(c.get("vin") or None): v for _k, c, v in inputs if v is not None}
    try:
        if replay_vpic:
            ke.lookup_vpic_from_cache = lambda vin: dict(vpic_by_vin.get(vin or None) or {})
        for flag in (True, False):
            for key, car, _vpic in inputs:
                out = merge_verified_specs(dict(car), include_extended_specs=flag)
                assert tuple(out) == RETURN_KEYS, f"{key}: return keys/order moved"
                results[str(flag)][key] = _normalize(out)
    finally:
        ke.lookup_vpic_from_cache = original
    return results


def _compare(expected: dict[str, dict[str, Any]], got: dict[str, dict[str, Any]]) -> None:
    mismatches: list[str] = []
    for flag in ("True", "False"):
        exp = expected[flag]
        act = got[flag]
        assert set(exp) == set(act), f"golden key set moved for include_extended_specs={flag}"
        for key in exp:
            if exp[key] != act[key]:
                diff = {
                    k: (exp[key].get(k), act[key].get(k))
                    for k in set(exp[key]) | set(act[key])
                    if exp[key].get(k) != act[key].get(k)
                }
                mismatches.append(f"[{flag}] {key}: {diff}")
    assert not mismatches, f"{len(mismatches)} golden mismatches; first 10:\n" + "\n".join(mismatches[:10])


# ---------------------------------------------------------------------------
# tests
# ---------------------------------------------------------------------------


def test_merge_verified_specs_golden_hermetic(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    data = _load()
    _use_catalog_built_from_tree(monkeypatch, tmp_path)
    _clear_caches()
    try:
        got = _run_all(data, replay_vpic=True)
    finally:
        _clear_caches()
    if RECORD:
        data["expected_hermetic"] = got
        _save(data)
        return
    _compare(data["expected_hermetic"], got)


@pytest.mark.integration
@pytest.mark.skipif(not LIVE, reason="set MERGE_SPECS_GOLDEN_LIVE_DB=1 to run against local Postgres (read-only)")
def test_merge_verified_specs_golden_live_db(monkeypatch: pytest.MonkeyPatch) -> None:
    data = _load()
    monkeypatch.setenv("INVENTORY_DATABASE_URL", LIVE_DB_URL)
    monkeypatch.setenv("INVENTORY_SQLITE_TESTS", "")
    monkeypatch.setenv("PGOPTIONS", READ_ONLY_PGOPTIONS)
    _clear_caches()
    try:
        got = _run_all(data, replay_vpic=False)
    finally:
        _clear_caches()
    if RECORD:
        data["expected_live"] = got
        _save(data)
        return
    _compare(data["expected_live"], got)


# ---------------------------------------------------------------------------
# input capture (read-only)
# ---------------------------------------------------------------------------


def capture_inputs(limit: int = 2000) -> None:
    """Read *limit* active cars + their vPIC decode from local Postgres, read-only."""
    os.environ["INVENTORY_DATABASE_URL"] = LIVE_DB_URL
    os.environ["PGOPTIONS"] = READ_ONLY_PGOPTIONS
    import psycopg
    from psycopg.rows import dict_row

    from backend.enrichment import knowledge_engine as ke

    with psycopg.connect(LIVE_DB_URL, row_factory=dict_row) as conn:
        conn.execute("SET default_transaction_read_only = on")
        rows = conn.execute(
            "SELECT * FROM cars WHERE listing_removed_at IS NULL ORDER BY md5(id::text) LIMIT %s",
            (limit,),
        ).fetchall()
    live = []
    for r in rows:
        car = {k: _normalize(v) for k, v in r.items() if k not in HEAVY_COLUMNS}
        live.append({"car": car, "vpic": _normalize(ke.lookup_vpic_from_cache(r.get("vin")))})
    data = _load() if FIXTURE.is_file() else {}
    data["live_cars"] = live
    data.setdefault("expected_hermetic", {})
    data.setdefault("expected_live", {})
    _save(data)
    print(f"captured {len(live)} cars -> {FIXTURE}")


if __name__ == "__main__":  # pragma: no cover - manual capture
    if sys.argv[1:2] == ["capture"]:
        capture_inputs()
