#!/usr/bin/env python3
"""
Merge laptop Postgres CSV exports (``laptopdb/``) into the local inventory database.

Imports normalized ``catalog_*`` tables and upserts inventory rows (``cars``,
``nhtsa_vpic_cache``, ``dealerships``, ``dealer_geopoints``, ``incomplete_listings``).

Keeps existing ``epa_master`` rows on the target DB when the laptop export is empty.

Usage:
  PYTHONPATH=. python -m backend.scripts.merge_laptopdb
  PYTHONPATH=. python -m backend.scripts.merge_laptopdb --dir laptopdb --dry-run
  PYTHONPATH=. python -m backend.scripts.merge_laptopdb --catalog-only
"""
from __future__ import annotations

import argparse
import csv
import logging
import sys
from pathlib import Path
from typing import Any, Iterable

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(_REPO_ROOT))

try:
    from backend.utils.project_env import load_project_dotenv

    load_project_dotenv()
except ImportError:
    pass

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("merge_laptopdb")

CATALOG_TABLES = (
    "catalog_package_features",
    "catalog_packages",
    "catalog_options",
    "catalog_exterior_colors",
    "catalog_interior_colors",
    "catalog_trims",
)

CATALOG_IMPORT_ORDER = (
    ("catalog_trims", "catalog_trims"),
    ("catalog_packages", "catalog_packages"),
    ("catalog_package_features", "catalog_package_features"),
    ("catalog_options", "catalog_options"),
    ("catalog_exterior_colors", "catalog_exterior_colors"),
    ("catalog_interior_colors", "catalog_interior_colors"),
)

INVENTORY_UPSERTS = (
    "cars",
    "nhtsa_vpic_cache",
    "dealerships",
    "dealer_geopoints",
    "incomplete_listings",
    "dealer_scan_profile",
)


def _find_csv(directory: Path, table: str) -> Path | None:
    prefix = f"{table}_"
    matches = sorted(
        p
        for p in directory.glob(f"{table}_*.csv")
        if p.name.startswith(prefix) and not p.name.startswith(f"{table}_meta")
    )
    return matches[-1] if matches else None


def _blank(v: str | None) -> bool:
    return v is None or str(v).strip() == ""


def _parse_bool(v: str | None) -> bool | None:
    if _blank(v):
        return None
    return str(v).strip().lower() in ("1", "true", "t", "yes", "y")


def _parse_int(v: str | None) -> int | None:
    if _blank(v):
        return None
    try:
        return int(float(str(v).strip()))
    except (TypeError, ValueError):
        return None


def _parse_float(v: str | None) -> float | None:
    if _blank(v):
        return None
    try:
        return float(str(v).strip())
    except (TypeError, ValueError):
        return None


def _read_rows(path: Path) -> list[dict[str, str]]:
    csv.field_size_limit(min(sys.maxsize, 10_000_000))
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def _table_columns(cur: Any, table: str) -> list[str]:
    cur.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = %s
        ORDER BY ordinal_position
        """,
        (table,),
    )
    return [r[0] for r in cur.fetchall()]


def _reset_sequence(cur: Any, table: str, col: str = "id") -> None:
    cur.execute(
        f"""
        SELECT setval(
            pg_get_serial_sequence(%s, %s),
            COALESCE((SELECT MAX({col}) FROM {table}), 1),
            (SELECT COUNT(*) > 0 FROM {table})
        )
        """,
        (table, col),
    )


def _import_catalog_table(
    cur: Any,
    table: str,
    rows: list[dict[str, str]],
    *,
    dry_run: bool,
) -> int:
    if not rows:
        return 0
    if dry_run:
        return len(rows)

    if table == "catalog_trims":
        sql = """
            INSERT INTO catalog_trims (
                id, year, make, model, trim, body_style, trim_level, engine_l, engine_desc,
                cylinders, horsepower, torque_lb_ft, fuel_type, forced_induction, transmission,
                trans_speeds, drivetrain, mpg_city, mpg_highway, mpg_combined, range_miles,
                base_msrp, source, notes, created_at, updated_at, search_vector, embedding
            ) VALUES (
                %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::tsvector,%s
            )
        """
        batch = []
        for r in rows:
            sv = (r.get("search_vector") or "").strip() or None
            batch.append(
                (
                    _parse_int(r.get("id")),
                    _parse_int(r.get("year")),
                    (r.get("make") or "").strip(),
                    (r.get("model") or "").strip(),
                    (r.get("trim") or "").strip() or None,
                    (r.get("body_style") or "").strip() or None,
                    (r.get("trim_level") or "").strip() or None,
                    (r.get("engine_l") or "").strip() or None,
                    (r.get("engine_desc") or "").strip() or None,
                    _parse_int(r.get("cylinders")),
                    _parse_int(r.get("horsepower")),
                    _parse_int(r.get("torque_lb_ft")),
                    (r.get("fuel_type") or "").strip() or None,
                    (r.get("forced_induction") or "").strip() or None,
                    (r.get("transmission") or "").strip() or None,
                    _parse_int(r.get("trans_speeds")),
                    (r.get("drivetrain") or "").strip() or None,
                    _parse_int(r.get("mpg_city")),
                    _parse_int(r.get("mpg_highway")),
                    _parse_int(r.get("mpg_combined")),
                    _parse_int(r.get("range_miles")),
                    _parse_float(r.get("base_msrp")),
                    (r.get("source") or "").strip() or None,
                    (r.get("notes") or "").strip() or None,
                    (r.get("created_at") or "").strip() or None,
                    (r.get("updated_at") or "").strip() or None,
                    sv,
                    (r.get("embedding") or "").strip() or None,
                )
            )
        cur.executemany(sql, batch)
        return len(batch)

    if table == "catalog_packages":
        sql = """
            INSERT INTO catalog_packages (
                id, vehicle_id, package_code, package_name, package_msrp,
                is_required, sort_order, notes
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
        """
        batch = [
            (
                _parse_int(r.get("id")),
                _parse_int(r.get("vehicle_id")),
                (r.get("package_code") or "").strip() or None,
                (r.get("package_name") or "").strip(),
                _parse_float(r.get("package_msrp")),
                bool(_parse_bool(r.get("is_required"))),
                _parse_int(r.get("sort_order")) or 0,
                (r.get("notes") or "").strip() or None,
            )
            for r in rows
        ]
        cur.executemany(sql, batch)
        return len(batch)

    if table == "catalog_package_features":
        sql = """
            INSERT INTO catalog_package_features (
                id, package_id, feature_name, feature_category, sort_order
            ) VALUES (%s,%s,%s,%s,%s)
        """
        batch = [
            (
                _parse_int(r.get("id")),
                _parse_int(r.get("package_id")),
                (r.get("feature_name") or "").strip(),
                (r.get("feature_category") or "").strip() or None,
                _parse_int(r.get("sort_order")) or 0,
            )
            for r in rows
        ]
        cur.executemany(sql, batch)
        return len(batch)

    if table == "catalog_options":
        sql = """
            INSERT INTO catalog_options (
                id, vehicle_id, option_code, option_name, option_msrp, category, description
            ) VALUES (%s,%s,%s,%s,%s,%s,%s)
        """
        batch = [
            (
                _parse_int(r.get("id")),
                _parse_int(r.get("vehicle_id")),
                (r.get("option_code") or "").strip() or None,
                (r.get("option_name") or "").strip(),
                _parse_float(r.get("option_msrp")),
                (r.get("category") or "").strip() or None,
                (r.get("description") or "").strip() or None,
            )
            for r in rows
        ]
        cur.executemany(sql, batch)
        return len(batch)

    if table == "catalog_exterior_colors":
        sql = """
            INSERT INTO catalog_exterior_colors (
                id, vehicle_id, color_name, color_code, finish_type, hex_code, extra_cost
            ) VALUES (%s,%s,%s,%s,%s,%s,%s)
        """
        batch = [
            (
                _parse_int(r.get("id")),
                _parse_int(r.get("vehicle_id")),
                (r.get("color_name") or "").strip(),
                (r.get("color_code") or "").strip() or None,
                (r.get("finish_type") or "").strip() or None,
                (r.get("hex_code") or "").strip() or None,
                _parse_float(r.get("extra_cost")),
            )
            for r in rows
        ]
        cur.executemany(sql, batch)
        return len(batch)

    if table == "catalog_interior_colors":
        sql = """
            INSERT INTO catalog_interior_colors (
                id, vehicle_id, color_name, color_code, material, hex_code, extra_cost
            ) VALUES (%s,%s,%s,%s,%s,%s,%s)
        """
        batch = [
            (
                _parse_int(r.get("id")),
                _parse_int(r.get("vehicle_id")),
                (r.get("color_name") or "").strip(),
                (r.get("color_code") or "").strip() or None,
                (r.get("material") or "").strip() or None,
                (r.get("hex_code") or "").strip() or None,
                _parse_float(r.get("extra_cost")),
            )
            for r in rows
        ]
        cur.executemany(sql, batch)
        return len(batch)

    raise ValueError(f"unsupported catalog table: {table}")


def import_catalog(cur: Any, directory: Path, *, dry_run: bool) -> dict[str, int]:
    from backend.db.catalog_schema import ensure_catalog_tables

    ensure_catalog_tables(cur, postgres=True)
    stats: dict[str, int] = {}

    if not dry_run:
        cur.execute(
            "TRUNCATE TABLE "
            + ", ".join(CATALOG_TABLES)
            + " RESTART IDENTITY CASCADE"
        )

    for prefix, table in CATALOG_IMPORT_ORDER:
        path = _find_csv(directory, prefix)
        if not path:
            log.warning("missing CSV for %s under %s", table, directory)
            stats[table] = 0
            continue
        rows = _read_rows(path)
        n = _import_catalog_table(cur, table, rows, dry_run=dry_run)
        stats[table] = n
        log.info("catalog %s: %d rows from %s", table, n, path.name)
        if not dry_run and n:
            _reset_sequence(cur, table)

    return stats


def _upsert_rows(
    cur: Any,
    table: str,
    rows: list[dict[str, str]],
    *,
    conflict_col: str,
    dry_run: bool,
) -> int:
    if not rows:
        return 0
    target_cols = _table_columns(cur, table)
    skip = {"id"} if conflict_col != "id" else set()
    cols = [c for c in rows[0].keys() if c in target_cols and c not in skip]
    if not cols:
        return 0
    if dry_run:
        return len(rows)

    placeholders = ", ".join(["%s"] * len(cols))
    col_list = ", ".join(cols)
    updates = ", ".join(f"{c} = EXCLUDED.{c}" for c in cols if c != conflict_col)
    sql = (
        f"INSERT INTO {table} ({col_list}) VALUES ({placeholders}) "
        f"ON CONFLICT ({conflict_col}) DO UPDATE SET {updates}"
    )
    batch = []
    not_null_cols = {
        "dealerships": {"name", "website_url", "city", "state"},
    }.get(table, set())
    for r in rows:
        vals = []
        for c in cols:
            raw = r.get(c)
            if _blank(raw):
                vals.append("" if c in not_null_cols else None)
            else:
                vals.append(raw)
        batch.append(tuple(vals))
    cur.executemany(sql, batch)
    return len(batch)


def _merge_cars(cur: Any, directory: Path, *, dry_run: bool) -> int:
    path = _find_csv(directory, "cars")
    if not path:
        return 0
    rows = _read_rows(path)
    target_cols = set(_table_columns(cur, "cars")) - {"id"}
    cols = [c for c in rows[0].keys() if c in target_cols]
    if not cols or "vin" not in cols:
        return 0
    if dry_run:
        return len(rows)

    placeholders = ", ".join(["%s"] * len(cols))
    col_list = ", ".join(cols)
    updates = ", ".join(f"{c} = EXCLUDED.{c}" for c in cols if c != "vin")
    sql = (
        f"INSERT INTO cars ({col_list}) VALUES ({placeholders}) "
        f"ON CONFLICT (vin) DO UPDATE SET {updates}"
    )
    batch = []
    for r in rows:
        vin = (r.get("vin") or "").strip()
        if not vin:
            continue
        batch.append(tuple((None if _blank(r.get(c)) else r.get(c) for c in cols)))
    cur.executemany(sql, batch)
    log.info("cars upserted: %d rows from %s", len(batch), path.name)
    return len(batch)


def _merge_incomplete_listings(cur: Any, directory: Path, *, dry_run: bool) -> int:
    path = _find_csv(directory, "incomplete_listings")
    if not path:
        return 0
    rows = _read_rows(path)
    if dry_run:
        return len(rows)

    inserted = 0
    for r in rows:
        vin = (r.get("vin") or "").strip().upper()
        if not vin:
            continue
        cur.execute("SELECT id FROM cars WHERE upper(vin) = %s LIMIT 1", (vin,))
        hit = cur.fetchone()
        if not hit:
            continue
        car_id = hit[0]
        cur.execute(
            """
            INSERT INTO incomplete_listings (car_id, vin, missing_fields_json, updated_at)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (car_id) DO UPDATE SET
                vin = EXCLUDED.vin,
                missing_fields_json = EXCLUDED.missing_fields_json,
                updated_at = EXCLUDED.updated_at
            """,
            (
                car_id,
                vin,
                r.get("missing_fields_json") or "[]",
                r.get("updated_at") or "",
            ),
        )
        inserted += 1
    log.info("incomplete_listings merged: %d rows", inserted)
    return inserted


def merge_inventory(cur: Any, directory: Path, *, dry_run: bool) -> dict[str, int]:
    stats: dict[str, int] = {}
    stats["cars"] = _merge_cars(cur, directory, dry_run=dry_run)

    for table, conflict in (
        ("nhtsa_vpic_cache", "vin"),
        ("dealerships", "id"),
        ("dealer_geopoints", "dealer_url"),
        ("dealer_scan_profile", "dealer_id"),
    ):
        path = _find_csv(directory, table)
        if not path:
            stats[table] = 0
            continue
        rows = _read_rows(path)
        n = _upsert_rows(cur, table, rows, conflict_col=conflict, dry_run=dry_run)
        stats[table] = n
        log.info("%s upserted: %d rows", table, n)

    stats["incomplete_listings"] = _merge_incomplete_listings(cur, directory, dry_run=dry_run)
    return stats


def drop_legacy_vehicle_specs(cur: Any, *, dry_run: bool) -> None:
    cur.execute(
        """
        SELECT EXISTS (
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = 'public' AND table_name = 'vehicle_specs'
        )
        """
    )
    if not cur.fetchone()[0]:
        return
    if dry_run:
        log.info("would drop legacy table vehicle_specs")
        return
    cur.execute("DROP TABLE vehicle_specs")
    log.info("dropped legacy table vehicle_specs")


def merge_laptopdb(
    directory: Path,
    *,
    catalog_only: bool = False,
    inventory_only: bool = False,
    dry_run: bool = False,
) -> dict[str, Any]:
    from backend.db.inventory_pg import inventory_postgres_dsn, is_inventory_postgres

    if not is_inventory_postgres():
        raise SystemExit("merge_laptopdb requires Postgres (INVENTORY_DATABASE_URL / DATABASE_URL).")

    if not directory.is_dir():
        raise SystemExit(f"laptopdb directory not found: {directory}")

    import psycopg

    summary: dict[str, Any] = {"catalog": {}, "inventory": {}}
    dsn = inventory_postgres_dsn()
    assert dsn is not None
    with psycopg.connect(dsn) as conn:
        cur = conn.cursor()
        if not inventory_only:
            summary["catalog"] = import_catalog(cur, directory, dry_run=dry_run)
        if not catalog_only:
            summary["inventory"] = merge_inventory(cur, directory, dry_run=dry_run)
        drop_legacy_vehicle_specs(cur, dry_run=dry_run)
        if dry_run:
            conn.rollback()
        else:
            conn.commit()
    return summary


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dir",
        type=Path,
        default=_REPO_ROOT / "laptopdb",
        help="Directory containing table CSV exports (default: repo laptopdb/)",
    )
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--catalog-only", action="store_true")
    parser.add_argument("--inventory-only", action="store_true")
    args = parser.parse_args(argv)

    summary = merge_laptopdb(
        args.dir.resolve(),
        catalog_only=args.catalog_only,
        inventory_only=args.inventory_only,
        dry_run=args.dry_run,
    )
    log.info("merge complete: %s", summary)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
