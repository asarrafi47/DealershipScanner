#!/usr/bin/env python3
"""
Download EPA fueleconomy.gov vehicles.csv and import it into the inventory `epa_master`.

Usage (from repo root):
  PYTHONPATH=. python backend/scripts/import_epa_master.py --append-years 2027 --csv vehicles.csv
  PYTHONPATH=. python backend/scripts/import_epa_master.py --append-years 2027 --csv vehicles.csv --apply
  PYTHONPATH=. python backend/scripts/import_epa_master.py --yes   # empty-catalog bootstrap only
  PYTHONPATH=. python backend/scripts/import_epa_master.py --refresh-induction --csv vehicles.csv
  PYTHONPATH=. python backend/scripts/import_epa_master.py --refresh-induction --csv vehicles.csv \
      --apply --backup induction_backup.csv
  PYTHONPATH=. python backend/scripts/import_epa_master.py --restore induction_backup.csv --apply

Runs on the inventory backend (Postgres via INVENTORY_DATABASE_URL; SQLite only under
pytest). Creates `epa_master` if missing.

``--append-years`` (P10B.3, D-ER1 a) is the one ingestion path: vehicles.csv -> dictionary
CSVs (``import_epa_to_dictionary`` for those years) -> ``build_epa_master_pg --apply``.
Rows reach ``epa_master`` through the builder, so they carry ``source='dictionary_epa'``
and an ``epa_key`` natural key, and a re-run inserts nothing. It never deletes and never
archives. Without ``--apply`` it writes nothing (the dictionary step goes to a scratch
folder and the builder runs as a dry run). ``import_csv(append=True)`` remains for
callers that pass a connection but is no longer reachable from the CLI.

``--yes`` (``import_csv(append=False)``) is the empty-catalog bootstrap only: the CLI
refuses it (exit 2) once epa_master holds any row. The underlying replace runs ``DELETE FROM
epa_master`` and reloads with fresh ids. That deletion cascades to
``epa_extended_specs`` and leaves every ``cars.epa_master_id`` dangling, so it is
refused (RuntimeError; exit 2 from the CLI, with the counts printed) while any car is
linked or any extended-specs row exists. It still works on an empty, unlinked catalog.

``--refresh-induction`` (P10A.8) re-labels forced induction on the rows already in
``epa_master`` from EPA's own tCharger/sCharger/eng_dscr fields, set-based (one COPY into a
temp table, one UPDATE FROM). It never deletes, never creates a NULL ``engine_display``,
rewrites only the induction suffix of an existing display, and leaves every other column
alone. Dry run by default; ``--apply`` needs ``--backup`` (a CSV of the touched rows'
previous values) and ``--restore`` puts them back. Postgres only.

Data: https://www.fueleconomy.gov/feg/epadata/vehicles.csv
"""
from __future__ import annotations

import csv
import hashlib
import json
import os
import sqlite3
import sys
import urllib.request
from collections import Counter
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
os.chdir(ROOT)
sys.path.insert(0, str(ROOT))

try:
    from backend.utils.project_env import load_project_dotenv

    load_project_dotenv()
except ImportError:
    pass

EPA_URL = "https://www.fueleconomy.gov/feg/epadata/vehicles.csv"


def ensure_table(conn: Any) -> None:
    # epa_master's full schema (including columns this script doesn't know about —
    # trim/body_style/engine_description/forced_induction/etc.) is centrally managed
    # in backend/db/inventory_pg.py's init_postgres_inventory(). A local ad-hoc
    # CREATE TABLE + PRAGMA-based column check here would be redundant against
    # Postgres and actively dangerous (PRAGMA becomes a no-op via the SQL adapter,
    # so ``have`` would always be empty, and the un-guarded ALTER TABLE ADD COLUMN
    # calls would crash on Postgres's "column already exists" error on any run
    # after the first).
    from backend.db.inventory_db import init_inventory_db

    init_inventory_db()


def norm_float(s: str) -> float | None:
    try:
        return float(str(s).strip()) if str(s).strip() else None
    except ValueError:
        return None


def norm_int(s: str) -> int | None:
    try:
        return int(float(str(s).strip())) if str(s).strip() else None
    except ValueError:
        return None


_INSERT_SQL = (
    "INSERT INTO epa_master (epa_vehicle_id, year, make, model, cylinders, displacement, trany, drive, "
    "fuel_type, city08, highway08, city_e, highway_e, atv_type, forced_induction, t_charger, s_charger, "
    "source, natural_key) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
)

SOURCE_CSV = "epa_vehicles_csv"

_DISPLAY_SUFFIXES = (" Twin Supercharged", " Twin Turbo", " Supercharged", " Turbo")


def _text(value: Any) -> str:
    return "" if value is None else str(value).strip()


def induction_label(
    *, t_charger: Any, s_charger: Any, eng_dscr: Any, year: Any, make: Any, model: Any, trim: Any = None,
    cylinders: Any = None, displacement: Any = None, atv_type: Any = None, fuel_type: Any = None,
    existing_label: str | None = None,
) -> str:
    """EPA-flag label for one catalog row; the twin decision is the one shared with the
    dictionary CSV importer (``epa_twin_hint``, D-ET5)."""
    from backend.dictionary.epa_engine import _epa_row_car_context
    from backend.utils.forced_induction import (
        classify_forced_induction_from_car_row,
        epa_twin_hint,
        forced_induction_from_epa,
    )

    ctx = _epa_row_car_context({
        "Make": make, "Model": model, "Trim": trim, "Year": year, "cylinders": cylinders,
        "displacement": displacement, "engineOptions": eng_dscr, "fuelType": fuel_type,
    })
    try:
        classifier = classify_forced_induction_from_car_row(ctx)
    except Exception:
        classifier = None
    return forced_induction_from_epa(
        t_charger, s_charger, _text(eng_dscr), _text(model), _text(atv_type),
        twin_hint=epa_twin_hint(classifier, existing_label),
    )


def rewrite_display_suffix(display: str | None, label: str) -> str | None:
    """``display`` with only its induction suffix replaced by the one ``label`` implies.

    A NULL or empty display stays as it is: the 461 id-only rows must not get a layout
    guessed from the cylinder count.
    """
    if display is None or not str(display).strip():
        return display
    from backend.utils.forced_induction import forced_induction_short_suffix

    base = str(display).strip()
    for suf in _DISPLAY_SUFFIXES:
        if base.endswith(suf):
            base = base[: -len(suf)]
            break
    suffix = forced_induction_short_suffix(label)
    return f"{base} {suffix}" if suffix else base


def _insert_batch(conn: Any, batch: list[tuple]) -> None:
    """Row-by-row on the Postgres adapter (no executemany), batch on SQLite."""
    if hasattr(conn, "executemany"):
        conn.executemany(_INSERT_SQL, batch)
        return
    for row in batch:
        conn.execute(_INSERT_SQL, row)


EXIT_REFUSED = 2


class FullReplaceRefused(RuntimeError):
    """A full replace of ``epa_master`` would orphan car links or extended specs."""

    def __init__(self, *, linked_cars: int, extended_specs: int, catalog_rows: int) -> None:
        self.linked_cars = linked_cars
        self.extended_specs = extended_specs
        self.catalog_rows = catalog_rows
        super().__init__(
            "Refusing to replace epa_master: "
            f"{linked_cars} car(s) have cars.epa_master_id set and epa_extended_specs holds "
            f"{extended_specs} row(s) (epa_master has {catalog_rows} row(s)). "
            "DELETE FROM epa_master would cascade to epa_extended_specs and leave every car "
            "link pointing at an id that no longer exists. Use --append-years to add model years."
        )


class BootstrapRefused(RuntimeError):
    """The CLI refused: the request is outside the single ingestion path."""


def _is_missing_table_error(exc: BaseException) -> bool:
    """True only for "table does not exist" (SQLite OperationalError / Postgres 42P01)."""
    if getattr(exc, "sqlstate", None) == "42P01":
        return True
    return isinstance(exc, sqlite3.OperationalError) and "no such table" in str(exc).lower()


def _scalar_count(conn: Any, sql: str) -> int:
    cur = conn.cursor()
    cur.execute(sql)
    row = cur.fetchone()
    return int(row[0] or 0) if row else 0


def _count_extended_specs(conn: Any) -> int:
    """Rows in ``epa_extended_specs``; 0 only when the table does not exist.

    The probe runs inside a savepoint: on Postgres a failed statement (the table is
    absent on SQLite and on some fresh databases) would otherwise abort the whole
    transaction. Any other error is re-raised, so the guard fails closed.
    """
    conn.execute("SAVEPOINT epa_xspecs_probe")
    try:
        n = _scalar_count(conn, "SELECT COUNT(*) FROM epa_extended_specs")
    except Exception as exc:
        conn.execute("ROLLBACK TO SAVEPOINT epa_xspecs_probe")
        conn.execute("RELEASE SAVEPOINT epa_xspecs_probe")
        if _is_missing_table_error(exc):
            return 0
        raise
    conn.execute("RELEASE SAVEPOINT epa_xspecs_probe")
    return n


def assert_full_replace_allowed(conn: Any) -> None:
    """Raise :class:`FullReplaceRefused` unless ``epa_master`` is safe to delete.

    Safe means no car carries an ``epa_master_id`` and ``epa_extended_specs`` is empty
    (or absent). Errors reading ``cars`` propagate, so the replace does not run.
    """
    linked = _scalar_count(conn, "SELECT COUNT(*) FROM cars WHERE epa_master_id IS NOT NULL")
    xspecs = _count_extended_specs(conn)
    if linked or xspecs:
        catalog = _scalar_count(conn, "SELECT COUNT(*) FROM epa_master")
        raise FullReplaceRefused(linked_cars=linked, extended_specs=xspecs, catalog_rows=catalog)


def assert_catalog_empty(conn: Any) -> None:
    """Raise :class:`FullReplaceRefused`-style refusal unless epa_master has no rows."""
    n = _scalar_count(conn, "SELECT COUNT(*) FROM epa_master")
    if n:
        raise BootstrapRefused(
            f"epa_master already holds {n} row(s); --yes is the empty-catalog bootstrap only. "
            "Use --append-years (vehicles.csv -> dictionary CSVs -> build_epa_master_pg --apply)."
        )


def append_years(years: set[int], csv_path: str, *, apply: bool = False,
                 dictionary_dir: Path | str | None = None) -> dict:
    """P10B.3: add model years through the single ingestion path.

    Dictionary CSVs for *years* are regenerated from *csv_path*, then the builder loads
    them (``--apply`` only; the builder never archives here). A dry run overlays the new
    dictionary files on a scratch copy of the real dictionary and runs the builder dry run on it.
    """
    import tempfile
    from collections import Counter

    from backend.enrichment import dictionary_paths
    from backend.scripts import build_epa_master_pg as builder
    from backend.scripts import import_epa_to_dictionary as to_dict

    stats: Counter = Counter()
    root = Path(dictionary_dir) if dictionary_dir else Path(dictionary_paths.DICTIONARY_ROOT)
    # process_epa_csv nests under epa/<make>/ only for the real dictionary root.
    epa_dir = root / "epa" if root.resolve() == Path(dictionary_paths.DICTIONARY_ROOT).resolve() else root
    if not apply:
        # Preview against a scratch copy of the real dictionary with the new-year files
        # overlaid, so the builder diff counts the rows --apply would insert.
        import shutil

        with tempfile.TemporaryDirectory(prefix="epa_append_dry_") as scratch:
            scratch_root = Path(scratch) / "dictionary"
            if root.is_dir():
                shutil.copytree(root, scratch_root)
            else:
                scratch_root.mkdir()
            written = to_dict.process_epa_csv(Path(csv_path), 1950, scratch_root, years=set(years), stats=stats)
            scratch_epa = scratch_root / "epa" if epa_dir != root else scratch_root
            report = builder.build_epa_master(dry_run=True, source_dir=scratch_epa)
        return {"mode": "dry_run", "years": sorted(years), "dictionary_files": written,
                "failed_rows": stats["failed"], "builder": report}
    written = to_dict.process_epa_csv(Path(csv_path), 1950, root, years=set(years), stats=stats)
    if stats["failed"]:
        raise BootstrapRefused(f"{stats['failed']} vehicles.csv row(s) failed to convert; builder not run")
    report = builder.build_epa_master(apply=True, source_dir=epa_dir)
    return {"mode": "apply", "years": sorted(years), "dictionary_files": written,
            "failed_rows": 0, "builder": report}


def import_csv(path: str, conn: Any, *, years: set[int] | None = None, append: bool = False) -> int:
    """Import vehicles.csv. ``append=True`` keeps every existing row and inserts
    only rows (restricted to *years* when given) whose epa_vehicle_id is not in
    the table yet: the way to pull a new model year (2027: 84 rows in July, 541
    in the September file) without touching the curated columns other scripts
    added (trim, body_style, engine_description).

    ``append=False`` replaces the whole table, and raises :class:`FullReplaceRefused`
    (a RuntimeError) before the DELETE while any car is linked or any extended-specs
    row exists. The check runs again just before the commit, in the same transaction,
    so a link written while the CSV loads rolls the replace back too."""
    ensure_table(conn)
    existing_ids: set[int] = set()
    if not append:
        try:
            assert_full_replace_allowed(conn)
        except Exception:
            conn.rollback()
            raise
    if append:
        cur = conn.cursor()
        if years:
            cur.execute(
                "SELECT epa_vehicle_id FROM epa_master WHERE year IN (" + ",".join("?" * len(years)) + ")",
                tuple(sorted(years)),
            )
        else:
            cur.execute("SELECT epa_vehicle_id FROM epa_master")
        existing_ids = {int(r[0]) for r in cur.fetchall() if r[0] is not None}
    else:
        conn.execute("DELETE FROM epa_master")
    rows = 0
    with open(path, "r", encoding="utf-8", errors="replace", newline="") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            return 0
        # Normalize header keys (strip BOM / spaces)
        fieldnames = [c.strip().lstrip("\ufeff") for c in reader.fieldnames]

        def norm_row(raw: dict) -> dict:
            return {(k or "").strip().lstrip("\ufeff"): v for k, v in raw.items()}

        def col(*names: str) -> str | None:
            for n in names:
                for fn in fieldnames:
                    if fn.lower() == n.lower():
                        return fn
            return None

        c_id = col("id")
        c_year = col("year")
        c_make = col("make")
        c_model = col("model")
        c_cyl = col("cylinders", "cyl")
        c_displ = col("displ", "displacement")
        c_trany = col("trany", "transmission")
        c_drive = col("drive", "drivetrain")
        c_fuel = col("fuelType", "fuel_type", "fuelType1")
        c_city = col("city08")
        c_hwy = col("highway08")
        c_citye = col("cityE")
        c_hwye = col("highwayE")
        c_tch = col("tCharger")
        c_sch = col("sCharger")
        c_dscr = col("eng_dscr")
        # fueleconomy.gov uses atvType; some exports differ slightly
        c_atv = col("atvType", "ATVType", "atv_type", "vehicleType", "VehType")
        if not c_atv:
            for fn in fieldnames:
                n = (fn or "").strip().replace(" ", "").replace("_", "").lower()
                if n == "atvtype" or n.endswith("atvtype"):
                    c_atv = fn
                    break

        if not c_year or not c_make or not c_model:
            print("Could not find year/make/model columns in CSV.", file=sys.stderr)
            print("Fields:", fieldnames[:20], file=sys.stderr)
            return 0
        if not c_atv:
            print(
                "Note: no atvType column found in CSV — PHEV/Hybrid 'Electric +' prefix will use trim/VIN rules only.",
                file=sys.stderr,
            )

        batch = []
        for raw in reader:
            row = norm_row(raw)
            def get(name: str | None) -> str:
                if not name:
                    return ""
                return (row.get(name) or "").strip()

            y = norm_int(get(c_year))
            mk = get(c_make)
            md = get(c_model)
            if y is None or not mk or not md:
                continue
            if years and y not in years:
                continue
            vid = norm_int(get(c_id)) if c_id else None
            if append and (vid is None or vid in existing_ids):
                continue
            cyl = norm_int(get(c_cyl)) if c_cyl else None
            displ = norm_float(get(c_displ)) if c_displ else None
            trany = get(c_trany) or None
            drive = get(c_drive) or None
            fuel = get(c_fuel) or None
            city08 = norm_float(get(c_city)) if c_city else None
            highway08 = norm_float(get(c_hwy)) if c_hwy else None
            city_e = norm_float(get(c_citye)) if c_citye else None
            highway_e = norm_float(get(c_hwye)) if c_hwye else None
            atv_type = (get(c_atv) or None) if c_atv else None
            t_ch, s_ch = get(c_tch), get(c_sch)
            label = induction_label(
                t_charger=t_ch, s_charger=s_ch, eng_dscr=get(c_dscr), year=y, make=mk, model=md,
                cylinders=cyl, displacement=displ, atv_type=atv_type, fuel_type=fuel,
            )
            batch.append(
                (
                    vid,
                    y,
                    mk,
                    md,
                    cyl,
                    displ,
                    trany,
                    drive,
                    fuel,
                    city08,
                    highway08,
                    city_e,
                    highway_e,
                    atv_type,
                    label,
                    t_ch,
                    s_ch,
                    SOURCE_CSV,
                    f"vid:{vid}" if vid is not None else None,
                )
            )
            rows += 1
            if len(batch) >= 2000:
                _insert_batch(conn, batch)
                batch = []
        if batch:
            _insert_batch(conn, batch)
    if not append:
        try:
            assert_full_replace_allowed(conn)
        except Exception:
            conn.rollback()
            raise
    conn.commit()
    return rows


# --- P10A.8: induction refresh and restore (Postgres, set-based) -------------------------------

EXIT_USAGE = 2
_REFRESH_COLS = ("forced_induction", "engine_display", "t_charger", "s_charger")
_NULL = "\\N"  # how the backup CSV spells NULL, so '' (refreshed, blank flag) survives a round trip


class RefreshRefused(RuntimeError):
    """The refresh or restore cannot run safely; nothing was written."""


def sha256_of(path: str | Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def load_csv_flags(path: str | Path) -> dict[int, dict[str, str]]:
    """vehicles.csv as ``{epa id: {t, s, dscr}}`` (only what the label rules read)."""
    out: dict[int, dict[str, str]] = {}
    with open(path, "r", encoding="utf-8", errors="replace", newline="") as f:
        for raw in csv.DictReader(f):
            row = {(k or "").strip().lstrip("\ufeff"): (v or "").strip() for k, v in raw.items()}
            vid = norm_int(row.get("id", ""))
            if vid is None:
                continue
            out[vid] = {"t": row.get("tCharger", ""), "s": row.get("sCharger", ""), "dscr": row.get("eng_dscr", "")}
    return out


def plan_refresh(rows: list[dict], flags: dict[int, dict[str, str]]) -> tuple[list[tuple], dict]:
    """Pure planner. ``rows`` are epa_master rows as dicts; returns ``(computed, stats)``.

    ``computed`` holds ``(id, old, new)`` for every row whose epa id is in the CSV, with
    ``old``/``new`` tuples over ``_REFRESH_COLS``. Rows without an id, or whose id the CSV
    lacks, are counted and left alone.
    """
    stats: dict[str, Any] = {"rows": len(rows), "no_epa_id": 0, "not_in_csv": 0, "matched": 0}
    transitions: Counter = Counter()
    computed: list[tuple] = []
    for r in rows:
        vid = r.get("epa_vehicle_id")
        if vid is None:
            stats["no_epa_id"] += 1
            continue
        src = flags.get(int(vid))
        if src is None:
            stats["not_in_csv"] += 1
            continue
        stats["matched"] += 1
        old = tuple(r.get(c) for c in _REFRESH_COLS)
        label = induction_label(
            t_charger=src["t"], s_charger=src["s"], eng_dscr=src["dscr"], year=r.get("year"),
            make=r.get("make"), model=r.get("model"), trim=r.get("trim"), cylinders=r.get("cylinders"),
            displacement=r.get("displacement"), atv_type=r.get("atv_type"), fuel_type=r.get("fuel_type"),
            existing_label=r.get("forced_induction"),
        )
        # a refreshed row stores '' for a blank flag, never NULL
        new = (label, rewrite_display_suffix(r.get("engine_display"), label), src["t"], src["s"])
        computed.append((int(r["id"]), old, new))
        if old != new:
            transitions[(old[0] if old[0] is not None else "NULL", new[0] or "(blank)")] += 1
    stats["changed"] = sum(1 for _, o, n in computed if o != n)
    stats["unchanged"] = stats["matched"] - stats["changed"]
    stats["transitions"] = {f"{a} -> {b}": n for (a, b), n in sorted(transitions.items())}
    return computed, stats


def _require_columns(conn: Any) -> None:
    have = {r[0] for r in conn.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema = 'public' AND table_name = 'epa_master'").fetchall()}
    missing = [c for c in _REFRESH_COLS if c not in have]
    if missing:
        raise RefreshRefused(f"epa_master lacks {missing}: apply migration V036 first")


def _write_backup(path: str | Path, computed: list[tuple]) -> str:
    p = Path(path)
    if p.exists():
        raise RefreshRefused(f"backup {p} already exists; refusing to overwrite it")
    with open(p, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(("id", *_REFRESH_COLS))
        for rid, old, new in computed:
            if old != new:
                w.writerow((rid, *[_NULL if v is None else v for v in old]))
    return sha256_of(p)


def _update_from_temp(conn: Any, rows: list[tuple]) -> int:
    """COPY ``(id, *_REFRESH_COLS)`` rows into a temp table, then one guarded UPDATE FROM."""
    cur = conn.cursor()
    cur.execute(
        "CREATE TEMP TABLE tmp_induction (id bigint PRIMARY KEY, forced_induction text, "
        "engine_display text, t_charger text, s_charger text) ON COMMIT DROP"
    )
    with cur.copy("COPY tmp_induction (id, forced_induction, engine_display, t_charger, s_charger) FROM STDIN") as cp:
        for row in rows:
            cp.write_row(row)
    cur.execute(
        "UPDATE epa_master m SET forced_induction = t.forced_induction, engine_display = t.engine_display, "
        "t_charger = t.t_charger, s_charger = t.s_charger FROM tmp_induction t WHERE m.id = t.id AND "
        "(m.forced_induction, m.engine_display, m.t_charger, m.s_charger) IS DISTINCT FROM "
        "(t.forced_induction, t.engine_display, t.t_charger, t.s_charger)"
    )
    return cur.rowcount


def refresh_induction(
    conn: Any, csv_path: str | Path, *, apply: bool = False, backup_path: str | Path | None = None,
    progress: Any = None,
) -> dict:
    """Re-label forced induction on every ``epa_master`` row from the pinned vehicles.csv.

    Dry run by default. With ``apply`` a backup path is required; the backup is written
    first, the UPDATE count must equal the planned change count or the transaction is
    rolled back. Every duplicate of an epa id is updated (the key is the surrogate id).
    """
    say = progress or (lambda msg: None)
    _require_columns(conn)
    if apply and not backup_path:
        raise RefreshRefused("--apply needs --backup <file> so --restore can undo it")
    cur = conn.cursor()
    cur.execute(
        "SELECT id, epa_vehicle_id, year, make, model, \"trim\", cylinders, displacement, fuel_type, atv_type, "
        "forced_induction, engine_display, t_charger, s_charger FROM epa_master ORDER BY id"
    )
    names = [d[0] for d in cur.description]
    rows = [dict(zip(names, r)) for r in cur.fetchall()]
    flags = load_csv_flags(csv_path)
    say(f"read {len(rows)} epa_master rows and {len(flags)} csv rows")
    computed, stats = plan_refresh(rows, flags)
    stats["csv_sha256"] = sha256_of(csv_path)
    stats["dry_run"] = not apply
    if not apply:
        conn.rollback()
        return stats
    try:
        stats["backup_sha256"] = _write_backup(backup_path, computed)
        say(f"backup written: {backup_path}")
        updated = _update_from_temp(conn, [(rid, *new) for rid, _, new in computed])
        if updated != stats["changed"]:
            raise RefreshRefused(f"UPDATE touched {updated} row(s), the plan said {stats['changed']}; rolled back")
        conn.commit()
    except BaseException:
        conn.rollback()
        raise
    stats["applied"] = updated
    say(f"applied {updated} row(s)")
    return stats


def restore_induction(conn: Any, backup_csv: str | Path, *, apply: bool = False) -> dict:
    """Put the ``_REFRESH_COLS`` back to a ``--backup`` file's values (temp table plus UPDATE FROM)."""
    _require_columns(conn)
    rows: list[tuple] = []
    with open(backup_csv, "r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        header = next(reader, None)
        if tuple(header or ()) != ("id", *_REFRESH_COLS):
            raise RefreshRefused(f"{backup_csv} is not an induction backup (header {header})")
        for rec in reader:
            rows.append((int(rec[0]), *[None if v == _NULL else v for v in rec[1:]]))
    stats: dict[str, Any] = {"backup_rows": len(rows), "backup_sha256": sha256_of(backup_csv), "dry_run": not apply}
    cur = conn.cursor()
    try:
        if not apply:
            cur.execute("SELECT id, forced_induction, engine_display, t_charger, s_charger FROM epa_master "
                        "WHERE id = ANY(%s)", ([r[0] for r in rows],))
            now = {r[0]: tuple(r[1:]) for r in cur.fetchall()}
            stats["missing_ids"] = sum(1 for r in rows if r[0] not in now)
            stats["would_change"] = sum(1 for r in rows if r[0] in now and now[r[0]] != tuple(r[1:]))
            conn.rollback()
            return stats
        stats["applied"] = _update_from_temp(conn, rows)
        conn.commit()
    except BaseException:
        conn.rollback()
        raise
    return stats


def main(argv: list[str] | None = None) -> int:
    import argparse
    import tempfile

    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--append-years", default="", help="comma-separated model years to ADD (no delete), e.g. 2027")
    ap.add_argument("--csv", default="", help="use this local vehicles.csv instead of downloading")
    ap.add_argument(
        "--yes",
        action="store_true",
        help="Required: this replaces ALL of epa_master with the downloaded government CSV "
        "(DELETE FROM epa_master, then reimport) — including this project's own curated/merged "
        "rows built by build_epa_master_pg.py and the 2026-07-06 dump import. Refused (exit 2) "
        "while any cars.epa_master_id is set or epa_extended_specs has rows.",
    )
    ap.add_argument("--refresh-induction", action="store_true",
                    help="Re-label forced induction on existing epa_master rows from --csv (set-based, never deletes).")
    ap.add_argument("--restore", default="", metavar="BACKUP_CSV",
                    help="Put the induction columns back to a --backup file's values.")
    ap.add_argument("--apply", action="store_true", help="With --append-years/--refresh-induction/--restore: write (default is a dry run).")
    ap.add_argument("--backup", default="", help="With --refresh-induction --apply: where to save the previous values.")
    args = ap.parse_args(argv)

    if args.refresh_induction or args.restore:
        return _main_induction(args)

    from backend.db.inventory_db import get_conn
    from backend.db.inventory_pg import is_inventory_postgres

    years = {int(y) for y in args.append_years.split(",") if y.strip().isdigit()}
    if years:
        path = args.csv
        if not path:
            tmp = tempfile.NamedTemporaryFile(mode="w+b", suffix=".csv", delete=False)
            tmp.close()
            path = tmp.name
            print(f"Downloading {EPA_URL} ...")
            urllib.request.urlretrieve(EPA_URL, path)
        from backend.scripts.build_epa_master_pg import BuildRefused

        try:
            result = append_years(years, path, apply=args.apply)
        except (BootstrapRefused, BuildRefused) as exc:
            print(f"import_epa_master: refused: {exc}", file=sys.stderr)
            return EXIT_REFUSED
        print(json.dumps(result, indent=2, sort_keys=True, default=str))
        if not args.apply:
            print("Dry run: nothing written. Re-run with --apply (owner-gated) to load these years.")
        return 0

    if not args.yes:
        conn = get_conn()
        try:
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM epa_master")
            n = cur.fetchone()[0]
        finally:
            conn.close()
        backend_name = "Postgres" if is_inventory_postgres() else "SQLite"
        print(
            f"Refusing to replace {n} existing row(s) in the {backend_name} epa_master table "
            "without --yes. This deletes everything currently in epa_master (including data "
            "from other sources) and replaces it with the fueleconomy.gov download."
        )
        return 1

    # Checked before the download (and again inside import_csv, in the replace's own
    # transaction). A short-lived connection, so no snapshot is held while downloading.
    conn = get_conn()
    try:
        ensure_table(conn)
        assert_full_replace_allowed(conn)
        assert_catalog_empty(conn)
    except (FullReplaceRefused, BootstrapRefused) as exc:
        print(str(exc), file=sys.stderr)
        return EXIT_REFUSED
    finally:
        conn.close()

    tmp = tempfile.NamedTemporaryFile(mode="w+b", suffix=".csv", delete=False)
    tmp.close()
    path = tmp.name
    try:
        print(f"Downloading {EPA_URL} ...")
        urllib.request.urlretrieve(EPA_URL, path)
        conn = get_conn()
        try:
            n = import_csv(path, conn)
        except FullReplaceRefused as exc:
            print(str(exc), file=sys.stderr)
            return EXIT_REFUSED
        finally:
            conn.close()
        print(f"Imported {n} EPA vehicle rows into epa_master.")
        return 0
    finally:
        try:
            os.unlink(path)
        except OSError:
            pass


def _main_induction(args: Any) -> int:
    import json

    from backend.db.inventory_pg import inventory_postgres_dsn, pg_connect

    if args.refresh_induction and args.restore:
        print("choose --refresh-induction or --restore, not both", file=sys.stderr)
        return EXIT_USAGE
    if args.refresh_induction and not args.csv:
        print("--refresh-induction needs --csv <the pinned vehicles.csv>", file=sys.stderr)
        return EXIT_USAGE
    if not inventory_postgres_dsn():
        print("set INVENTORY_DATABASE_URL to a postgresql:// DSN (the refresh is Postgres only)", file=sys.stderr)
        return EXIT_USAGE
    conn = pg_connect()
    try:
        if args.refresh_induction:
            stats = refresh_induction(
                conn, args.csv, apply=args.apply, backup_path=args.backup or None,
                progress=lambda m: print(m, file=sys.stderr, flush=True),
            )
        else:
            stats = restore_induction(conn, args.restore, apply=args.apply)
    except RefreshRefused as exc:
        print(f"refused: {exc}", file=sys.stderr)
        return EXIT_REFUSED
    finally:
        conn.close()
    print(json.dumps(stats, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
