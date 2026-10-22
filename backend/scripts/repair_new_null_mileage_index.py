#!/usr/bin/env python3
"""
Targeted repair of the incomplete-listings index for the F12 class (N2.2, IL-02).

    .venv/bin/python -m backend.scripts.repair_new_null_mileage_index              # dry run
    .venv/bin/python -m backend.scripts.repair_new_null_mileage_index --apply \\
        --backup-file workspace/backups/incomplete_newnull_<stamp>.csv --expect-db <db>

Before N2.1, ``_mileage_blank`` flagged a NULL odometer on every car, so the index
(``incomplete_listings``) carries a ``mileage`` code on active New and
blank-condition cars that have no odometer value (21,152 in the 10-13 clean-room
scan, 28,983 in the 10-04 prod dump). Production hides every indexed car
(``data_quality_repo.py``), so those cars are invisible. N2.1 (owner decision D-N12)
changed the rule; this script repairs only the rows that rule changed. A full index
rebuild would also re-judge every other code on every car and mix unrelated changes
into the same release.

Selection, in batches of ``--batch-size`` (default 1,000) index rows:

* index rows whose ``missing_fields_json`` contains the ``mileage`` code, read in
  ``car_id`` order (keyset pages, so every batch is a short transaction);
* whose car exists, is active (``COALESCE(listing_active, 1) = 1``) and has a NULL or
  empty odometer;
* and for which the N2.1 rule itself, :func:`backend.utils.listing_completeness.
  _mileage_blank` (imported, not copied), no longer reports the mileage gap. On a NULL
  odometer that is every car that is not used or certified by its raw condition or
  ``is_cpo``: New, blank, and other non-used values such as ``ctp`` or ``demo``. A
  used or CPO car keeps its code and is counted as ``kept_used_or_cpo``.

Change, per selected row: the ``mileage`` code is removed and every other stored code
is kept as it is. A row left with no codes is deleted (the car becomes visible); any
other row is updated with the remaining codes and a fresh ``updated_at``. Both writes
compare-and-set on the ``missing_fields_json`` text that was read, so a row changed by
another writer in between is left alone and counted as ``changed_concurrently``.
Stale flags on the other codes are N6.1's index rebuild, not this repair.

``--dry-run`` is the default. It opens the session read-only (Postgres
``SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY``, SQLite ``PRAGMA
query_only``) and prints the counts; ``repair_rows`` is the number of index rows the
apply would change (IL-02 under the N2.1 rule). ``census_il02_shape`` counts the same
rows the way the N1.6 census SQL does (raw condition ``new`` or blank, NULL odometer,
``is_cpo`` ignored) so the dry run can be matched to the census number line by line.

``--apply`` needs ``--backup-file PATH`` (a new file; an existing one is never
overwritten) and, on Postgres, ``--expect-db NAME`` equal to the connected
``current_database()``. Before each batch is written, the original index rows of
that batch are appended to the backup as CSV (``car_id,vin,missing_fields_json,
updated_at``, header first, flushed and fsynced). After the last batch a read-only
pass recounts the class; ``remaining_after_apply`` must be 0.

Restore from a backup (Postgres)::

    CREATE TEMP TABLE il_restore (LIKE incomplete_listings);
    \\copy il_restore FROM '<backup.csv>' WITH (FORMAT csv, HEADER true)
    INSERT INTO incomplete_listings SELECT * FROM il_restore
      ON CONFLICT (car_id) DO UPDATE SET vin = EXCLUDED.vin,
        missing_fields_json = EXCLUDED.missing_fields_json, updated_at = EXCLUDED.updated_at;

The scanner must not run during an apply (prod: ``SCAN_FLEET=0``). The web picks the
change up by itself: on Postgres the listings cache token is a 60-second bucket, and
the grid card store re-serializes a card whose public-incomplete flag changed.

Target: ``INVENTORY_DATABASE_URL``, else ``DATABASE_URL`` (the backend's own
precedence). The script prints which variable it used, the database name and whether
the server is local, never the host or credentials. Exit codes: 0 done, 2 refused
(arguments, target or backup file), 1 error.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable, Sequence

_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from backend.db import incomplete_listings_db, inventory_db, inventory_pg  # noqa: E402
from backend.utils.listing_completeness import _mileage_blank  # noqa: E402  (the N2.1 rule)

BATCH_SIZE = 1000
BATCH_SECONDS_BUDGET = 30.0
MILEAGE_CODE = "mileage"
BACKUP_COLUMNS = ("car_id", "vin", "missing_fields_json", "updated_at")
EXIT_OK, EXIT_ERROR, EXIT_REFUSED = 0, 1, 2

# Bound parameter, never a literal: psycopg would read a bare % in SQL text as a placeholder.
_MILEAGE_LIKE = '%"' + MILEAGE_CODE + '"%'
_IN_CHUNK = 500  # ids per IN (...) list, well under SQLite's variable limit

_PAGE_SQL = (
    "SELECT car_id, vin, missing_fields_json, updated_at FROM incomplete_listings "
    "WHERE car_id > ? AND missing_fields_json LIKE ? ORDER BY car_id LIMIT ?"
)
_UPDATE_SQL = (
    "UPDATE incomplete_listings SET missing_fields_json = ?, updated_at = ? "
    "WHERE car_id = ? AND missing_fields_json = ?"
)
_DELETE_SQL = "DELETE FROM incomplete_listings WHERE car_id = ? AND missing_fields_json = ?"
_CLASS_SQL = (
    "SELECT id, condition, is_cpo, mileage FROM cars "
    "WHERE COALESCE(listing_active, 1) = 1 "
    "AND (mileage IS NULL OR TRIM(CAST(mileage AS TEXT)) = '')"
)

# classify() verdicts
REPAIR = "repair"
KEPT_USED_OR_CPO = "kept_used_or_cpo"
SKIP_CAR_MISSING = "skipped_car_missing"
SKIP_CAR_INACTIVE = "skipped_car_inactive"
SKIP_HAS_ODOMETER = "skipped_has_odometer"
SKIP_BAD_JSON = "skipped_bad_json"
SKIP_NO_MILEAGE_CODE = "skipped_no_mileage_code"
VERDICTS = (
    REPAIR,
    KEPT_USED_OR_CPO,
    SKIP_CAR_MISSING,
    SKIP_CAR_INACTIVE,
    SKIP_HAS_ODOMETER,
    SKIP_BAD_JSON,
    SKIP_NO_MILEAGE_CODE,
)


class Refused(Exception):
    """A precondition failed before anything was written (exit 2)."""


# ---------------------------------------------------------------------------
# pure helpers
# ---------------------------------------------------------------------------


def odometer_blank(value: Any) -> bool:
    """NULL or empty odometer: the inputs N2.1 changed (D-N12). Whether such a car is
    still missing mileage is decided by the rule, ``_mileage_blank``."""
    return value is None or (isinstance(value, str) and not value.strip())


def is_active(listing_active: Any) -> bool:
    """``COALESCE(listing_active, 1) = 1``, the filter every public query and the index use."""
    if listing_active is None:
        return True
    try:
        return int(listing_active) == 1
    except (TypeError, ValueError):
        return False


def parse_codes(raw: Any) -> list[str] | None:
    """The stored code list, or None when it is not a JSON array of strings."""
    try:
        value = json.loads(raw) if isinstance(raw, (str, bytes)) else raw
    except (TypeError, ValueError):
        return None
    if not isinstance(value, list) or not all(isinstance(c, str) for c in value):
        return None
    return value


def condition_key(condition: Any) -> str:
    return str(condition or "").strip().lower() or "(blank)"


def classify(codes_json: Any, car: dict[str, Any] | None) -> tuple[str, list[str] | None]:
    """Verdict for one index row coded ``mileage`` and its car, plus the codes to store
    when the verdict is :data:`REPAIR` (an empty list means: delete the index row)."""
    if car is None:
        return SKIP_CAR_MISSING, None
    if not is_active(car.get("listing_active")):
        return SKIP_CAR_INACTIVE, None
    if not odometer_blank(car.get("mileage")):
        return SKIP_HAS_ODOMETER, None
    codes = parse_codes(codes_json)
    if codes is None:
        return SKIP_BAD_JSON, None
    if MILEAGE_CODE not in codes:
        return SKIP_NO_MILEAGE_CODE, None
    if _mileage_blank(car):
        return KEPT_USED_OR_CPO, None
    return REPAIR, [c for c in codes if c != MILEAGE_CODE]


def in_census_il02_shape(car: dict[str, Any]) -> bool:
    """The N1.6 census C2 filter on the car side: NULL odometer, raw condition new or blank."""
    return car.get("mileage") is None and condition_key(car.get("condition")) in ("new", "(blank)")


def check_target(*, is_postgres: bool, current_db: str | None, expect_db: str | None, apply: bool) -> None:
    """Refuse an apply on Postgres without ``--expect-db``, and any run whose
    ``--expect-db`` does not name the connected database."""
    if not is_postgres:
        return
    if apply and not expect_db:
        raise Refused("--apply on Postgres needs --expect-db <name of the target database>")
    if expect_db and expect_db != current_db:
        raise Refused(f"connected database is {current_db!r}, --expect-db says {expect_db!r}; nothing written")


def open_backup(path: str | os.PathLike[str]):
    """Create the backup file (never overwrite) and write the CSV header."""
    p = Path(path)
    if p.exists():
        raise Refused(f"backup file already exists, refusing to overwrite: {p}")
    if not p.parent.is_dir():
        raise Refused(f"backup directory does not exist: {p.parent}")
    fh = open(p, "x", newline="", encoding="utf-8")
    csv.writer(fh).writerow(BACKUP_COLUMNS)
    fh.flush()
    os.fsync(fh.fileno())
    return fh


# ---------------------------------------------------------------------------
# database access
# ---------------------------------------------------------------------------


def describe_target() -> dict[str, Any]:
    """Backend, source variable, local/remote; never the host or credentials."""
    if not inventory_pg.is_inventory_postgres():
        return {
            "backend": "sqlite",
            "inventory": str(inventory_db._resolve_db_path()),
            "index": str(incomplete_listings_db.DB_PATH),
        }
    source = "INVENTORY_DATABASE_URL" if (os.environ.get("INVENTORY_DATABASE_URL") or "").strip() else "DATABASE_URL"
    host = ""
    try:
        from psycopg.conninfo import conninfo_to_dict

        host = str(conninfo_to_dict(inventory_pg.inventory_postgres_dsn() or "").get("host") or "")
    except Exception:
        host = "?"
    local = host == "" or host.startswith("/") or host in ("localhost", "127.0.0.1", "::1")
    return {"backend": "postgres", "source": source, "server": "local" if local else "remote"}


def _session_setup(conn: Any, *, is_postgres: bool, read_only: bool) -> None:
    if is_postgres:
        if read_only:
            conn.execute("SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY")
        else:
            conn.execute("SET lock_timeout = '5s'")
        conn.commit()
    elif read_only:
        conn.execute("PRAGMA query_only = ON")


def _fetch_cars(inv: Any, ids: Sequence[int]) -> dict[int, dict[str, Any]]:
    out: dict[int, dict[str, Any]] = {}
    for i in range(0, len(ids), _IN_CHUNK):
        chunk = list(ids[i : i + _IN_CHUNK])
        marks = ", ".join("?" for _ in chunk)
        rows = inv.execute(
            f"SELECT id, condition, is_cpo, mileage, listing_active FROM cars WHERE id IN ({marks})",
            chunk,
        ).fetchall()
        for r in rows:
            out[int(r[0])] = {
                "id": int(r[0]),
                "condition": r[1],
                "is_cpo": r[2],
                "mileage": r[3],
                "listing_active": r[4],
            }
    return out


def _class_cars_active(inv: Any) -> int:
    """Every active car in the class, indexed or not (the plan's class count)."""
    n = 0
    for r in inv.execute(_CLASS_SQL).fetchall():
        car = {"id": r[0], "condition": r[1], "is_cpo": r[2], "mileage": r[3]}
        if odometer_blank(car["mileage"]) and not _mileage_blank(car):
            n += 1
    return n


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def scan(
    inv: Any,
    idx: Any,
    *,
    batch_size: int = BATCH_SIZE,
    apply: bool = False,
    backup: Any = None,
    log: Callable[[str], None] = print,
) -> dict[str, Any]:
    """One keyset pass over the index rows coded ``mileage``; writes only when *apply*."""
    if batch_size < 1:
        raise Refused("--batch-size must be at least 1")
    if apply and backup is None:
        raise Refused("--apply needs a backup file")
    verdicts: Counter[str] = Counter()
    by_condition: Counter[str] = Counter()
    repair_delete = repair_update = census_shape = 0
    applied_deleted = applied_updated = changed = backup_rows = 0
    batch_seconds: list[float] = []
    writer = csv.writer(backup) if backup is not None else None
    last_id = -1
    while True:
        t0 = time.monotonic()
        page = idx.execute(_PAGE_SQL, (last_id, _MILEAGE_LIKE, int(batch_size))).fetchall()
        if not page:
            idx.rollback()
            break
        last_id = int(page[-1][0])
        cars = _fetch_cars(inv, [int(r[0]) for r in page])
        inv.rollback()  # read-only; release the snapshot between batches
        updates: list[tuple[Any, ...]] = []
        deletes: list[tuple[Any, ...]] = []
        originals: list[tuple[Any, ...]] = []
        now = _now_iso()
        for car_id, vin, codes_json, updated_at in page:
            car = cars.get(int(car_id))
            verdict, new_codes = classify(codes_json, car)
            verdicts[verdict] += 1
            if car is not None and verdict in (REPAIR, KEPT_USED_OR_CPO) and in_census_il02_shape(car):
                census_shape += 1
            if verdict != REPAIR:
                continue
            by_condition[condition_key(car.get("condition"))] += 1
            originals.append((car_id, vin, codes_json, updated_at))
            if new_codes:
                repair_update += 1
                updates.append((json.dumps(new_codes), now, car_id, codes_json))
            else:
                repair_delete += 1
                deletes.append((car_id, codes_json))
        if apply and originals:
            writer.writerows(originals)
            backup.flush()
            os.fsync(backup.fileno())
            backup_rows += len(originals)
            try:
                cur = idx.cursor()
                n_upd = n_del = 0
                if updates:
                    cur.executemany(_UPDATE_SQL, updates)
                    n_upd = max(0, int(cur.rowcount or 0))
                if deletes:
                    cur.executemany(_DELETE_SQL, deletes)
                    n_del = max(0, int(cur.rowcount or 0))
                idx.commit()
            except Exception:
                idx.rollback()
                raise
            applied_updated += n_upd
            applied_deleted += n_del
            changed += len(updates) + len(deletes) - n_upd - n_del
        else:
            idx.rollback()
        secs = round(time.monotonic() - t0, 3)
        batch_seconds.append(secs)
        log(
            f"batch {len(batch_seconds)}: {len(page)} index rows coded mileage up to car_id {last_id}, "
            f"{len(originals)} to repair, {secs:.2f}s"
        )
    out: dict[str, Any] = {
        "scanned_index_rows_coded_mileage": sum(verdicts.values()),
        "repair_rows": verdicts[REPAIR],
        "repair_delete_index_row": repair_delete,
        "repair_update_keep_other_codes": repair_update,
        "repair_by_raw_condition": dict(by_condition.most_common()),
        "census_il02_shape": census_shape,
        **{v: verdicts[v] for v in VERDICTS if v != REPAIR},
        "batches": len(batch_seconds),
        "batch_size": int(batch_size),
        "batch_seconds_max": max(batch_seconds) if batch_seconds else 0.0,
        "batches_over_budget": sum(1 for s in batch_seconds if s > BATCH_SECONDS_BUDGET),
    }
    if apply:
        out.update(
            applied_deleted=applied_deleted,
            applied_updated=applied_updated,
            changed_concurrently=changed,
            backup_rows=backup_rows,
        )
    return out


def _count_index_rows(idx: Any) -> int:
    row = idx.execute("SELECT COUNT(*) FROM incomplete_listings").fetchone()
    idx.rollback()
    return int(row[0]) if row else 0


def _require_writable(idx: Any, is_postgres: bool) -> None:
    """An apply on a read-only session (``PGOPTIONS`` left over from a census, a standby)
    would back up the first batch and then fail; refuse before the backup is created."""
    if not is_postgres:
        return
    row = idx.execute("SHOW transaction_read_only").fetchone()
    idx.rollback()
    if row and str(row[0]).lower() == "on":
        raise Refused("--apply on a read-only session (check PGOPTIONS); nothing written")


def _current_db(inv: Any, is_postgres: bool) -> str | None:
    if not is_postgres:
        return None
    row = inv.execute("SELECT current_database()").fetchone()
    inv.rollback()
    return str(row[0]) if row else None


def run(
    *,
    apply: bool = False,
    backup_file: str | None = None,
    expect_db: str | None = None,
    batch_size: int = BATCH_SIZE,
    log: Callable[[str], None] = print,
) -> dict[str, Any]:
    """Dry run (default) or apply. Raises :class:`Refused` before any write when a
    precondition fails."""
    if apply and not backup_file:
        raise Refused("--apply needs --backup-file <new file> (the original index rows are written there first)")
    if batch_size < 1:
        raise Refused("--batch-size must be at least 1")
    is_pg = inventory_pg.is_inventory_postgres()
    target = describe_target()
    inv = inventory_db.get_conn()
    idx = None
    backup = None
    try:
        _session_setup(inv, is_postgres=is_pg, read_only=True)
        current_db = _current_db(inv, is_pg)
        target["db"] = current_db
        log("target: " + " ".join(f"{k}={v}" for k, v in target.items()))
        check_target(is_postgres=is_pg, current_db=current_db, expect_db=expect_db, apply=apply)
        idx = incomplete_listings_db.get_conn()
        _session_setup(idx, is_postgres=is_pg, read_only=not apply)
        try:
            index_rows_before = _count_index_rows(idx)
        except Exception as exc:  # no table: there is nothing to repair, and no DDL here
            raise Refused(f"incomplete_listings is not readable ({type(exc).__name__}); nothing written") from exc
        class_cars = _class_cars_active(inv)
        inv.rollback()
        if apply:
            _require_writable(idx, is_pg)
            backup = open_backup(backup_file)
        t0 = time.monotonic()
        result = scan(inv, idx, batch_size=batch_size, apply=apply, backup=backup, log=log)
        summary: dict[str, Any] = {
            "mode": "apply" if apply else "dry_run",
            "target": target,
            "index_rows_total": index_rows_before,
            "class_cars_active": class_cars,
            **result,
            "seconds": round(time.monotonic() - t0, 2),
        }
        if apply:
            backup.close()
            backup = None
            summary["backup_file"] = str(backup_file)
            _session_setup(idx, is_postgres=is_pg, read_only=True)
            after = scan(inv, idx, batch_size=batch_size, apply=False, log=lambda _s: None)
            summary["remaining_after_apply"] = after["repair_rows"]
            summary["index_rows_total_after"] = _count_index_rows(idx)
        return summary
    finally:
        if backup is not None:
            backup.close()
        for conn in (idx, inv):
            if conn is not None:
                try:
                    conn.close()
                except Exception:
                    pass


def _print_summary(summary: dict[str, Any], log: Callable[[str], None]) -> None:
    log(f"mode: {summary['mode']}")
    log(f"incomplete_listings rows: {summary['index_rows_total']}")
    log(f"class cars (active, not used/CPO, NULL or empty odometer; indexed or not): {summary['class_cars_active']}")
    log(f"index rows coded mileage, scanned: {summary['scanned_index_rows_coded_mileage']}")
    log(f"repair_rows (IL-02 under the N2.1 rule): {summary['repair_rows']}")
    log(f"  index row deleted, mileage was its only code (car becomes visible): {summary['repair_delete_index_row']}")
    log(f"  mileage code removed, other codes kept: {summary['repair_update_keep_other_codes']}")
    log(f"  by raw condition: {json.dumps(summary['repair_by_raw_condition'])}")
    log(f"census_il02_shape (N1.6 C2 filter: condition new/blank, NULL odometer): {summary['census_il02_shape']}")
    log(f"kept (used or CPO, the rule still flags a NULL odometer): {summary[KEPT_USED_OR_CPO]}")
    log(
        "skipped: "
        f"car missing {summary[SKIP_CAR_MISSING]}, car inactive {summary[SKIP_CAR_INACTIVE]}, "
        f"odometer present {summary[SKIP_HAS_ODOMETER]}, bad JSON {summary[SKIP_BAD_JSON]}, "
        f"no mileage code {summary[SKIP_NO_MILEAGE_CODE]}"
    )
    log(
        f"batches: {summary['batches']} of up to {summary['batch_size']}, slowest {summary['batch_seconds_max']:.2f}s, "
        f"over {BATCH_SECONDS_BUDGET:.0f}s: {summary['batches_over_budget']}"
    )
    if summary["mode"] == "apply":
        log(
            f"applied: {summary['applied_deleted']} deleted, {summary['applied_updated']} updated, "
            f"{summary['changed_concurrently']} changed concurrently (left as found)"
        )
        log(f"backup: {summary['backup_rows']} rows in {summary['backup_file']}")
        log(f"remaining_after_apply: {summary['remaining_after_apply']} (must be 0)")
        log(f"incomplete_listings rows after: {summary['index_rows_total_after']}")
    else:
        log("DRY RUN: nothing written. Apply with --apply --backup-file <new file> [--expect-db <db>].")
    log("summary: " + json.dumps(summary, sort_keys=True, default=str))


def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(
        prog="repair_new_null_mileage_index",
        description="Remove the stale `mileage` code from incomplete_listings rows of active "
        "New / blank-condition cars with no odometer value (N2.1 rule, D-N12).",
    )
    mode = ap.add_mutually_exclusive_group()
    mode.add_argument("--dry-run", action="store_true", help="count only, read-only session (default)")
    mode.add_argument("--apply", action="store_true", help="write the repair (needs --backup-file)")
    ap.add_argument("--backup-file", help="new CSV file that receives the original index rows before each batch")
    ap.add_argument("--expect-db", help="name of the target database; required with --apply on Postgres")
    ap.add_argument("--batch-size", type=int, default=BATCH_SIZE, help=f"index rows per batch (default {BATCH_SIZE})")
    return ap


def main(argv: Iterable[str] | None = None, *, log: Callable[[str], None] = print) -> int:
    args = build_parser().parse_args(list(argv) if argv is not None else None)
    try:
        summary = run(
            apply=bool(args.apply),
            backup_file=args.backup_file,
            expect_db=args.expect_db,
            batch_size=args.batch_size,
            log=log,
        )
    except Refused as exc:
        log(f"REFUSED: {exc}")
        return EXIT_REFUSED
    _print_summary(summary, log)
    if summary["mode"] == "apply" and summary.get("remaining_after_apply"):
        return EXIT_ERROR
    return EXIT_OK


if __name__ == "__main__":
    raise SystemExit(main())
