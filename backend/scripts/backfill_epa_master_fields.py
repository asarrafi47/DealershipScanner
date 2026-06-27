#!/usr/bin/env python3
"""
Backfill ``engine_display`` and ``forced_induction`` on ``epa_master`` rows.

Dictionary EPA CSVs include (or derive) these fields, but ``build_epa_master`` originally
skipped them. This script recomputes them from each row's engine/trim/displacement data
via ``catalog_engine_fields`` (same logic as ``augment_dictionary_engines.py``).

Usage:
  PYTHONPATH=. python -m backend.scripts.backfill_epa_master_fields
  PYTHONPATH=. python -m backend.scripts.backfill_epa_master_fields --dry-run
"""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(_REPO_ROOT))

from backend.dictionary.epa_engine import catalog_engine_fields

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("backfill_epa_master_fields")


def _row_to_catalog_dict(row: tuple) -> dict[str, str]:
    (
        _id,
        year,
        make,
        model,
        trim,
        engine_description,
        cylinders,
        displacement,
        fuel_type,
        drive,
        trany,
    ) = row
    return {
        "Year": str(year or ""),
        "Make": make or "",
        "Model": model or "",
        "Trim": trim or "",
        "engineOptions": engine_description or "",
        "cylinders": str(cylinders) if cylinders is not None else "",
        "displacement": str(displacement) if displacement is not None else "",
        "fuelType": fuel_type or "",
        "drivetrainOptions": drive or "",
        "transmissionOptions": trany or "",
    }


def backfill(*, dry_run: bool = False) -> dict[str, int]:
    from backend.db.inventory_db import get_conn

    stats = {"examined": 0, "updated": 0, "engine_display": 0, "forced_induction": 0}
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT id, year, make, model, trim, engine_description,
                   cylinders, displacement, fuel_type, drive, trany
            FROM epa_master
            ORDER BY id
            """
        )
        rows = cur.fetchall()
        stats["examined"] = len(rows)

        pending: list[tuple[str | None, str | None, int]] = []
        for row in rows:
            rid = row[0] if not isinstance(row, dict) else row["id"]
            catalog = catalog_engine_fields(_row_to_catalog_dict(tuple(row)))
            eng_disp = (catalog.get("engineDisplay") or "").strip() or None
            forced = (catalog.get("forcedInduction") or "").strip() or None
            if not eng_disp and not forced:
                continue
            if eng_disp:
                stats["engine_display"] += 1
            if forced:
                stats["forced_induction"] += 1
            pending.append((eng_disp, forced, int(rid)))
            if len(pending) >= 1000:
                if not dry_run:
                    cur.executemany(
                        """
                        UPDATE epa_master
                        SET engine_display = ?,
                            forced_induction = ?
                        WHERE id = ?
                        """,
                        pending,
                    )
                    conn.commit()
                stats["updated"] += len(pending)
                pending.clear()

        if pending:
            if not dry_run:
                cur.executemany(
                    """
                    UPDATE epa_master
                    SET engine_display = COALESCE(?, engine_display),
                        forced_induction = COALESCE(?, forced_induction)
                    WHERE id = ?
                    """,
                    pending,
                )
                conn.commit()
            stats["updated"] += len(pending)
    finally:
        conn.close()
    return stats


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true", help="Report counts only")
    args = parser.parse_args(argv)

    stats = backfill(dry_run=args.dry_run)
    prefix = "Would update" if args.dry_run else "Updated"
    log.info(
        "%s %d/%d rows (engine_display=%d, forced_induction=%d)",
        prefix,
        stats["updated"],
        stats["examined"],
        stats["engine_display"],
        stats["forced_induction"],
    )


if __name__ == "__main__":
    main()
