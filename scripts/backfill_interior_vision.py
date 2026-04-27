#!/usr/bin/env python3
"""
Run Ollama LLaVA cabin-color inference for existing inventory rows (not only a live scan).

Requires a local Ollama host (``OLLAMA_HOST``) and a vision model (``OLLAMA_VISION_MODEL``,
default ``llava:13b``). Cabin image selection uses URL heuristics + LLaVA gallery
classification (see ``backend.scanner_post_pipeline.select_url_for_cabin_vision``).

By default only rows with empty/placeholder ``interior_color`` are updated (unless
``--overwrite``). Set ``INTERIOR_VISION_OVERWRITE=1`` in the environment to allow
replacing non-empty colors from vision (same as a normal scan with that env).

Usage:
  python scripts/backfill_interior_vision.py --limit 20
  python scripts/backfill_interior_vision.py --dealer-id tuttle-click-mazda --limit 100
  python scripts/backfill_interior_vision.py --vin 1HGBH41JXMN109186
  INTERIOR_VISION_FALLBACK_HERO=1 python scripts/backfill_interior_vision.py --limit 5
"""

from __future__ import annotations

import argparse
import os
import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.utils.field_clean import is_effectively_empty  # noqa: E402
from backend.utils.project_env import load_project_dotenv  # noqa: E402
from backend.scanner_post_pipeline import (  # noqa: E402
    run_interior_vision_for_vins,
)

try:
    load_project_dotenv()
except ImportError:
    pass


def _vins_to_process(
    conn: sqlite3.Connection,
    *,
    dealer_id: str | None,
    single_vin: str | None,
    limit: int | None,
    overwrite: bool,
) -> list[str]:
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    if single_vin:
        v = (single_vin or "").strip().upper()
        cur.execute(
            "SELECT vin, interior_color FROM cars WHERE UPPER(TRIM(vin)) = ? "
            "AND (COALESCE(listing_active, 1) = 1)",
            (v,),
        )
        rows = cur.fetchall()
    else:
        q = "SELECT vin, interior_color, dealer_id FROM cars WHERE (COALESCE(listing_active, 1) = 1)"
        params: list[object] = []
        if dealer_id:
            q += " AND dealer_id = ?"
            params.append(dealer_id)
        q += " ORDER BY id ASC"
        cur.execute(q, params)
        rows = cur.fetchall()

    out: list[str] = []
    for r in rows:
        ic = r["interior_color"]
        if not overwrite and not is_effectively_empty(ic):
            continue
        vin = (r["vin"] or "").strip()
        if len(vin) < 11:
            continue
        out.append(vin)
    if limit is not None and limit > 0:
        out = out[:limit]
    return out


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Backfill interior_color via LLaVA (cabin image) for existing cars rows."
    )
    ap.add_argument(
        "--db",
        type=str,
        default=os.environ.get("INVENTORY_DB_PATH", "inventory.db"),
        help="SQLite path (default: INVENTORY_DB_PATH or ./inventory.db)",
    )
    ap.add_argument("--dealer-id", type=str, default=None, help="Filter by cars.dealer_id")
    ap.add_argument("--vin", type=str, default=None, help="Process a single VIN only")
    ap.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Max VINs to process (after filters)",
    )
    ap.add_argument(
        "--overwrite",
        action="store_true",
        help="Include rows that already have interior_color (use with INTERIOR_VISION_OVERWRITE=1 to replace)",
    )
    ap.add_argument(
        "--dry-run",
        action="store_true",
        help="Print VINs that would be processed, no Ollama calls",
    )
    args = ap.parse_args()
    if args.overwrite and os.environ.get("INTERIOR_VISION_OVERWRITE", "").strip() not in (
        "1",
        "true",
        "True",
        "yes",
    ):
        print(
            "Note: --overwrite selects rows with non-empty interior_color, but vision merge "
            "only replaces when INTERIOR_VISION_OVERWRITE=1 is set.",
            file=sys.stderr,
        )
    path = os.path.abspath(args.db)
    if not os.path.isfile(path):
        print(f"Database not found: {path}", file=sys.stderr)
        return 1
    conn = sqlite3.connect(path)
    try:
        vins = _vins_to_process(
            conn,
            dealer_id=(args.dealer_id or None),
            single_vin=(args.vin or None),
            limit=args.limit,
            overwrite=bool(args.overwrite),
        )
    finally:
        conn.close()
    if not vins:
        print("No matching VINs to process.", file=sys.stderr)
        return 0
    print(f"Selected {len(vins)} VIN(s) for interior vision.")
    if args.dry_run:
        for v in vins[:50]:
            print(v)
        if len(vins) > 50:
            print(f"... and {len(vins) - 50} more")
        return 0
    os.environ["INVENTORY_DB_PATH"] = path
    summary = run_interior_vision_for_vins(vins)
    print(repr(summary))
    return 0 if summary.get("rows_applied", 0) or summary.get("vins", 0) == 0 else 0


if __name__ == "__main__":
    raise SystemExit(main())
