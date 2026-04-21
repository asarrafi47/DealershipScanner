#!/usr/bin/env python3
"""
Batch-parse dealer listing descriptions into structured ``cars.packages`` JSON
(``packages_normalized``, ``dealer_description_parsed``) and optional ``interior_color``.

Re-indexing: after a bulk run, refresh listing embeddings / pgvector so hybrid search
picks up the new ``build_semantic_listing_document`` segment — e.g. run your project's
vector reindex command (see ``scripts/reindex_vectors.py`` / ``backend.vector.pgvector_service``).

Environment:
  LISTING_DESC_PARSE_USE_LLM=1  — optional second-tier extraction via local Ollama
  OLLAMA_HOST, LISTING_DESC_LLM_MODEL (defaults per ``listing_description_extract``).
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.db.inventory_db import get_conn, update_car_row_partial  # noqa: E402
from backend.utils.listing_description_extract import (  # noqa: E402
    extract_listing_description,
    normalize_listing_description,
)
from backend.utils.listing_description_persist import (  # noqa: E402
    build_row_updates_from_parse,
    listing_description_parse_is_current,
    listing_description_source_fingerprint,
    merge_description_parse_into_packages,
    packages_column_is_sparse,
)


def _iter_targets(
    conn: sqlite3.Connection,
    *,
    car_id: int | None,
    limit: int | None,
    only_sparse_packages: bool,
) -> list[dict]:
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    if car_id is not None:
        cur.execute("SELECT * FROM cars WHERE id = ?", (car_id,))
    else:
        cur.execute(
            """
            SELECT * FROM cars
            WHERE description IS NOT NULL AND TRIM(description) != ''
            ORDER BY id DESC
            """
        )
    rows = [dict(r) for r in cur.fetchall()]
    if only_sparse_packages:
        rows = [r for r in rows if packages_column_is_sparse(r.get("packages"))]
    if limit is not None:
        rows = rows[: max(0, limit)]
    return rows


def main() -> int:
    ap = argparse.ArgumentParser(description="Parse listing descriptions into packages JSON.")
    ap.add_argument("--dry-run", action="store_true", help="Parse and log only; no DB writes.")
    ap.add_argument("--limit", type=int, default=None, help="Max rows to process.")
    ap.add_argument("--car-id", type=int, default=None, help="Single car id.")
    ap.add_argument(
        "--skip-fresh",
        action="store_true",
        help="Skip when packages JSON already has this parser_version and matching description hash.",
    )
    ap.add_argument(
        "--all-packages-rows",
        action="store_true",
        help="Include rows even when packages JSON already has features (default: only sparse/empty).",
    )
    ap.add_argument("--force", action="store_true", help="Ignore skip-fresh.")
    args = ap.parse_args()
    only_sparse = not bool(args.all_packages_rows)
    conn = get_conn()
    try:
        targets = _iter_targets(conn, car_id=args.car_id, limit=args.limit, only_sparse_packages=only_sparse)
    finally:
        conn.close()

    n_done = 0
    for row in targets:
        desc = row.get("description") or ""
        norm = normalize_listing_description(desc)
        if len(norm) < 20:
            continue
        fp = listing_description_source_fingerprint(norm)
        if args.skip_fresh and listing_description_parse_is_current(
            row.get("packages"), source_fingerprint=fp, force=args.force
        ):
            continue
        ctx = {
            "make": row.get("make"),
            "model": row.get("model"),
            "year": row.get("year"),
            "trim": row.get("trim"),
            "vin": row.get("vin"),
        }
        parsed = extract_listing_description(desc, ctx)
        merged = merge_description_parse_into_packages(row.get("packages"), parsed, source_fingerprint=fp)
        updates = build_row_updates_from_parse(row, parsed, merged_packages_json=merged, source_fingerprint=fp)
        if args.dry_run:
            print(f"DRY id={row.get('id')} keys={list(updates.keys())} packages_len={len(merged)}")
        else:
            update_car_row_partial(int(row["id"]), updates)
        n_done += 1
    print(f"Processed {n_done} row(s).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
