#!/usr/bin/env python3
"""
Rebuild pgvector embedding tables from SQLite (inventory + BMW OEM).

Usage (from project root):
  python scripts/reindex_vectors.py
  python scripts/reindex_vectors.py --inventory-only

Environment:
  INVENTORY_DB_PATH — defaults to ./inventory.db
  PGVECTOR_URL or DATABASE_URL — Postgres with ``CREATE EXTENSION vector;``
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
os.chdir(ROOT)


def main() -> int:
    p = argparse.ArgumentParser(description="Reindex pgvector embedding tables.")
    p.add_argument(
        "--inventory-only",
        action="store_true",
        help="Only rebuild listing + dealer embeddings (skip BMW OEM tables).",
    )
    args = p.parse_args()

    from backend.vector.pgvector_service import reindex_all, reindex_inventory_only

    counts = reindex_inventory_only() if args.inventory_only else reindex_all()
    print(json.dumps({"ok": True, "counts": counts}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
