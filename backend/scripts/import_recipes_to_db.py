#!/usr/bin/env python3
"""
Backfill ``workspace/recipes/*.json`` into the ``dealer_recipes`` table.

Idempotent: a dealer's DB row is only overwritten when the file is fresher
(max ``saved_at``). Safe to re-run any time — e.g. after a scan that ran with
pre-DB code finishes writing recipe files.

  PYTHONPATH=. python backend/scripts/import_recipes_to_db.py [--dry-run]
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_REPO_ROOT))

from backend.utils.project_env import load_project_dotenv

load_project_dotenv()

from backend.scanner.recipe_store import db_load_recipes, db_save_recipes  # noqa: E402
from backend.scanner.recipes import RECIPES_DIR  # noqa: E402


def main() -> None:
    ap = argparse.ArgumentParser(description="Import recipe files into dealer_recipes")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    files = sorted(RECIPES_DIR.glob("*.json")) if RECIPES_DIR.is_dir() else []
    stats = {"files": len(files), "imported": 0, "kept_db": 0, "unparsable": 0}
    for path in files:
        try:
            rows = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            stats["unparsable"] += 1
            continue
        if not isinstance(rows, list) or not rows:
            stats["unparsable"] += 1
            continue
        dealer_id = path.stem
        file_saved = max(float(r.get("saved_at") or 0) for r in rows if isinstance(r, dict))
        existing = db_load_recipes(dealer_id)
        if existing is not None and existing[1] >= file_saved:
            stats["kept_db"] += 1
            continue
        stats["imported"] += 1
        if not args.dry_run:
            db_save_recipes(dealer_id, rows)
    print(stats, flush=True)


if __name__ == "__main__":
    main()
