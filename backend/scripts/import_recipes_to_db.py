#!/usr/bin/env python3
"""
Import ``workspace/recipes/*.json`` into the ``dealer_recipes`` table.

A wrapper around ``reconcile_recipe_store --cache-dir`` on the recipe cache dir
(``RECIPES_CACHE_DIR``, else ``<repo>/workspace/recipes``); see that module for the
rules. It merges each cache file into the store per recipe and never overwrites a newer
DB set wholesale: a cache recipe older than the DB set's last write is dropped, the DB's
copy of a recipe on both sides stays unless the cache shows a newer success, and DB-only
recipes are kept. (Until P1C.2 it pushed every file whose max ``saved_at`` beat the DB
row's over that row, so a July file replaced a September set saved with ``saved_at=0``.)
The cache files that differ from the merged set are then rewritten from the DB.

Without ``--dry-run`` it applies: the cache dir is tarred and every row it will write is
exported into ``<backup dir>/recipes_reconcile_<UTC stamp>/`` first (default backup dir
``<repo>/workspace/backups``), each write is guarded, and a cache tagged for another
store (``_store.json``) is refused. Exit status: 0 done, 1 some dealer skipped or
failed (see the report), 2 refused.

  PYTHONPATH=. python backend/scripts/import_recipes_to_db.py [--dry-run] [--backup-dir DIR]
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from backend.utils.project_env import load_project_dotenv  # noqa: E402

load_project_dotenv()

from backend.scanner import recipes  # noqa: E402
from backend.scripts import reconcile_recipe_store  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Import recipe files into dealer_recipes "
                                             "(reconcile_recipe_store --cache-dir on the recipe cache dir)")
    ap.add_argument("--dry-run", action="store_true", help="report only; write nothing but the report")
    ap.add_argument("--backup-dir", default=None,
                    help=f"where the run dir goes (default {reconcile_recipe_store.DEFAULT_BACKUP_PARENT})")
    args = ap.parse_args(argv)
    backup_dir = args.backup_dir or str(reconcile_recipe_store.DEFAULT_BACKUP_PARENT)
    forwarded = ["--cache-dir", str(recipes.RECIPES_DIR), "--backup-dir", backup_dir]
    if not args.dry_run:
        forwarded.append("--apply")
    return reconcile_recipe_store.main(forwarded)


if __name__ == "__main__":
    raise SystemExit(main())
