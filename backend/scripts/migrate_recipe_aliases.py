"""
Migrate orphaned scan recipes to their dealers' current identities.

    .venv/bin/python -m backend.scripts.migrate_recipe_aliases            # dry-run (default)
    .venv/bin/python -m backend.scripts.migrate_recipe_aliases --apply    # move files + update DB

Dealer identity is a pure function of the site URL (``dev.dealers.slug_from_url``),
so a hostname change mints a NEW dealer_id and strands the recipe file, the
``dealer_recipes`` row, and any ``scan_hints`` saved under the old one.
``workspace/recipes/_aliases.json`` ({old_slug: current_dealer_id}) keeps those
readable in the meantime (see ``backend.scanner.recipes``); this script makes the
migration physical so the alias entries can eventually be retired:

  - for each alias entry, if the OLD recipe file exists and the CURRENT one does
    not, MOVE the file to the current slug (rewriting the embedded ``dealer_id``
    field in each recipe row);
  - update the ``dealer_recipes`` row's key old -> new (recipes + scan_hints live
    on the same row), unless a row already exists under the new key — then the
    old row is left alone and reported, never merged or deleted.

Dry-run is the default and prints every action it WOULD take; nothing is written
without ``--apply``. Idempotent: entries already migrated are reported as no-ops.
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from backend.db.connect import connect as db_connect  # noqa: E402

_log = logging.getLogger("migrate_recipe_aliases")

RECIPES_DIR = _REPO_ROOT / "workspace" / "recipes"
ALIASES_PATH = RECIPES_DIR / "_aliases.json"


def _slug(dealer_id: str) -> str:
    """Must match backend.scanner.recipes._recipe_slug."""
    return re.sub(r"[^a-z0-9_-]+", "-", (dealer_id or "unknown").lower()).strip("-") or "unknown"


def _load_aliases() -> dict[str, str]:
    try:
        raw = json.loads(ALIASES_PATH.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise SystemExit(f"cannot read {ALIASES_PATH}: {exc}")
    if not isinstance(raw, dict):
        raise SystemExit(f"{ALIASES_PATH} is not a JSON object")
    return {
        _slug(k): str(v).strip()
        for k, v in raw.items()
        if isinstance(k, str) and isinstance(v, str) and k.strip() and v.strip()
    }


def _move_file(old_path: Path, new_path: Path, new_id: str, *, apply: bool) -> bool:
    """Move old recipe file to the current slug, rewriting embedded dealer_id."""
    try:
        rows = json.loads(old_path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        _log.warning("  file: %s unreadable (%s) — skipped", old_path.name, exc)
        return False
    if isinstance(rows, list):
        for r in rows:
            if isinstance(r, dict) and "dealer_id" in r:
                r["dealer_id"] = new_id
    if not apply:
        _log.info("  file: would move %s -> %s", old_path.name, new_path.name)
        return True
    new_path.write_text(json.dumps(rows, indent=1), encoding="utf-8")
    old_path.unlink()
    _log.info("  file: moved %s -> %s", old_path.name, new_path.name)
    return True


def _migrate_db_row(cur, old_slug: str, new_slug: str, *, apply: bool) -> str:
    """Re-key the dealer_recipes row (recipes_json + scan_hints). Returns outcome."""
    cur.execute("SELECT 1 FROM dealer_recipes WHERE dealer_id = %s", (old_slug,))
    if not cur.fetchone():
        return "no_old_row"
    cur.execute("SELECT 1 FROM dealer_recipes WHERE dealer_id = %s", (new_slug,))
    if cur.fetchone():
        # Never merge or clobber: the new identity already has its own row
        # (a scan under the new id ran since). Leave the old row for a human.
        return "new_row_exists"
    if not apply:
        return "would_update"
    cur.execute(
        "UPDATE dealer_recipes SET dealer_id = %s WHERE dealer_id = %s",
        (new_slug, old_slug),
    )
    return "updated"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--apply", action="store_true",
                    help="perform the file moves and DB updates (default: dry-run)")
    args = ap.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    aliases = _load_aliases()
    if not aliases:
        _log.info("no alias entries in %s — nothing to do", ALIASES_PATH)
        return 0
    _log.info("%d alias entr%s%s", len(aliases), "y" if len(aliases) == 1 else "ies",
              "" if args.apply else " (DRY-RUN — pass --apply to write)")

    conn = db_connect()
    conn.autocommit = False
    cur = conn.cursor()

    moved = db_updated = skipped = 0
    try:
        for old_slug, new_id in sorted(aliases.items()):
            new_slug = _slug(new_id)
            _log.info("%s -> %s", old_slug, new_slug)
            if old_slug == new_slug:
                _log.warning("  self-referential entry — skipped")
                skipped += 1
                continue

            old_path = RECIPES_DIR / f"{old_slug}.json"
            new_path = RECIPES_DIR / f"{new_slug}.json"
            if not old_path.exists():
                _log.info("  file: %s absent — nothing to move", old_path.name)
            elif new_path.exists():
                _log.info("  file: %s already exists — old file left in place", new_path.name)
            elif _move_file(old_path, new_path, new_id, apply=args.apply):
                moved += 1

            outcome = _migrate_db_row(cur, old_slug, new_slug, apply=args.apply)
            if outcome in ("updated", "would_update"):
                db_updated += 1
                _log.info("  db:   dealer_recipes %s -> %s (%s)", old_slug, new_slug, outcome)
            elif outcome == "new_row_exists":
                _log.warning("  db:   row already exists under %s — old row left for review", new_slug)
            else:
                _log.info("  db:   no row under %s — nothing to update", old_slug)

        if args.apply:
            conn.commit()
        else:
            conn.rollback()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    _log.info("done%s: %d file(s) moved, %d DB row(s) re-keyed, %d skipped",
              "" if args.apply else " (dry-run)", moved, db_updated, skipped)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
