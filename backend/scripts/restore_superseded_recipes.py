"""Restore a recipe that a re-synthesis archived (N13 P7.5).

``replace_recipes`` moves the recipes it replaces into ``dealer_recipes.superseded_json``
for ``SUPERSEDED_KEEP_DAYS`` days. This lists a dealer's archive and, with ``--apply``,
brings one entry back into the live set (un-stale; a live recipe with the same key is
archived in its place). Default is a dry run.

    python -m backend.scripts.restore_superseded_recipes <dealer_id>
    python -m backend.scripts.restore_superseded_recipes <dealer_id> --index 0 --apply
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("dealer_id")
    ap.add_argument("--index", type=int, help="entry to restore, as numbered in the listing")
    ap.add_argument("--apply", action="store_true", help="write (default: dry run)")
    args = ap.parse_args(argv)

    from backend.scanner import recipe_store
    from backend.scanner.recipes import _recipe_slug, restore_superseded

    row = recipe_store.db_read(_recipe_slug(args.dealer_id))
    entries = sorted(row.superseded if row else [], key=lambda e: -float(e.get("superseded_at") or 0.0))
    if not entries:
        print(f"{args.dealer_id}: no archived recipes")
        return 0
    for i, e in enumerate(entries):
        when = datetime.fromtimestamp(float(e.get("superseded_at") or 0.0), timezone.utc).isoformat(timespec="seconds")
        print(f"[{i}] {when} {e.get('reason')} {' '.join(e.get('key') or [])}")
    if args.index is None:
        return 0
    if not 0 <= args.index < len(entries):
        print("no such entry", file=sys.stderr)
        return 2
    pick = entries[args.index]
    if not args.apply:
        print("dry run: nothing restored (pass --apply)")
        return 0
    ok = restore_superseded(args.dealer_id, tuple(pick["key"]), superseded_at=float(pick["superseded_at"]))
    print("restored" if ok else "not restored")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
