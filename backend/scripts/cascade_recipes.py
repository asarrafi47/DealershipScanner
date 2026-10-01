"""
Give the dealers that synthesis could not model a recipe borrowed from one that works.

    .venv/bin/python -m backend.scripts.cascade_recipes --dry-run
    .venv/bin/python -m backend.scripts.cascade_recipes            # saves winners

Run this after backend/scripts/synthesize_recipes.py. Synthesis fills in every dealer on
a platform we have a template for; this picks up what is left -- platform recognised but
untemplated, platform unknown, or parameters not present in the HTML -- by replaying the
request shapes that already work elsewhere. See backend/scanner/recipe_cascade.py for why
rehosting a shape works and where it is refused.

Donor shapes come from rooftops with live inventory, so "known working" means observed
recently rather than merely recorded once.
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from backend.db.connect import connect as db_connect  # noqa: E402

_log = logging.getLogger("cascade")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--dry-run", action="store_true", help="report winners, save nothing")
    ap.add_argument("--min-vins", type=int, default=5)
    ap.add_argument("--max-shapes", type=int, default=40, help="shapes to try per dealer")
    ap.add_argument("--limit", type=int, help="only this many target dealers")
    ap.add_argument("--dealer", help="one dealer_key")
    args = ap.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    from backend.scanner.recipe_cascade import cascade_for_dealer, collect_shapes

    conn = db_connect(autocommit=True)
    cur = conn.cursor()

    # Donors: rooftops that currently have live inventory, i.e. shapes observed working.
    cur.execute(
        "SELECT DISTINCT dealer_id FROM cars WHERE listing_removed_at IS NULL "
        "AND dealer_id IS NOT NULL AND dealer_id <> ''"
    )
    donor_ids = [r[0] for r in cur.fetchall()]
    _log.info("donor rooftops with live inventory: %d", len(donor_ids))

    # Targets: registered, still silent, and still without a usable recipe.
    if args.dealer:
        cur.execute(
            "SELECT dealer_key, dealer_name, website_url FROM dealer_scan_status WHERE dealer_key = %s",
            (args.dealer,),
        )
    else:
        cur.execute(
            """
            SELECT s.dealer_key, s.dealer_name, s.website_url
            FROM dealer_scan_status s
            LEFT JOIN dealer_recipes r ON r.dealer_id = s.dealer_key
            WHERE COALESCE(r.recipe_count, 0) = 0
            ORDER BY s.dealer_key
            """
        )
    targets = cur.fetchall()
    if args.limit:
        targets = targets[: args.limit]
    _log.info("dealers still without a recipe: %d", len(targets))
    if not targets:
        return 0

    shapes = collect_shapes(donor_ids)
    if not shapes:
        _log.error("no donor shapes available; run a successful scan first")
        return 2

    won = 0
    results: list[tuple[str, int, int]] = []
    for key, name, url in targets:
        recipe, vins, tried = cascade_for_dealer(
            key, url, name or key, shapes,
            min_vins=args.min_vins, max_shapes=args.max_shapes,
        )
        results.append((key, vins, tried))
        if not recipe:
            _log.info("  %-38s no shape fit (%d tried)", key[:38], tried)
            continue
        won += 1
        if args.dry_run:
            _log.info("  %-38s WOULD SAVE: %d VINs via %s", key[:38], vins, recipe.url[:60])
            continue
        from backend.scanner.recipes import save_recipes

        save_recipes(key, [recipe])
        _log.info("  %-38s SAVED: %d VINs via %s", key[:38], vins, recipe.url[:60])

    _log.info(
        "cascade complete: %d of %d dealers matched a borrowed shape%s",
        won, len(targets), " (dry run, nothing written)" if args.dry_run else "",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
