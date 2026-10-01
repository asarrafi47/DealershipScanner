"""
Feed CONFIRMED brochure-read option/package facts into the observed-price registry.

The registry (package_observations / package_values, see backend/enrichment/package_registry.py)
is what backend/enrichment/generated_spec_sheet.py reads to quote a real name/price for a car's
build sheet Options tab. Until now it had three feeds -- parsed OEM sticker PDFs, dealer listing
text, and photographed window stickers (backend/scripts/feed_package_registry_from_vision.py).
This adds a fourth: option/package facts read directly off manufacturer brochure PDFs by the
vision pipeline (backend/scripts/local_brochure_vision_extract.py and its cloud counterpart),
stored in brochure_vision_facts.

Only review_verdict='confirmed' rows are fed. Rows from the 24/7 local-model lane land with
review_verdict=NULL (see V015's docstring -- local extraction alone hasn't earned the
'confirmed' bar) and are deliberately skipped here until an independent review pass promotes
them. This is the same discipline as the vision-photo feed: a shared, customer-facing registry
does not trust a single unreviewed pass.

A brochure describes what a TRIM offers in general, not what one specific car was built with --
see package_registry._SOURCE_AUTHORITY for why 'brochure' sits below 'oem_sticker'.

Usage:
    .venv/bin/python -m backend.scripts.feed_package_registry_from_brochure_vision --dry-run
    .venv/bin/python -m backend.scripts.feed_package_registry_from_brochure_vision
"""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from backend.db.connect import connect as db_connect, inventory_dsn  # noqa: E402

_log = logging.getLogger("brochure_vision_to_registry")

SOURCE = "brochure"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--dry-run", action="store_true", help="report what would be fed, write nothing")
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    inventory_dsn(export=True)

    from backend.enrichment import package_registry

    if SOURCE not in package_registry._SOURCE_AUTHORITY:
        raise SystemExit(f"{SOURCE!r} is not registered in backend/enrichment/package_registry.py")

    conn = db_connect(autocommit=True)
    cur = conn.cursor()

    cur.execute(
        """
        SELECT year, make, model, trim, fact_type, name, value_text, price
        FROM brochure_vision_facts
        WHERE review_verdict = 'confirmed'
          AND fact_type IN ('option', 'package')
        ORDER BY year, make, model, trim
        """
    )
    rows = cur.fetchall()
    _log.info("candidate fact(s): %d", len(rows))

    # Group by (year, make, model, trim) so one registry call handles one trim's items,
    # matching record_package_observations' per-car calling convention.
    grouped: dict[tuple, list[dict]] = {}
    for year, make, model, trim, fact_type, name, value_text, price in rows:
        key = (year, make, model, trim)
        grouped.setdefault(key, []).append({
            "kind": fact_type,
            "name": name,
            "price": price,
            "category": value_text,
        })

    cars_fed = observations = priced = named_only = 0
    for (year, make, model, trim), items in grouped.items():
        car = {"year": year, "make": make, "model": model, "trim": trim}
        priced += sum(1 for i in items if i.get("price") is not None)
        named_only += sum(1 for i in items if i.get("price") is None)
        if args.dry_run:
            cars_fed += 1
            observations += len(items)
            continue
        try:
            written = package_registry.record_package_observations(car, SOURCE, items)
        except Exception as exc:  # noqa: BLE001 - one bad trim must not stop the feed
            _log.warning("%s %s %s %s: %s", year, make, model, trim, str(exc)[:120])
            continue
        cars_fed += 1
        observations += written

    _log.info(
        "%s %d trim(s): %d observation(s) — %d priced, %d name-only",
        "would feed" if args.dry_run else "fed", cars_fed, observations, priced, named_only,
    )
    if not args.dry_run:
        cur.execute("SELECT count(*) FROM package_observations WHERE source = %s", (SOURCE,))
        _log.info("registry now holds %d observation(s) from %s", cur.fetchone()[0], SOURCE)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
