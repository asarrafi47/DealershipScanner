"""
Feed CONFIRMED car-photo option/package facts into the observed-price registry.

Mirrors feed_package_registry_from_brochure_vision.py exactly, pointed at
car_image_vision_facts (V017) instead of brochure_vision_facts (V015). See that
script's docstring for the full rationale -- the short version: the registry
(package_observations / package_values, backend/enrichment/package_registry.py)
is what generated_spec_sheet.py reads to quote a real name/price for a car's
build sheet. Only review_verdict='confirmed' rows are fed; the 24/7 local-model
lane (local_car_image_vision_extract.py) writes review_verdict=NULL and is
deliberately skipped here until an independent review pass promotes it.

Fed under the EXISTING 'sticker_photo' source tier: this is the same real-world
evidence type (a photographed window sticker) as the cloud Sonnet vision-scan
campaign already feeds through feed_package_registry_from_vision.py -- just read
locally instead. Trim-level, never claimed as a fact about the specific VIN it
was photographed on; see package_registry._SOURCE_AUTHORITY.

Usage:
    .venv/bin/python -m backend.scripts.feed_package_registry_from_car_image_vision --dry-run
    .venv/bin/python -m backend.scripts.feed_package_registry_from_car_image_vision
"""
from __future__ import annotations

import argparse
import logging
import re
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

_log = logging.getLogger("car_image_vision_to_registry")

SOURCE = "sticker_photo"


def _dsn() -> str:
    import os

    dsn = (os.environ.get("INVENTORY_DATABASE_URL") or os.environ.get("DATABASE_URL") or "").strip()
    if dsn:
        return dsn
    env = _REPO_ROOT / ".env"
    if env.exists():
        m = re.search(r"^INVENTORY_DATABASE_URL=(.+)$", env.read_text(), re.M)
        if m:
            return m.group(1).strip().strip("'\"")
    raise SystemExit("INVENTORY_DATABASE_URL is not set")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--dry-run", action="store_true", help="report what would be fed, write nothing")
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    import os

    os.environ.setdefault("INVENTORY_DATABASE_URL", _dsn())

    import psycopg

    from backend.enrichment import package_registry

    if SOURCE not in package_registry._SOURCE_AUTHORITY:
        raise SystemExit(f"{SOURCE!r} is not registered in backend/enrichment/package_registry.py")

    conn = psycopg.connect(_dsn(), connect_timeout=15)
    conn.autocommit = True
    cur = conn.cursor()

    cur.execute(
        """
        SELECT year, make, model, trim, fact_type, name, value_text, price
        FROM car_image_vision_facts
        WHERE review_verdict = 'confirmed'
          AND fact_type IN ('option', 'package')
        ORDER BY year, make, model, trim
        """
    )
    rows = cur.fetchall()
    _log.info("candidate fact(s): %d", len(rows))

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
