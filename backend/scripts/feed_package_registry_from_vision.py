"""
Feed sticker prices read off photographs into the observed-price registry.

The registry (`package_observations` / `package_values`, see
backend/enrichment/package_registry.py) is a self-improving price book: every time we see
an option priced on a real document, it records the sighting and folds it into a per
year/make/model/trim value. It is what lets the generated build sheet quote a real number
for "Premium Package" instead of leaving it blank.

Until now it had two feeds: parsed OEM sticker PDFs, and dealer listing text. The vision
backfill adds a third -- options read directly off photographs of Monroneys that dealers
publish in their galleries -- covering cars for which no sticker PDF exists.

    .venv/bin/python -m backend.scripts.feed_package_registry_from_vision --dry-run
    .venv/bin/python -m backend.scripts.feed_package_registry_from_vision

Why authority 2 and not 3
-------------------------
`oem_sticker` (a parsed PDF) sits at authority 3, `dealer_listing` at 2. Photographed
stickers go in at **2**, deliberately below the PDF, for one specific reason: a photo taken
at an angle shifts the price column against the label column, so an option can inherit its
neighbour's price. That failure is documented three times over in this corpus (car 883479's
$1,450 "First Aid Kit", car 887269's centre caps, car 888415's "Decoding additional
functions"). A parsed PDF cannot fail that way.

The guards catch most of it -- and everything fed from here has already passed the VIN
gate, the original-Monroney gate, the USD gate, the skew filter and the corpus-wide
corroboration scrub -- but "mostly caught" is not the same as "structurally impossible",
and the registry's authority ladder exists precisely to encode that difference. If these
observations prove out against the PDF-sourced ones over time, raising the tier is a
one-line change.

Only rows whose provenance is *provable* are fed. Rows recorded before the provenance
guards existed are marked `unverified_pre_guard` and are skipped -- not because they are
presumed wrong, but because we cannot show they are right.
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import sys
from pathlib import Path
from typing import Any

_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

_log = logging.getLogger("vision_to_registry")

# Declared in backend/enrichment/package_registry.py, in BOTH the authority and
# confidence maps. This script only asserts it is there.
VISION_SOURCE = "sticker_photo"


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
    ap.add_argument("--limit", type=int, help="cap cars processed")
    ap.add_argument(
        "--include-unverified", action="store_true",
        help="also feed rows recorded before the provenance guards existed",
    )
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    # Export the DSN before touching repo internals. This script parses .env directly
    # (python-dotenv asserts under some entry points here), but package_registry and the
    # rest of backend.db read os.environ -- so without this they raise
    # "INVENTORY_DATABASE_URL must be set" while the URL sits in a local variable.
    import os

    os.environ.setdefault("INVENTORY_DATABASE_URL", _dsn())

    import psycopg

    from backend.enrichment import package_registry

    # The source is declared inside package_registry itself, in both the authority and
    # confidence maps. It was briefly patched in from here instead, which registered it in
    # one map and not the other -- a bare KeyError that surfaced as "unknown source".
    if VISION_SOURCE not in package_registry._SOURCE_AUTHORITY:
        raise SystemExit(
            f"{VISION_SOURCE!r} is not registered in backend/enrichment/package_registry.py"
        )

    conn = psycopg.connect(_dsn(), connect_timeout=15)
    conn.autocommit = True
    cur = conn.cursor()

    sql = """
        SELECT t.car_id, t.summary, c.vin, c.year, c.make, c.model, c.trim
        FROM car_image_text t
        JOIN cars c ON c.id = t.car_id
        WHERE t.version = 100
          AND t.summary IS NOT NULL
          AND c.vin IS NOT NULL AND c.vin <> ''
    """
    if not args.include_unverified:
        # Only what we can prove. See module docstring.
        sql += " AND COALESCE(t.summary->>'msrp_provenance','') <> 'unverified_pre_guard'"
    sql += " ORDER BY t.car_id"
    if args.limit:
        sql += f" LIMIT {int(args.limit)}"
    cur.execute(sql)
    rows = cur.fetchall()
    _log.info("candidate cars: %d", len(rows))

    cars_fed = observations = priced = named_only = skipped_no_id = 0

    for car_id, summary, vin, year, make, model, trim in rows:
        s = summary if isinstance(summary, dict) else json.loads(summary)

        # A price book keyed on year/make/model is useless without them.
        if not (year and make and model):
            skipped_no_id += 1
            continue

        items: list[dict[str, Any]] = []
        for opt in s.get("priced_options") or []:
            name = str(opt.get("name") or "").strip()
            try:
                price = float(opt.get("price"))
            except (TypeError, ValueError):
                continue
            if not name:
                continue
            # Refuse fees here too, independently of the recording filter. A destination
            # charge is not purchasable equipment, and 14 of them reached the price book
            # before anything checked. Two gates because this one protects a shared,
            # self-improving registry that other subsystems read.
            from backend.scripts.image_batch import _is_mandatory_fee

            if _is_mandatory_fee(name):
                continue
            low = name.lower()
            items.append({
                "kind": "package" if ("package" in low or "pkg" in low) else "option",
                "name": name,
                "price": price,
            })
            priced += 1

        # Packages seen but not priced still carry information: they say this trim CAN
        # have that package, which is what the build sheet needs to list it at all.
        priced_names = {i["name"].lower() for i in items}
        for pkg in s.get("packages") or []:
            name = str(pkg).strip()
            if name and name.lower() not in priced_names:
                items.append({"kind": "package", "name": name})
                named_only += 1

        if not items:
            continue

        car = {"vin": vin, "year": year, "make": make, "model": model, "trim": trim}
        if args.dry_run:
            cars_fed += 1
            observations += len(items)
            continue
        try:
            # No conn= : the registry's SQL uses '?' placeholders that only the repo's
            # own connection wrapper rewrites for psycopg. Handing it a raw psycopg
            # connection makes every statement arrive with "0 placeholders but N
            # parameters". Letting it open its own is both correct and simpler.
            written = package_registry.record_package_observations(
                car, VISION_SOURCE, items
            )
        except Exception as exc:  # noqa: BLE001 - one bad car must not stop the feed
            _log.warning("car %s: %s", car_id, str(exc)[:120])
            continue
        cars_fed += 1
        observations += written

    _log.info(
        "%s %d car(s): %d observation(s) — %d priced, %d package-name-only",
        "would feed" if args.dry_run else "fed", cars_fed, observations, priced, named_only,
    )
    if skipped_no_id:
        _log.info("skipped %d car(s) lacking year/make/model", skipped_no_id)

    if not args.dry_run:
        cur.execute(
            "SELECT count(*) FROM package_observations WHERE source = %s", (VISION_SOURCE,)
        )
        _log.info("registry now holds %d observation(s) from %s", cur.fetchone()[0], VISION_SOURCE)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
