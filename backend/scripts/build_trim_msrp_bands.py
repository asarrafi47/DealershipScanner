"""
Rebuild ``trim_msrp_bands`` from stickers we have actually read.

    .venv/bin/python -m backend.scripts.build_trim_msrp_bands --dry-run
    .venv/bin/python -m backend.scripts.build_trim_msrp_bands

Sources, both of them documents rather than feed claims:

* ``car_image_text`` rows at the agent-vision version whose MSRP passed the provenance
  gates -- VIN confirmed, total read rather than reconstructed, original Monroney, USD.
* ``cars.msrp`` on NEW inventory, which for a new car is the sticker figure the
  manufacturer set. Used/CPO rows are excluded: their feed msrp is a marketing "was"
  price, which is the contamination this project already suppresses at read time.

The output is a band, deliberately. See migrations/V010__trim_msrp_bands.sql for why a
median must never be served as a car's MSRP: the observed spread within a single trim runs
to five figures because a Monroney total includes options.

A cohort needs ``--min-observations`` sightings before it is written. Two observations
describe two cars, not a trim.
"""

from __future__ import annotations

import argparse
import json
import logging
import statistics
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any

_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from backend.db.connect import connect as db_connect, inventory_dsn  # noqa: E402

_log = logging.getLogger("trim_msrp_bands")

# Same band the sticker parser accepts; anything outside is a misread, not a car.
_MIN_MSRP, _MAX_MSRP = 5_000, 500_000


def _pct(values: list[int], q: float) -> int:
    """Percentile without numpy; values must be sorted."""
    if not values:
        return 0
    if len(values) == 1:
        return values[0]
    idx = min(len(values) - 1, max(0, int(round(q * (len(values) - 1)))))
    return values[idx]


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--min-observations", type=int, default=3)
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    inventory_dsn(export=True)
    conn = db_connect(autocommit=True)
    cur = conn.cursor()

    # cohort -> {"values": [...], "sources": {name: count}}
    cohorts: dict[tuple[int, str, str, str], dict[str, Any]] = defaultdict(
        lambda: {"values": [], "sources": defaultdict(int)}
    )

    def add(year, make, model, trim, msrp, source) -> None:
        try:
            value = int(round(float(msrp)))
        except (TypeError, ValueError):
            return
        if not (_MIN_MSRP <= value <= _MAX_MSRP) or not (year and make and model):
            return
        key = (int(year), str(make).strip(), str(model).strip(), str(trim or "").strip())
        cohorts[key]["values"].append(value)
        cohorts[key]["sources"][source] += 1

    # 1) Photographed Monroneys that cleared every provenance gate. Dealer build
    #    sheets are excluded even though (since 2026-08-18) cmd_record stores
    #    their totals: a band claims what the FACTORY stickers this trim at, and
    #    a dealer-generated sheet can price a different vehicle entirely (car
    #    874954's sheet covered the base chassis of a camper conversion).
    cur.execute(
        """
        SELECT c.year, c.make, c.model, c.trim, t.sticker_msrp
        FROM car_image_text t
        JOIN cars c ON c.id = t.car_id
        WHERE t.sticker_msrp IS NOT NULL
          AND t.summary->>'msrp_read_directly' = 'true'
          AND COALESCE(t.summary->>'msrp_provenance','') <> 'unverified_pre_guard'
          AND COALESCE(t.summary->>'msrp_document','monroney') <> 'dealer_build_sheet'
        """
    )
    photo_rows = cur.fetchall()
    for year, make, model, trim, msrp in photo_rows:
        add(year, make, model, trim, msrp, "sticker_photo")

    # 2) New-car feed MSRP. On a new car that is the manufacturer's sticker figure; on a
    #    used one it is a marketing "was" price, so used/CPO is excluded entirely.
    cur.execute(
        """
        SELECT year, make, model, trim, msrp
        FROM cars
        WHERE msrp IS NOT NULL AND msrp > 0
          AND listing_removed_at IS NULL
          AND LOWER(COALESCE(condition,'')) = 'new'
          AND COALESCE(is_cpo, 0) IN (0, FALSE::int)
          AND msrp <> price
        """
    )
    feed_rows = cur.fetchall()
    for year, make, model, trim, msrp in feed_rows:
        add(year, make, model, trim, msrp, "new_feed_msrp")

    _log.info(
        "inputs: %d verified photographed sticker(s), %d new-car feed msrp(s)",
        len(photo_rows), len(feed_rows),
    )

    rows: list[tuple] = []
    for (year, make, model, trim), data in cohorts.items():
        values = sorted(data["values"])
        if len(values) < args.min_observations:
            continue
        rows.append((
            year, make, model, trim, len(values),
            values[0], _pct(values, 0.25), int(statistics.median(values)),
            _pct(values, 0.75), values[-1],
            json.dumps(dict(data["sources"])),
        ))

    _log.info(
        "%s %d cohort(s) with >= %d observation(s) (from %d candidate cohorts)",
        "would write" if args.dry_run else "writing", len(rows),
        args.min_observations, len(cohorts),
    )
    if args.dry_run:
        for r in sorted(rows, key=lambda x: -x[4])[:8]:
            spread = r[9] - r[5]
            _log.info(
                "  %s %s %s %s  n=%d  $%s-$%s (median $%s, spread $%s)",
                r[0], r[1], r[2], r[3] or "-", r[4],
                f"{r[5]:,}", f"{r[9]:,}", f"{r[7]:,}", f"{spread:,}",
            )
        return 0

    # Full rebuild: every row is derived, so a stale cohort is worse than a missing one.
    cur.execute("TRUNCATE trim_msrp_bands")
    for r in rows:
        cur.execute(
            "INSERT INTO trim_msrp_bands (year, make, model, trim, observations, "
            "msrp_min, msrp_p25, msrp_median, msrp_p75, msrp_max, sources) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            r,
        )
    _log.info("trim_msrp_bands rebuilt: %d cohort(s)", len(rows))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
