#!/usr/bin/env python3
"""Decode every active listing's VIN into ``nhtsa_vpic_cache``.

WHY
---
``epa_extended_specs`` is contaminated at the model level — one scraped value
stamped across a nameplate's every trim and model year (BMW 3 Series: a single
0-60 of 3.7s across 579 rows, 1984-2026). The knowledge engine correctly refuses
horsepower and torque from a family that scraped to one value, and nothing fills
in behind it, so cars render with the field blank.

vPIC fills it. It is the manufacturer's own filing keyed on the INDIVIDUAL VIN,
so model-level contamination is impossible by construction, and it is free with
no API key. On 2026-08-04, 30,388 of 55,246 active listings had a cached decode
and 76% of those carried a usable ``EngineHP``. This decodes the rest.

BATCH ENDPOINT
--------------
``DecodeVINValuesBatch`` takes a semicolon-separated batch in one POST and
returns the same flat ``Results`` rows as the per-VIN call, which is what
``backend.utils.vpic_specs`` reads. Batching keeps this to a few hundred
requests instead of ~25,000.

Idempotent: only VINs absent from the cache are fetched, so a killed run
resumes by simply being re-run. Failures are per-batch and non-fatal.

Usage:
    .venv/bin/python backend/scripts/backfill_vpic_cache.py [--limit N] [--batch 50]
"""
from __future__ import annotations

import argparse
import json
import logging
import re
import sys
import time

sys.path.insert(0, ".")

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger("vpic_backfill")

_ENDPOINT = "https://vpic.nhtsa.dot.gov/api/vehicles/DecodeVINValuesBatch/"
# vPIC is a public US government service with no published rate limit. A short
# pause between batches keeps this well-mannered rather than hammering it.
_PAUSE_S = 0.6
_TIMEOUT_S = 90
_VIN_RE = re.compile(r"^[A-HJ-NPR-Z0-9]{17}$")


def _fetch_batch(vins: list[str]) -> dict[str, dict]:
    """``{vin: single-decode response}`` shaped exactly like the per-VIN call.

    The batch response holds one flat row per VIN under ``Results``. Each row is
    re-wrapped as its own ``{"Results": [row]}`` so what lands in the cache is
    byte-compatible with what ``vpic_specs._decode_row`` expects, whether the
    row arrived from this script or from a single-VIN decode elsewhere.
    """
    import requests

    resp = requests.post(
        _ENDPOINT,
        data={"format": "json", "data": ";".join(vins)},
        timeout=_TIMEOUT_S,
    )
    resp.raise_for_status()
    rows = (resp.json() or {}).get("Results") or []
    out: dict[str, dict] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        vin = str(row.get("VIN") or "").strip().upper()
        if vin:
            out[vin] = {"Count": 1, "Message": "Batch decode", "Results": [row]}
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=0, help="max VINs to decode this run")
    ap.add_argument("--batch", type=int, default=50)
    ap.add_argument("--dealers", default="", help="comma-separated dealer_ids; default = whole fleet")
    args = ap.parse_args()
    dealers = [d.strip() for d in args.dealers.split(",") if d.strip()]

    import psycopg

    url = re.search(
        r"^INVENTORY_DATABASE_URL=(.+)$", open(".env").read(), re.M
    ).group(1).strip()
    conn = psycopg.connect(url)
    conn.autocommit = True
    cur = conn.cursor()

    cur.execute(
        """SELECT DISTINCT c.vin FROM cars c
            LEFT JOIN nhtsa_vpic_cache v ON v.vin = c.vin
           WHERE c.listing_active = 1 AND v.vin IS NULL
             AND c.vin IS NOT NULL AND LENGTH(TRIM(c.vin)) = 17"""
        + (" AND c.dealer_id = ANY(%s)" if dealers else ""),
        (dealers,) if dealers else None,
    )
    todo = [r[0].strip().upper() for r in cur.fetchall()]
    todo = [v for v in todo if _VIN_RE.match(v)]
    if args.limit:
        todo = todo[: args.limit]

    cur.execute(
        """SELECT COUNT(*) FROM cars c JOIN nhtsa_vpic_cache v ON v.vin = c.vin
            WHERE c.listing_active = 1"""
    )
    before = cur.fetchone()[0]
    log.info("cached decodes for active listings BEFORE: %d", before)
    log.info("VINs to decode: %d (batches of %d)\n", len(todo), args.batch)

    stored = failed = 0
    for i in range(0, len(todo), args.batch):
        chunk = todo[i : i + args.batch]
        try:
            decoded = _fetch_batch(chunk)
        except Exception as exc:
            failed += len(chunk)
            log.warning("  batch %d failed: %s", i // args.batch, str(exc)[:90])
            time.sleep(_PAUSE_S)
            continue
        for vin, payload in decoded.items():
            try:
                cur.execute(
                    """INSERT INTO nhtsa_vpic_cache (vin, response_json, fetched_at)
                       VALUES (%s, %s, now())
                       ON CONFLICT (vin) DO NOTHING""",
                    (vin, json.dumps(payload)),
                )
                stored += cur.rowcount
            except Exception as exc:
                log.debug("store %s failed: %s", vin, exc)
        if (i // args.batch) % 20 == 0:
            log.info("  %d/%d decoded, %d stored", min(i + args.batch, len(todo)), len(todo), stored)
        time.sleep(_PAUSE_S)

    cur.execute(
        """SELECT COUNT(*) FROM cars c JOIN nhtsa_vpic_cache v ON v.vin = c.vin
            WHERE c.listing_active = 1"""
    )
    after = cur.fetchone()[0]
    log.info("\nstored %d new decode(s), %d VIN(s) failed", stored, failed)
    log.info("cached decodes for active listings AFTER: %d (was %d)", after, before)
    conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
