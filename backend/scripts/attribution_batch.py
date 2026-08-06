"""
Find out where the unlocatable cars actually are, from their photographs.

    .venv/bin/python -m backend.scripts.attribution_batch prepare --out DIR --shards 10
    .venv/bin/python -m backend.scripts.attribution_batch apply --verdicts DIR/aN/verdicts.json

`classify_attribution` judges cars we already have gallery readings for. This handles the
rest: cars filed under a dealer that is demonstrably served a group-wide feed, where nothing
has yet looked at the pictures. `bmwofmurrieta-com` alone holds 1,921 active cars and only 99
have been read; of those, 76 name a rooftop other than BMW of Murrieta, in Georgia, North
Carolina and Tennessee.

Verdicts, and what each is allowed to do:

  ``rooftop``      -- signage, plate frame, watermark or a Sold-To box names a specific
                      store. Recorded as observed_rooftop. This is the useful outcome.
  ``owner_only``   -- the photos name a parent group ("Hendrick Automotive Group",
                      "AutoNation") but no rooftop. That is ownership, not location, and it
                      is NOT evidence of misattribution: BMW of Dallas legitimately carries
                      AutoNation branding.
  ``cannot_assess``-- no dealer identity visible, or images unreadable. The honest answer
                      when the pictures do not say. Kept strictly separate from owner_only.

Nothing here moves a car between dealers. Re-attribution needs the destination rooftop
registered and geocoded first, and a plate frame alone is not enough -- an earlier pass let
an invoice "Sold To" overwrite a correct plate-frame reading and moved cars to the wrong
store. This records evidence; moving is a separate, reviewable step.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

_log = logging.getLogger("attribution_batch")


def _dsn() -> str:
    dsn = (os.environ.get("INVENTORY_DATABASE_URL") or os.environ.get("DATABASE_URL") or "").strip()
    if dsn:
        return dsn
    env = _REPO_ROOT / ".env"
    if env.exists():
        m = re.search(r"^INVENTORY_DATABASE_URL=(.+)$", env.read_text(), re.M)
        if m:
            return m.group(1).strip().strip("'\"")
    raise SystemExit("INVENTORY_DATABASE_URL is not set")


def _connect():
    import psycopg

    os.environ.setdefault("INVENTORY_DATABASE_URL", _dsn())
    conn = psycopg.connect(_dsn(), connect_timeout=20)
    conn.autocommit = True
    return conn


def _sample(urls: list[str], limit: int) -> list[str]:
    """
    Stride across the whole gallery, endpoints inclusive.

    Same shape as the fix in recheck_msrp: head-only sampling made agents conclude a sticker
    did not exist because it sat late in the set, and an off-by-one that dropped the LAST
    image did the same thing again. Dealer signage and plate frames cluster in the exterior
    shots at the START, and Sold-To boxes appear on paperwork at the END, so both ends matter
    here for the same reason.
    """
    if len(urls) <= limit:
        return urls
    last = len(urls) - 1
    picked, seen = [], set()
    for i in range(limit):
        idx = round(i * last / (limit - 1))
        if idx not in seen:
            seen.add(idx)
            picked.append(urls[idx])
    return picked


def cmd_prepare(args: argparse.Namespace) -> int:
    conn = _connect()
    cur = conn.cursor()
    # Cars at a group-fed dealer that no photograph has yet placed. Ordered so the worst
    # offender is drained first.
    cur.execute(
        """
        SELECT c.id, c.vin, c.year, c.make, c.model, c.trim, c.dealer_id,
               COALESCE(c.dealer_name, ''), c.gallery
        FROM cars c
        JOIN dealer_feed_scope s ON s.dealer_id = c.dealer_id AND s.scope = 'group'
        LEFT JOIN car_attribution a ON a.car_id = c.id
        WHERE c.listing_removed_at IS NULL
          AND c.gallery IS NOT NULL AND c.gallery NOT IN ('', '[]')
          AND (a.car_id IS NULL OR a.status = 'unverified')
        ORDER BY s.photos_naming_other DESC, c.id
        LIMIT %s
        """,
        (args.limit,),
    )
    rows = cur.fetchall()
    conn.close()
    _log.info("%d unplaced car(s) at group-fed dealers", len(rows))
    if not rows:
        return 0

    out = Path(args.out)
    shards: list[list[dict]] = [[] for _ in range(max(1, args.shards))]
    for i, (cid, vin, year, make, model, trim, did, dname, gallery) in enumerate(rows):
        try:
            images = json.loads(gallery) if isinstance(gallery, str) else (gallery or [])
        except (TypeError, ValueError):
            images = []
        shards[i % len(shards)].append({
            "car_id": cid,
            "vin": vin or "",
            "year": year, "make": make, "model": model, "trim": trim,
            "filed_dealer_id": did,
            "filed_dealer_name": dname,
            "images": _sample([u for u in images if isinstance(u, str)], args.images),
        })

    for n, cars in enumerate(shards):
        d = out / f"a{n}"
        d.mkdir(parents=True, exist_ok=True)
        (d / "manifest.json").write_text(json.dumps({"cars": cars}, indent=1))
    _log.info("wrote %d shard(s) under %s", len(shards), out)
    return 0


def cmd_apply(args: argparse.Namespace) -> int:
    payload = json.loads(Path(args.verdicts).read_text())
    items = payload.get("verdicts") if isinstance(payload, dict) else payload
    if not isinstance(items, list):
        raise SystemExit("verdicts file must be a list, or {'verdicts': [...]}")

    conn = _connect()
    cur = conn.cursor()
    placed = owner = unknown = skipped = 0

    for item in items:
        if not isinstance(item, dict):
            continue
        car_id = item.get("car_id")
        verdict = str(item.get("verdict") or "").strip().lower()
        rooftop = (item.get("rooftop") or "").strip() or None
        note = str(item.get("notes") or "").strip()
        if not car_id:
            continue

        cur.execute("SELECT dealer_id, COALESCE(dealer_name,'') FROM cars WHERE id = %s", (car_id,))
        row = cur.fetchone()
        if not row:
            skipped += 1
            continue
        filed_id, filed_name = row

        if verdict == "rooftop" and rooftop:
            from backend.scripts.classify_attribution import _names_same_store

            same = _names_same_store(rooftop, filed_name, filed_id)
            status = "confirmed" if same else "conflicting"
            evidence = f"gallery names {rooftop!r}" + ("" if same else f"; filed as {filed_name!r}")
            if note:
                evidence = f"{evidence} -- {note}"[:900]
            placed += 1
        elif verdict == "owner_only":
            # Names the parent company. Ownership, not location: it neither confirms nor
            # contradicts the filing, so the car stays unverified rather than becoming a
            # false conflict.
            status, rooftop = "unverified", None
            evidence = f"photos name a parent group only, no rooftop -- {note}"[:900]
            owner += 1
        else:
            status, rooftop = "unverified", None
            evidence = f"no dealer identity visible in gallery -- {note}"[:900]
            unknown += 1

        cur.execute(
            """
            INSERT INTO car_attribution
                (car_id, status, observed_rooftop, filed_dealer_id, evidence, decided_at)
            VALUES (%s, %s, %s, %s, %s, NOW())
            ON CONFLICT (car_id) DO UPDATE SET
                status = EXCLUDED.status,
                observed_rooftop = EXCLUDED.observed_rooftop,
                filed_dealer_id = EXCLUDED.filed_dealer_id,
                evidence = EXCLUDED.evidence,
                decided_at = NOW()
            """,
            (car_id, status, rooftop, filed_id, evidence),
        )

    _log.info(
        "placed %d, owner-branding-only %d, no identity %d, skipped %d",
        placed, owner, unknown, skipped,
    )
    conn.close()
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    sub = ap.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("prepare")
    p.add_argument("--out", required=True)
    p.add_argument("--shards", type=int, default=10)
    p.add_argument("--images", type=int, default=10)
    p.add_argument("--limit", type=int, default=200)
    p.set_defaults(fn=cmd_prepare)

    a = sub.add_parser("apply")
    a.add_argument("--verdicts", required=True)
    a.set_defaults(fn=cmd_apply)

    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    return args.fn(args)


if __name__ == "__main__":
    raise SystemExit(main())
