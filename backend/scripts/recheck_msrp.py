"""
Re-read the MSRPs whose caveat we destroyed.

``record`` truncated the agent's ``notes`` at 500 characters. 562 rows hit that cutoff and
**174 of them carry an ``sticker_msrp``** -- 52% of every MSRP this project holds. On car
883394 the severed sentence was the agent explaining that the figure sat on the Destination
Charge row rather than the Total row; the price column was shifted and the number was
withdrawn. We cannot know how many of the other 173 carry a similar warning, because the
text was cut before it was stored. It is not recoverable. It has to be read again.

    .venv/bin/python -m backend.scripts.recheck_msrp prepare --out DIR --shards 12
    .venv/bin/python -m backend.scripts.recheck_msrp apply --verdicts DIR/vN/verdicts.json

Why static shards here, when the sweep claims dynamically
---------------------------------------------------------
The sweep partitions by atomic claim because its candidate pool is huge, changing, and
shared with concurrent runs -- fixed ranges there would lose work when an agent dies and
would fight the priority ordering. This set is the opposite: 174 known ids, fixed for the
duration, no competing consumer. Splitting them N ways up front is simpler and needs no
reservation table.

A verdict never silently overwrites. ``confirmed`` leaves the row alone; ``corrected``
requires the agent to supply the value it actually read on the Total line; ``withdraw``
nulls the MSRP and records why. Anything else is left for a human.
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

_log = logging.getLogger("recheck_msrp")

_MIN_MSRP, _MAX_MSRP = 5_000, 500_000
VERSION = 100


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
    conn = psycopg.connect(_dsn(), connect_timeout=15)
    conn.autocommit = True
    return conn


# The truncation was a hard slice at 500, so a stored note of exactly 500 characters is a
# note that lost its tail. 499 is the safe boundary for the comparison.
_TRUNCATED = "length(t.summary->>'notes') >= 499"


def _sample(urls: list[str], limit: int) -> list[str]:
    """
    Spread the sample across the whole gallery.

    The first version of this took ``urls[:limit]`` -- the first N photos -- which is the
    exact defect already diagnosed and fixed in the sweep's own image selection: dealers
    lead with exterior beauty shots and put the Monroney deep in the set, so head-only
    sampling finds no stickers and reports the car as having none.

    It cost 158 false withdrawals on the first recheck run. Median gallery in that set was
    19 photos and the pass saw 10, so "no sticker visible in any of the 10 images" was a
    property of the sample rather than of the car. Three of the withdrawn cars had been
    VIN-matched and arithmetically reconciled by an independent auditor hours earlier.

    Taking every k-th image gives the same budget a view of the entire gallery.
    """
    if len(urls) <= limit:
        return urls
    # Endpoints INCLUSIVE. The first version used int(i * len/limit), whose largest index
    # is int((limit-1) * len/limit) -- strictly less than len-1 whenever the gallery is
    # bigger than the sample. So the LAST photo was structurally unreachable, and the last
    # photo is where dealers put the Monroney. That produced 150 "cannot_assess" verdicts
    # in which agents wrote paragraphs about zoom levels and focus while a flat, sharp,
    # table-top shot of the sticker sat one slot past the end of what they were handed;
    # an auditor pulled the withheld tail for 20 of them and found a legible sticker in 19.
    #
    # Both ends now always appear, and the interior is spread evenly between them.
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
    # --pending: only rows never actually verified. 'cannot_assess' and
    # 'withdrawal_reverted_bad_sampling' both mean "a broken sampler looked at this and
    # learned nothing"; 'confirmed' means a human-equivalent read succeeded and must not
    # be redone.
    pending = (
        "AND COALESCE(t.summary->>'msrp_recheck','') <> 'confirmed'"
        if args.pending else ""
    )
    cur.execute(
        f"""
        SELECT t.car_id, t.sticker_msrp, c.vin, c.year, c.make, c.model, c.trim,
               c.price, c.condition, c.gallery, t.summary->>'notes'
        FROM car_image_text t
        JOIN cars c ON c.id = t.car_id
        WHERE t.version = %s
          AND t.sticker_msrp IS NOT NULL
          AND ({_TRUNCATED} OR COALESCE(t.summary->>'msrp_recheck','') = 'cannot_assess')
          {pending}
        ORDER BY t.car_id
        """,
        (VERSION,),
    )
    rows = cur.fetchall()
    conn.close()
    _log.info("%d MSRP row(s) with a truncated caveat", len(rows))
    if not rows:
        return 0

    out = Path(args.out)
    shards: list[list[dict]] = [[] for _ in range(max(1, args.shards))]
    for i, (car_id, msrp, vin, year, make, model, trim, price, condition, gallery, notes) in enumerate(rows):
        try:
            images = json.loads(gallery) if isinstance(gallery, str) else (gallery or [])
        except (TypeError, ValueError):
            images = []
        shards[i % len(shards)].append({
            "car_id": car_id,
            "recorded_msrp": float(msrp),
            "vin": vin or "",
            "year": year, "make": make, "model": model, "trim": trim,
            "price": float(price) if price else None,
            "condition": condition,
            # The caveat as stored -- cut off, but its surviving half is still the best
            # hint about what the agent was worried about.
            "truncated_note": notes or "",
            "images": _sample([u for u in images if isinstance(u, str)], args.images),
        })

    for n, cars in enumerate(shards):
        d = out / f"v{n}"
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
    confirmed = corrected = withdrawn = skipped = unassessed = 0

    for item in items:
        if not isinstance(item, dict):
            continue
        car_id = item.get("car_id")
        verdict = str(item.get("verdict") or "").strip().lower()
        if not car_id:
            continue

        # Only touch rows this pass actually selected, and only at our version.
        cur.execute(
            "SELECT sticker_msrp, summary FROM car_image_text WHERE car_id = %s AND version = %s",
            (car_id, VERSION),
        )
        row = cur.fetchone()
        if not row or row[0] is None:
            skipped += 1
            continue
        summary = row[1] if isinstance(row[1], dict) else json.loads(row[1] or "{}")
        note = str(item.get("notes") or "").strip()

        if verdict == "confirmed":
            summary["msrp_recheck"] = "confirmed"
            summary["msrp_recheck_note"] = note
            cur.execute(
                "UPDATE car_image_text SET summary = %s WHERE car_id = %s AND version = %s",
                (json.dumps(summary), car_id, VERSION),
            )
            confirmed += 1

        elif verdict == "corrected":
            try:
                value = float(str(item.get("msrp")).replace(",", "").replace("$", ""))
            except (TypeError, ValueError):
                skipped += 1
                continue
            # A correction is a new reading, so it faces the same band the original did.
            if not (_MIN_MSRP <= value <= _MAX_MSRP):
                skipped += 1
                continue
            summary["msrp_recheck"] = "corrected"
            summary["msrp_recheck_note"] = note
            summary["msrp_recheck_previous"] = row[0]
            cur.execute(
                "UPDATE car_image_text SET sticker_msrp = %s, summary = %s "
                "WHERE car_id = %s AND version = %s",
                (value, json.dumps(summary), car_id, VERSION),
            )
            corrected += 1

        elif verdict == "cannot_assess":
            # "I could not find a sticker in the images I was shown" is a statement about
            # our sample, not about the car, and it must never null a price. Collapsing it
            # into `withdraw` is what turned a head-only sampling bug into 158 silent
            # deletions: every one of those withdrawals was accurately reasoned about the
            # ten photos the agent received, and wrong about the vehicle.
            summary["msrp_recheck"] = "cannot_assess"
            summary["msrp_recheck_note"] = note
            cur.execute(
                "UPDATE car_image_text SET summary = %s WHERE car_id = %s AND version = %s",
                (json.dumps(summary), car_id, VERSION),
            )
            unassessed += 1

        elif verdict == "withdraw":
            # Withdrawing keeps the row and the reason; only the number goes. The car
            # stays analysed, so the sweep will not re-claim it.
            summary["msrp_recheck"] = "withdrawn"
            summary["msrp_recheck_note"] = note
            summary["msrp_recheck_previous"] = row[0]
            summary["msrp_provenance"] = "withdrawn_on_recheck"
            cur.execute(
                "UPDATE car_image_text SET sticker_msrp = NULL, summary = %s "
                "WHERE car_id = %s AND version = %s",
                (json.dumps(summary), car_id, VERSION),
            )
            withdrawn += 1
        else:
            skipped += 1

    conn.close()
    _log.info(
        "confirmed %d, corrected %d, withdrawn %d, cannot-assess %d, skipped %d",
        confirmed, corrected, withdrawn, unassessed, skipped,
    )
    # A pass that withdraws most of what it touches is reporting on its own input, not on
    # the data. 158 of 161 was the first run; it was a head-only image sample.
    decided = confirmed + corrected + withdrawn + unassessed
    if decided and withdrawn > decided * 0.5:
        _log.warning(
            "REFUSING TO TRUST: %d of %d verdicts were withdrawals. Check what the agents "
            "were actually shown before believing this.", withdrawn, decided,
        )
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    sub = ap.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("prepare")
    p.add_argument("--out", required=True)
    p.add_argument("--shards", type=int, default=12)
    p.add_argument("--pending", action="store_true",
                   help="only rows not yet successfully verified")
    p.add_argument("--images", type=int, default=10)
    p.set_defaults(fn=cmd_prepare)

    a = sub.add_parser("apply")
    a.add_argument("--verdicts", required=True)
    a.set_defaults(fn=cmd_apply)

    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    return args.fn(args)


if __name__ == "__main__":
    raise SystemExit(main())
