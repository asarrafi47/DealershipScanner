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


# Selection and download both come from image_batch. They were duplicated here, and every
# copy drifted: a head-only sample deleted 158 verified prices, an off-by-one that hid the
# LAST photo wasted a whole run, and per-agent curl let a CDN truncate the evidence to 7
# images per car. image_batch's versions were correct the entire time, 30 lines from where
# the broken copies were written.
from backend.scripts.image_batch import _download, _select_images


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
            "images": _select_images([u for u in images if isinstance(u, str)], args.images),
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
    confirmed = corrected = withdrawn = skipped = unassessed = proposed = 0

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

        elif verdict == "withdraw" and not args.allow_withdraw:
            # Agents do not get to delete prices. Three independent passes produced
            # 179 withdrawals and NOT ONE was correct:
            #
            #   attempt 1  158 withdrawn -- head-only image sample; they never saw the sticker
            #   attempt 2    4 withdrawn -- called a $1,350 destination charge a "First Aid Kit"
            #   attempt 3   17 withdrawn -- called the true total a "pre-destination subtotal"
            #
            # Attempt 3 is the instructive one. Every withdrawal came with correct arithmetic
            # (base + options summed exactly to the recorded figure) and a confident reading
            # of a shifted column. It was still wrong: the listed price sits $85-$799 above
            # the figure, identically for every car at the same dealer, which is a per-dealer
            # DOC FEE, not a missing ~$1,350 destination charge. Cars 887041/887105/887254
            # were withdrawn and restored twice.
            #
            # The failure is structural, not a prompt problem. A verdict that can only be
            # checked by reopening the image is not a verdict an image-reading agent can be
            # trusted to self-certify. So withdraw now PARKS the row for review and leaves
            # the number alone. Pass --allow-withdraw only after a human has looked.
            summary["msrp_recheck"] = "withdraw_proposed"
            summary["msrp_recheck_note"] = note
            cur.execute(
                "UPDATE car_image_text SET summary = %s WHERE car_id = %s AND version = %s",
                (json.dumps(summary), car_id, VERSION),
            )
            proposed += 1

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
        "confirmed %d, corrected %d, withdrawn %d, withdrawal-proposed %d, cannot-assess %d, skipped %d",
        confirmed, corrected, withdrawn, proposed, unassessed, skipped,
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
    a.add_argument("--allow-withdraw", action="store_true",
                   help="actually null the MSRP on a withdraw verdict. Off by default: "
                        "179 agent withdrawals across three passes, none correct.")
    a.set_defaults(fn=cmd_apply)

    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    return args.fn(args)


if __name__ == "__main__":
    raise SystemExit(main())
