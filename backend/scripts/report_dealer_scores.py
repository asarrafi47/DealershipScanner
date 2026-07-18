"""Print a composite dealer-score ranking from live Postgres.

Usage::

    PYTHONPATH=. python backend/scripts/report_dealer_scores.py [--top N] [--breakdown DEALER_ID]

Read-only. Ranks every dealer with active priced inventory via
:mod:`backend.intelligence.dealer_score` (reputation + pricing aggressiveness +
inventory) and prints the top and bottom of the board plus one full breakdown.

Pricing bands are only as fresh as the last
``python -m backend.scripts.compute_market_stats`` run; run that first (or
nightly) for current pricing-aggressiveness numbers.
"""

from __future__ import annotations

import argparse
import json

from backend.db.inventory_pg import pg_connect
from backend.intelligence import dealer_score as ds


def _hdr(title: str) -> None:
    print("\n" + "=" * 78)
    print(title)
    print("=" * 78)


def _fmt(v: object, width: int = 6) -> str:
    return f"{v:>{width}}" if v is not None else " " * (width - 1) + "-"


def _row(r: dict) -> str:
    rep = r["reputation"]
    pr = r["pricing_aggressiveness"]
    inv = r["inventory"]
    rating = rep["google_rating"]
    rating_s = f"{rating:.1f}({rep['google_review_count'] or 0})" if rating else "-"
    med = pr["median_pct_from_market"]
    med_s = f"{med:+.1f}%" if med is not None else "-"
    return (
        f"{str(r['composite']):>6}  "
        f"{str(r['dealer_id'])[:30]:<30}  "
        f"rep={_fmt(rep['score'])} ({rating_s:>9})  "
        f"price={_fmt(pr['score'])} (med {med_s:>7})  "
        f"inv={_fmt(inv['score'])} ({inv['size']:>4})"
    )


def _print_breakdown(r: dict) -> None:
    _hdr(f"FULL BREAKDOWN — {r['dealer_id']} ({r['dealer_name']})")
    print(json.dumps(r, indent=2, default=str))


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--top", type=int, default=15, help="how many top/bottom dealers to show")
    ap.add_argument("--min-inventory", type=int, default=5, help="min active priced listings to rank")
    ap.add_argument("--breakdown", type=str, default=None, help="dealer_id to fully expand")
    args = ap.parse_args()

    conn = pg_connect()
    try:
        ranked = ds.rank_dealers(conn=conn, min_inventory=args.min_inventory)
    finally:
        conn.close()

    print(f"Ranked {len(ranked):,} dealers with >= {args.min_inventory} active priced listings.")
    scored_rep = sum(1 for r in ranked if r["reputation"]["score"] is not None)
    scored_price = sum(1 for r in ranked if r["pricing_aggressiveness"]["score"] is not None)
    print(f"  {scored_rep:,} have a reputation signal; {scored_price:,} have a pricing signal.")

    _hdr(f"TOP {args.top} DEALERS BY COMPOSITE")
    for r in ranked[: args.top]:
        print(_row(r))

    _hdr(f"BOTTOM {args.top} DEALERS BY COMPOSITE")
    for r in ranked[-args.top :]:
        print(_row(r))

    target = None
    if args.breakdown:
        target = next((r for r in ranked if r["dealer_id"] == args.breakdown), None)
        if target is None:
            print(f"\n(no ranked dealer with id {args.breakdown!r}; showing the top dealer instead)")
    if target is None and ranked:
        target = ranked[0]
    if target is not None:
        _print_breakdown(target)


if __name__ == "__main__":
    main()
