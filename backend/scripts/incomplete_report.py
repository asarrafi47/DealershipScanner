"""
Incomplete-listing rates split by the new-arrival window (N6.4, D-N13).

    .venv/bin/python -m backend.scripts.incomplete_report --window 3

Read-only. For every active car the window test is the D-N13 one: the car is inside the
window when (its latest ``scraped_at`` - its ``first_seen_at``) <= N days, measured
against the row's own latest scrape and never the wall clock. The report prints the
inside-window and outside-window rates separately; later phases quote the OUTSIDE number
(inside-window cars are mostly waiting on a price the dealer has not posted yet).

  incomplete  = indexed with a gap that is not a pending new arrival
  pending     = indexed, price/photo gap only, inside the window (shown, "details coming")
  rate        = (incomplete + pending) / cars, so the inside-window figure stays comparable
                with the pre-N6.4 number; "incomplete only" is printed beside it.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from backend.utils.listing_completeness import (  # noqa: E402
    PENDING_WINDOW_DAYS,
    inside_new_arrival_window,
)


def compute_report(cars: list[tuple], index: dict[int, int], window_days: int) -> dict:
    """``cars`` = (id, first_seen_at, scraped_at) of active cars; ``index`` = {car_id: pending 0/1}."""
    out = {
        "window_days": window_days,
        "inside": {"cars": 0, "incomplete": 0, "pending": 0},
        "outside": {"cars": 0, "incomplete": 0, "pending": 0},
    }
    for cid, first_seen, scraped in cars:
        bucket = out["inside" if inside_new_arrival_window(first_seen, scraped, window_days) else "outside"]
        bucket["cars"] += 1
        if cid in index:
            bucket["pending" if index[cid] else "incomplete"] += 1
    for bucket in (out["inside"], out["outside"]):
        n = bucket["cars"]
        bucket["rate_pct"] = round(100.0 * (bucket["incomplete"] + bucket["pending"]) / n, 2) if n else 0.0
        bucket["incomplete_only_pct"] = round(100.0 * bucket["incomplete"] / n, 2) if n else 0.0
    return out


def format_report(rep: dict) -> str:
    lines = [f"New-arrival window: {rep['window_days']} day(s), measured against each row's latest scrape"]
    for name in ("inside", "outside"):
        b = rep[name]
        lines.append(
            f"  {name:<8} cars={b['cars']:>7}  incomplete={b['incomplete']:>6}  pending={b['pending']:>6}  "
            f"rate={b['rate_pct']:.2f}%  incomplete-only={b['incomplete_only_pct']:.2f}%"
        )
    return "\n".join(lines)


def load_inputs() -> tuple[list[tuple], dict[int, int]]:
    """Active cars and the index, by plain SELECTs (nothing is written)."""
    from backend.db import incomplete_listings_db as ild
    from backend.db.inventory_db import get_conn as inv_get_conn

    inv = inv_get_conn()
    try:
        cur = inv.cursor()
        cur.execute(
            "SELECT id, first_seen_at, scraped_at FROM cars WHERE COALESCE(listing_active, 1) = 1"
        )
        cars = [(int(r[0]), r[1], r[2]) for r in cur.fetchall()]
    finally:
        inv.close()
    conn = ild.get_conn()
    try:
        ild._ensure_schema(conn)
        cur = conn.cursor()
        cur.execute("SELECT car_id, COALESCE(pending, 0) FROM incomplete_listings")
        index = {int(r[0]): int(r[1]) for r in cur.fetchall()}
    finally:
        conn.close()
    return cars, index


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--window", type=int, default=PENDING_WINDOW_DAYS, help="new-arrival window in days")
    args = ap.parse_args(argv)
    cars, index = load_inputs()
    print(format_report(compute_report(cars, index, args.window)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
