"""Print an inventory aging + price-drop signal report from live Postgres.

Usage::

    PYTHONPATH=. python backend/scripts/report_inventory_signals.py

Read-only. Pulls active + removed listings via
``backend.intelligence.inventory_signals`` loaders and prints:

* days-on-lot distribution (buckets + stats),
* the oldest-sitting active cars (buyer-leverage candidates),
* the biggest recent price drops,
* per-dealer stale share,
* fastest- and slowest-turning models (from removed listings).
"""

from __future__ import annotations

from datetime import datetime, timezone

from backend.intelligence import inventory_signals as sig


def _hdr(title: str) -> None:
    print("\n" + "=" * 72)
    print(title)
    print("=" * 72)


def main() -> None:
    now = datetime.now(timezone.utc)
    active = sig.load_active_cars()
    removed = sig.load_removed_cars()
    print(f"Loaded {len(active):,} active and {len(removed):,} removed listings "
          f"(as of {now.date().isoformat()})")

    # --- aging distribution ---------------------------------------------------
    _hdr("DAYS-ON-LOT DISTRIBUTION (active inventory)")
    dist = sig.aging_distribution(active, now=now)
    b = dist["buckets"]
    total = dist["total"] or 1
    for name in ("fresh", "normal", "stale", "very_stale", "unknown"):
        n = b[name]
        bar = "#" * int(round(40 * n / total))
        print(f"  {name:11s} {n:6,d} ({100*n/total:5.1f}%) {bar}")
    if "days_on_lot" in dist:
        d = dist["days_on_lot"]
        print(f"  days-on-lot -> min {d['min']}  median {d['median']}  "
              f"mean {d['mean']}  max {d['max']}")
    print(f"  stale share (>{sig.STALE_MIN_DAYS}d): {100*dist['stale_share']:.1f}%")

    # --- oldest active cars ---------------------------------------------------
    _hdr("OLDEST ACTIVE LISTINGS (highest buyer leverage)")
    for r in sig.oldest_active(active, now=now, limit=8):
        price = f"${r['price']:,.0f}" if r["price"] else "n/a"
        print(f"  {r['days_on_lot']:3d}d [{r['aging_bucket']:10s}] "
              f"{r['year']} {r['make']} {r['model']:20.20s} {price:>10s}  "
              f"{r['dealer']}")

    # --- price drops ----------------------------------------------------------
    _hdr("PRICE-DROP SUMMARY (active inventory)")
    pd = sig.price_drop_summary(active, now=now)
    print(f"  cars with a markdown: {pd['cars_with_drop']:,} / {pd['total']:,} "
          f"({100*pd['drop_share']:.1f}%)")
    print(f"  recent (<= {sig.DEFAULT_RECENT_WINDOW_DAYS}d) markdowns: {pd['recent_drops']:,}")
    if "avg_drop_pct" in pd:
        print(f"  avg drop {pd['avg_drop_pct']}%   median drop {pd['median_drop_pct']}%")

    _hdr("BIGGEST RECENT PRICE DROPS")
    for r in sig.top_price_drops(active, now=now, limit=10, recent_only=True):
        print(f"  -{r['drop_pct']:5.1f}%  ${r['from_price']:,.0f} -> ${r['to_price']:,.0f}  "
              f"({r['days_ago']}d ago, {r['days_on_lot']}d on lot)  "
              f"{r['year']} {r['make']} {r['model']}  [{r['dealer']}]")

    # --- dealer stale share ---------------------------------------------------
    _hdr("DEALERS WITH THE MOST STALE INVENTORY")
    dealers = sig.stale_share_by_dealer(active, now=now, min_inventory=25)
    if not any(d["stale"] for d in dealers):
        print("  (no dealer currently carries stale >60d inventory; ranking by "
              "median days-on-lot instead)")
        dealers.sort(key=lambda d: (d["median_days_on_lot"] or 0), reverse=True)
    for d in dealers[:8]:
        print(f"  stale {100*d['stale_share']:4.1f}%  median {d['median_days_on_lot']:4}d  "
              f"({d['total']:4,d} cars)  {d['dealer']}")

    # --- turn time ------------------------------------------------------------
    _hdr("FASTEST-TURNING MODELS (from removed listings = demand)")
    for r in sig.fastest_turning_models(removed, min_count=15, limit=10):
        print(f"  {r['median_turn_days']:5.1f}d median  ({r['count']:4,d} sold, "
              f"mean {r['mean_turn_days']}d)  {r['make']} {r['model']}")

    _hdr("SLOWEST-TURNING MODELS")
    for r in sig.fastest_turning_models(removed, min_count=15, limit=8, slowest=True):
        print(f"  {r['median_turn_days']:5.1f}d median  ({r['count']:4,d} sold, "
              f"mean {r['mean_turn_days']}d)  {r['make']} {r['model']}")


if __name__ == "__main__":
    main()
