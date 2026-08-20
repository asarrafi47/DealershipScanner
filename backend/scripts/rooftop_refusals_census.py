"""
Census of the ``rooftop_refusals`` ledger: who refused, why, how long ago, and
whether the refusing dealers still show any active inventory.

    .venv/bin/python -m backend.scripts.rooftop_refusals_census
    .venv/bin/python -m backend.scripts.rooftop_refusals_census --json

The ledger (migrations/V009, written by backend/scanner/rooftop_ledger.py) records
every payload the rooftop attribution gate refused to WRITE. This script is the
summary that has never been run over it. Three readings:

  1. DEALER x REASON — refusal events and refused-row totals grouped by the store
     being scanned and the gate's reason. ``target_rooftop_unidentified`` is a
     statement about OUR roster (we could not tell which rooftop is this store);
     only ``sibling_rooftop`` / ``single_rooftop_is_not_this_store`` are evidence
     about a car (see backend/scanner/rooftop_disown.py EVIDENCE_BACKED_REJECTS).

  2. AGE DISTRIBUTION — how stale the ledger is. A census of week-old refusals
     describes last week's feeds, not today's.

  3. INVENTORY CROSS-REFERENCE — each refused rooftop against its active car
     count. A dealer whose scans refuse everything AND holds zero active cars is
     the "refusals starve the store" failure mode; a dealer refusing thousands of
     rows while still holding inventory is simply being served a group feed.

Strictly read-only: the session is set READ ONLY and nothing is ever un-listed or
modified here. Refuse to WRITE without evidence, but NEVER un-list without it —
a census must not become a cleanup pass.

The sibling readings (unregistered-rooftop gaps, the zero-refusal regression
alarm) live in backend/scripts/report_rooftop_refusals.py.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))


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

    conn = psycopg.connect(_dsn(), connect_timeout=15)
    with conn.cursor() as cur:
        # A census must not become a way to edit history.
        cur.execute("SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY")
    conn.commit()
    return conn


def _as_utc(ts: Any) -> datetime | None:
    """scanned_at as an aware UTC datetime; psycopg hands back aware datetimes,
    anything else is parsed best-effort."""
    if isinstance(ts, datetime):
        return ts.astimezone(timezone.utc) if ts.tzinfo else ts.replace(tzinfo=timezone.utc)
    try:
        dt = datetime.fromisoformat(str(ts))
    except (TypeError, ValueError):
        return None
    return dt.astimezone(timezone.utc) if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


# ---------------------------------------------------------------------------
# Reading 1: dealer x reason
# ---------------------------------------------------------------------------


def dealer_reason_census(conn) -> list[dict[str, Any]]:
    """One row per (dealer, reason): events, refused-row total, distinct rooftop
    names the feed claimed, first/last refusal. Worst (most rows) first."""
    cur = conn.cursor()
    cur.execute(
        "SELECT dealer_id, reason, COUNT(*), COALESCE(SUM(rows_refused), 0), "
        "       MIN(scanned_at), MAX(scanned_at) "
        "FROM rooftop_refusals GROUP BY dealer_id, reason"
    )
    rows = [
        {
            "dealer_id": dealer_id,
            "reason": reason,
            "events": int(events),
            "rows_refused": int(total),
            "first_scanned_at": str(first),
            "last_scanned_at": str(last),
        }
        for dealer_id, reason, events, total, first, last in cur.fetchall()
    ]
    # Distinct rooftops per dealer come from the JSONB, not the grouped query.
    cur.execute("SELECT dealer_id, rooftops_seen FROM rooftop_refusals")
    per_dealer: dict[str, set[str]] = {}
    for dealer_id, seen in cur.fetchall():
        names = seen if isinstance(seen, list) else []
        per_dealer.setdefault(dealer_id, set()).update(str(n) for n in names if str(n).strip())
    for r in rows:
        r["distinct_rooftops_seen"] = len(per_dealer.get(r["dealer_id"], set()))
    rows.sort(key=lambda r: (-r["rows_refused"], r["dealer_id"], r["reason"]))
    return rows


# ---------------------------------------------------------------------------
# Reading 2: age distribution
# ---------------------------------------------------------------------------

_AGE_BUCKETS: tuple[tuple[str, float], ...] = (
    ("< 1 day", 1.0),
    ("1-7 days", 7.0),
    ("7-30 days", 30.0),
    ("30-90 days", 90.0),
    ("> 90 days", float("inf")),
)


def age_distribution(conn, now: datetime | None = None) -> dict[str, Any]:
    """Refusal events (and their refused-row weight) bucketed by age."""
    now = now or datetime.now(timezone.utc)
    cur = conn.cursor()
    cur.execute("SELECT scanned_at, COALESCE(rows_refused, 0) FROM rooftop_refusals")
    buckets = {label: {"events": 0, "rows_refused": 0} for label, _ in _AGE_BUCKETS}
    oldest: datetime | None = None
    newest: datetime | None = None
    for scanned_at, rows_refused in cur.fetchall():
        ts = _as_utc(scanned_at)
        if ts is None:
            continue
        oldest = ts if oldest is None or ts < oldest else oldest
        newest = ts if newest is None or ts > newest else newest
        age_days = (now - ts).total_seconds() / 86400.0
        for label, ceiling in _AGE_BUCKETS:
            if age_days < ceiling:
                buckets[label]["events"] += 1
                buckets[label]["rows_refused"] += int(rows_refused)
                break
    return {
        "buckets": buckets,
        "oldest": oldest.isoformat() if oldest else None,
        "newest": newest.isoformat() if newest else None,
    }


# ---------------------------------------------------------------------------
# Reading 3: refused rooftops vs active inventory
# ---------------------------------------------------------------------------


def inventory_cross_reference(conn) -> list[dict[str, Any]]:
    """Per refused dealer: refusal totals beside the store's active car count.

    ``active_cars == 0`` is the correlation this reading exists to expose: a
    store whose payloads are all refused AND which lists nothing is being
    starved by the gate (or was never really a rooftop of its own), while a
    store refusing rows and still listing cars is just shedding a group feed's
    siblings. Observation only — un-listing on it would need car-level evidence.
    """
    cur = conn.cursor()
    cur.execute(
        "SELECT dealer_id, COUNT(*), COALESCE(SUM(rows_refused), 0), MAX(scanned_at) "
        "FROM rooftop_refusals GROUP BY dealer_id"
    )
    refused = {
        dealer_id: {
            "dealer_id": dealer_id,
            "refusal_events": int(events),
            "rows_refused": int(total),
            "last_refusal_at": str(last),
        }
        for dealer_id, events, total, last in cur.fetchall()
    }
    if not refused:
        return []
    cur.execute(
        "SELECT TRIM(COALESCE(dealer_id, '')), COUNT(*) FROM cars "
        "WHERE COALESCE(listing_active, 1) = 1 AND TRIM(COALESCE(dealer_id, '')) = ANY(%s) "
        "GROUP BY 1",
        (list(refused.keys()),),
    )
    active = {dealer_id: int(n) for dealer_id, n in cur.fetchall()}
    out = []
    for dealer_id, entry in refused.items():
        entry["active_cars"] = active.get(dealer_id, 0)
        entry["zero_inventory"] = entry["active_cars"] == 0
        out.append(entry)
    out.sort(key=lambda e: (-int(e["zero_inventory"]), -e["rows_refused"], e["dealer_id"]))
    return out


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def build_census(conn) -> dict[str, Any]:
    return {
        "dealer_reason": dealer_reason_census(conn),
        "age_distribution": age_distribution(conn),
        "inventory_cross_reference": inventory_cross_reference(conn),
    }


def print_report(census: dict[str, Any], limit: int) -> None:
    rows = census["dealer_reason"]
    total_events = sum(r["events"] for r in rows)
    total_rows = sum(r["rows_refused"] for r in rows)
    dealers = {r["dealer_id"] for r in rows}
    print(
        f"ROOFTOP REFUSALS CENSUS — {total_events} events, {total_rows} rows refused, "
        f"{len(dealers)} dealer(s)"
    )

    print("\nBY DEALER AND REASON (worst first)")
    if not rows:
        print("  (ledger is empty)")
    for r in rows[:limit]:
        print(
            f"  {r['rows_refused']:>7} rows  {r['events']:>4} events  "
            f"{r['distinct_rooftops_seen']:>3} rooftops seen  {r['dealer_id']}  "
            f"[{r['reason']}]  {r['first_scanned_at']} .. {r['last_scanned_at']}"
        )

    ages = census["age_distribution"]
    print("\nAGE DISTRIBUTION (of refusal events)")
    for label, _ in _AGE_BUCKETS:
        b = ages["buckets"][label]
        if b["events"]:
            print(f"  {label:>10}: {b['events']:>4} events / {b['rows_refused']:>6} rows")
    print(f"  span: {ages['oldest']} .. {ages['newest']}")

    xref = census["inventory_cross_reference"]
    zero = [e for e in xref if e["zero_inventory"]]
    print("\nACTIVE-INVENTORY CROSS-REFERENCE (zero-inventory dealers first)")
    for e in xref[:limit]:
        marker = "ZERO INVENTORY" if e["zero_inventory"] else f"{e['active_cars']} active cars"
        print(
            f"  {e['rows_refused']:>7} rows refused  {e['refusal_events']:>4} events  "
            f"{e['dealer_id']}  -> {marker}"
        )
    if xref:
        print(
            f"  {len(zero)} of {len(xref)} refused dealer(s) hold zero active inventory"
            + ("" if zero else " — refusals are shedding group-feed siblings, not starving stores")
        )


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument("--json", action="store_true", help="machine-readable output instead of the report")
    ap.add_argument("--limit", type=int, default=40, help="max rows printed per section (default 40)")
    args = ap.parse_args(argv)

    try:
        conn = _connect()
    except Exception as exc:
        print(f"FATAL: could not open inventory connection: {exc}", file=sys.stderr)
        return 2
    try:
        census = build_census(conn)
    except Exception as exc:
        print(f"FATAL: census failed: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 2
    finally:
        try:
            conn.close()
        except Exception:
            pass

    if args.json:
        print(json.dumps(census, indent=2, default=str))
    else:
        print_report(census, args.limit)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
