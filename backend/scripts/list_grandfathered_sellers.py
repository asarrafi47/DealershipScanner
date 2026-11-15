"""List the accounts card S19's GRANDFATHER_CUTOFF rule grandfathers (read only).

Run by the owner inside the live web container, where the database address is already in the
environment (``INVENTORY_DATABASE_URL``, else ``DATABASE_URL``)::

    railway ssh -s web "python -m backend.scripts.list_grandfathered_sellers"

A grandfathered account had at least one ``dealer_vehicles`` row created before the cut-off. While
``MARKETPLACE_POSTING_ENABLED`` is off, only these accounts' cars reach the public site. The report
also lists accounts made after the cut-off that still have live public cars: their next save takes
those cars off the public site (or suspend them in /admin/users to do it at once).

Safety: standard library plus psycopg only; the session and the transaction are READ ONLY; every
statement is a SELECT; it prints account ids and counts only (no names, emails, phone numbers,
VINs or the database address). It never writes, creates or drops anything.
"""

from __future__ import annotations

import os
import sys
from datetime import datetime, timezone

# Must equal backend.dealer.portal_sync.GRANDFATHER_CUTOFF (a test checks). Copied, not imported,
# so this script needs nothing but the standard library and psycopg.
GRANDFATHER_CUTOFF = datetime(2026, 11, 15, 0, 0, 0, tzinfo=timezone.utc)
PORTAL_PREFIX = "dealer-portal-"

# Every statement this script runs. Only SELECT (a test checks there is no write or DDL).
SQL_VEHICLE_ROWS = "SELECT user_id, created_at FROM dealer_vehicles"
SQL_LIVE_PUBLIC = (
    "SELECT dealer_id, COUNT(*) FROM cars "
    "WHERE dealer_id LIKE 'dealer-portal-%' AND listing_active = 1 GROUP BY dealer_id"
)
STATEMENTS = (SQL_VEHICLE_ROWS, SQL_LIVE_PUBLIC)


def _parse_created_at(raw: object) -> datetime | None:
    if isinstance(raw, datetime):
        dt = raw
    else:
        text = str(raw or "").strip()
        if not text:
            return None
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            dt = datetime.fromisoformat(text)
        except ValueError:
            return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def summarise(vehicle_rows: list[tuple], live_rows: list[tuple]) -> dict:
    """Pure: the report from the two query results (tests call this without a database)."""
    before: dict[int, int] = {}
    total: dict[int, int] = {}
    unreadable = 0
    for user_id, created_at in vehicle_rows:
        try:
            uid = int(user_id)
        except (TypeError, ValueError):
            continue
        total[uid] = total.get(uid, 0) + 1
        dt = _parse_created_at(created_at)
        if dt is None:
            unreadable += 1
            continue
        if dt < GRANDFATHER_CUTOFF:
            before[uid] = before.get(uid, 0) + 1
    live: dict[int, int] = {}
    for dealer_id, count in live_rows:
        text = str(dealer_id or "")
        if not text.startswith(PORTAL_PREFIX):
            continue
        tail = text[len(PORTAL_PREFIX):]
        if not (tail.isascii() and tail.isdigit()):
            continue
        live[int(tail)] = int(count or 0)
    grandfathered = sorted(before)
    after_with_live = sorted(uid for uid in live if uid not in before)
    return {
        "grandfathered": [
            {"user_id": uid, "rows_before_cutoff": before[uid], "rows_now": total.get(uid, 0),
             "live_public_cars": live.get(uid, 0)}
            for uid in grandfathered
        ],
        "not_grandfathered_with_live_cars": [
            {"user_id": uid, "rows_now": total.get(uid, 0), "live_public_cars": live[uid]}
            for uid in after_with_live
        ],
        "accounts_with_rows": len(total),
        "unreadable_dates": unreadable,
    }


def render(report: dict) -> str:
    lines = [f"Cut-off: {GRANDFATHER_CUTOFF.isoformat()}"]
    lines.append(f"Accounts with saved cars: {report['accounts_with_rows']}")
    lines.append(f"Grandfathered accounts: {len(report['grandfathered'])}")
    for r in report["grandfathered"]:
        lines.append(
            f"  account {r['user_id']}: {r['rows_before_cutoff']} cars saved before the cut-off, "
            f"{r['rows_now']} saved now, {r['live_public_cars']} live on the site"
        )
    lines.append(
        f"Accounts after the cut-off with live public cars: {len(report['not_grandfathered_with_live_cars'])}"
    )
    for r in report["not_grandfathered_with_live_cars"]:
        lines.append(f"  account {r['user_id']}: {r['rows_now']} saved now, {r['live_public_cars']} live on the site")
    if report["unreadable_dates"]:
        lines.append(f"Rows with an unreadable date (not counted): {report['unreadable_dates']}")
    return "\n".join(lines)


def _dsn() -> str:
    for name in ("INVENTORY_DATABASE_URL", "DATABASE_URL"):
        value = (os.environ.get(name) or "").strip()
        if value:
            return value
    return ""


def main() -> int:
    dsn = _dsn()
    if not dsn:
        print("No database address in INVENTORY_DATABASE_URL or DATABASE_URL.", file=sys.stderr)
        return 2
    import psycopg  # noqa: PLC0415 - only needed when run for real

    with psycopg.connect(dsn, connect_timeout=10) as conn:
        conn.read_only = True  # READ ONLY session, before the first statement
        with conn.cursor() as cur:
            cur.execute("SET TRANSACTION READ ONLY")
            cur.execute(SQL_VEHICLE_ROWS)
            vehicle_rows = list(cur.fetchall())
            cur.execute(SQL_LIVE_PUBLIC)
            live_rows = list(cur.fetchall())
        conn.rollback()
    print(render(summarise(vehicle_rows, live_rows)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
