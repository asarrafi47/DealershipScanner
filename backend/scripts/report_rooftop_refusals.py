"""
Read the ``rooftop_refusals`` ledger: the three readings V009 promised.

    .venv/bin/python -m backend.scripts.report_rooftop_refusals            # all three
    .venv/bin/python -m backend.scripts.report_rooftop_refusals --census   # one section
    .venv/bin/python -m backend.scripts.report_rooftop_refusals --gaps
    .venv/bin/python -m backend.scripts.report_rooftop_refusals --alarm --days 3

The ledger (backend/scanner/rooftop_ledger.py) has been write-only since it shipped:
migrations/V009__rooftop_refusals.sql promised three readings and none was ever built,
so both of its indexes served queries that did not exist. This script is those readings.

  1. CENSUS — "which dealers are being served other people's inventory, worst first."
     A dealer whose scans repeatedly refuse rows naming other rooftops is by definition
     receiving a group feed; the refusal totals rank the misattribution exposure.

  2. GAPS — rooftop identifiers the feeds actually claimed (``rooftops_seen``) that
     match no row in the dealership registry. These are stores whose inventory arrives
     on our doorstep and which we do not even know exist. Identifiers that are street
     addresses or "City, ST" strings are reported with their kind rather than matched:
     the parser captured a location, not a store name, and pretending an address can
     miss the registry would make every address a false gap.

  3. ALARM — a refusal count that drops to zero while scans ran means the gate stopped
     firing: either every group feed was fixed at once, or the gate (or this ledger)
     broke and misattributed rows are flowing through unrecorded. Silence is the one
     state this table exists to make visible.

Dates are embedded as generated literals rather than bound parameters so the same
readers run against Postgres in production and the SQLite harness in tests; every
literal comes from ``strftime`` on a datetime this module computed itself.

EXIT CODES
    0  report printed; if the alarm section ran, the gate is alive (or idle with no
       scan evidence, which is indistinguishable from "nothing ran" and not alarmed)
    2  the run itself failed (no DSN, table missing, connection refused)
    3  the regression alarm FIRED: zero refusals in the window while scans ran
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from backend.db.connect import connect as db_connect  # noqa: E402

ALARM_EXIT_CODE = 3
DEFAULT_ALARM_DAYS = 3


def _connect():
    # Reading history must never become a way to edit it.
    return db_connect(read_only=True)


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


def _parse_seen(value: Any) -> list[str]:
    """``rooftops_seen`` as a list of strings: psycopg hands JSONB back as a list,
    the SQLite test harness hands back the JSON text the ledger wrote."""
    if value is None:
        return []
    if isinstance(value, (list, tuple)):
        return [str(v) for v in value if str(v).strip()]
    try:
        parsed = json.loads(value)
    except (TypeError, ValueError):
        return []
    if isinstance(parsed, list):
        return [str(v) for v in parsed if str(v).strip()]
    return []


def _norm(s: Any) -> str:
    """Case/punctuation-insensitive comparison form of a rooftop or registry name."""
    return re.sub(r"[^a-z0-9]+", " ", str(s or "").lower()).strip()


def _host_key(url: str) -> str:
    """The dealer-id form of a URL's host (``terrylabontechevy-com``), matching how
    scanner dealer ids are derived from registry URLs."""
    host = urlparse(url if "://" in url else f"https://{url}").netloc.lower()
    if host.startswith("www."):
        host = host[4:]
    return host.replace(".", "-")


def identifier_kind(ident: str) -> str:
    """What the feed actually captured for this rooftop.

    Only ``name`` and ``host`` identifiers can meaningfully be matched against the
    registry; an ``address`` or ``locality`` identifier names a place, not a store,
    and matching it would manufacture false gaps.
    """
    s = str(ident or "").strip()
    if "<br" in s.lower() or re.match(r"^\d", s):
        return "address"
    if re.fullmatch(r"[^,\d]+,\s*[A-Za-z]{2}\.?", s):
        return "locality"
    if re.fullmatch(r"[a-z0-9.-]+\.[a-z]{2,}", s.lower()):
        return "host"
    return "name"


def _cutoffs(days: int, now: datetime | None = None) -> tuple[str, str]:
    """(timestamp literal, date literal) for the start of the window, in UTC.

    The timestamp carries an explicit ``+00:00`` so Postgres does not reinterpret it
    in the session timezone; SQLite compares the shared prefix lexically.
    """
    now = now or datetime.now(timezone.utc)
    cutoff = now - timedelta(days=days)
    return cutoff.strftime("%Y-%m-%d %H:%M:%S+00:00"), cutoff.strftime("%Y-%m-%d")


# ---------------------------------------------------------------------------
# Reading 1: the group-feed census, worst first
# ---------------------------------------------------------------------------


def refusal_census(conn) -> list[dict[str, Any]]:
    """Per dealer: total rows refused, refusal events, distinct rooftops the feed
    claimed, last refusal time. Ordered worst (most rows refused) first.

    This is the "which dealers are being served other people's inventory" reading:
    the top of this list is the open misattribution investigation, answered as a
    by-product of scanning.
    """
    cur = conn.cursor()
    cur.execute(
        "SELECT dealer_id, reason, rows_refused, rooftops_seen, scanned_at "
        "FROM rooftop_refusals"
    )
    per: dict[str, dict[str, Any]] = {}
    for dealer_id, reason, rows_refused, seen, scanned_at in cur.fetchall():
        entry = per.setdefault(
            dealer_id,
            {"dealer_id": dealer_id, "rows_refused": 0, "events": 0,
             "rooftops": set(), "reasons": {}, "last_scanned_at": None},
        )
        entry["rows_refused"] += int(rows_refused or 0)
        entry["events"] += 1
        entry["rooftops"].update(_parse_seen(seen))
        entry["reasons"][reason] = entry["reasons"].get(reason, 0) + int(rows_refused or 0)
        ts = str(scanned_at) if scanned_at is not None else None
        if ts and (entry["last_scanned_at"] is None or ts > entry["last_scanned_at"]):
            entry["last_scanned_at"] = ts
    out = []
    for entry in per.values():
        entry["distinct_rooftops"] = len(entry["rooftops"])
        entry["rooftops"] = sorted(entry["rooftops"])
        out.append(entry)
    out.sort(key=lambda e: (-e["rows_refused"], e["dealer_id"]))
    return out


# ---------------------------------------------------------------------------
# Reading 2: rooftops the feeds claimed that the registry does not know
# ---------------------------------------------------------------------------


def registry_identities(conn) -> dict[str, set[str]]:
    """Comparison forms of every active registry rooftop: normalized names and
    host-derived dealer keys (both raw and normalized)."""
    cur = conn.cursor()
    cur.execute(
        "SELECT name, website_url, dealer_website_url FROM dealerships "
        "WHERE COALESCE(is_active, 1) = 1 AND duplicate_of_id IS NULL"
    )
    names: set[str] = set()
    keys: set[str] = set()
    for name, url, alt_url in cur.fetchall():
        n = _norm(name)
        if n:
            names.add(n)
        for u in (url, alt_url):
            if u:
                key = _host_key(str(u))
                if key:
                    keys.add(key)
                    keys.add(_norm(key))
    return {"names": names, "keys": keys}


def unregistered_rooftops(conn) -> list[dict[str, Any]]:
    """Rooftop identifiers appearing in ``rooftops_seen`` with no registry match.

    Returns one row per unmatched identifier: the identifier as the feed spelled
    it, its kind (name / host / address / locality — only the first two were
    matchable), how many refusal events named it, and which of our dealers' scans
    reported it. Sorted by events, most-seen first.
    """
    reg = registry_identities(conn)
    cur = conn.cursor()
    cur.execute("SELECT dealer_id, rooftops_seen FROM rooftop_refusals")
    seen_by: dict[str, dict[str, Any]] = {}
    for dealer_id, seen in cur.fetchall():
        for ident in _parse_seen(seen):
            entry = seen_by.setdefault(
                ident, {"identifier": ident, "events": 0, "dealers": set()}
            )
            entry["events"] += 1
            entry["dealers"].add(dealer_id)

    gaps: list[dict[str, Any]] = []
    for ident, entry in seen_by.items():
        kind = identifier_kind(ident)
        if kind in ("name", "host"):
            forms = {_norm(ident)}
            if kind == "host":
                forms.add(_host_key(ident))
                forms.add(_norm(_host_key(ident)))
            if forms & reg["names"] or forms & reg["keys"]:
                continue  # registered; not a gap
        gaps.append(
            {
                "identifier": ident,
                "kind": kind,
                "events": entry["events"],
                "dealers_reporting": sorted(entry["dealers"]),
            }
        )
    gaps.sort(key=lambda g: (-g["events"], g["identifier"]))
    return gaps


# ---------------------------------------------------------------------------
# Reading 3: the regression alarm — silence while scans ran
# ---------------------------------------------------------------------------


def zero_refusal_alarm(conn, days: int = DEFAULT_ALARM_DAYS,
                       now: datetime | None = None) -> dict[str, Any]:
    """Did the gate go quiet while scanning continued?

    ``alarm`` is True only when the window holds ZERO refusals AND there is scan
    evidence (cars scraped in the window). No scans and no refusals is idleness,
    not breakage, and is deliberately not alarmed — an alarm that fires on a paused
    scanner would be muted within a week and then missed when it mattered.
    """
    ts_cutoff, day_cutoff = _cutoffs(days, now)
    cur = conn.cursor()
    # Internally generated literals (see module docstring): portable across the
    # production Postgres and the SQLite test harness.
    cur.execute(
        f"SELECT COUNT(*) FROM rooftop_refusals WHERE scanned_at >= '{ts_cutoff}'"
    )
    refusals = int(cur.fetchone()[0] or 0)
    cur.execute(
        f"SELECT COUNT(*) FROM cars WHERE substr(scraped_at, 1, 10) >= '{day_cutoff}'"
    )
    scans = int(cur.fetchone()[0] or 0)
    return {
        "window_days": days,
        "refusals_in_window": refusals,
        "cars_scraped_in_window": scans,
        "alarm": refusals == 0 and scans > 0,
    }


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument("--census", action="store_true", help="only the worst-first dealer census")
    ap.add_argument("--gaps", action="store_true", help="only the unregistered-rooftop gap list")
    ap.add_argument("--alarm", action="store_true", help="only the zero-refusals regression alarm")
    ap.add_argument("--days", type=int, default=DEFAULT_ALARM_DAYS,
                    help=f"alarm window in days (default {DEFAULT_ALARM_DAYS})")
    ap.add_argument("--limit", type=int, default=30, help="max census/gap rows printed")
    args = ap.parse_args(argv)

    run_all = not (args.census or args.gaps or args.alarm)

    try:
        conn = _connect()
    except Exception as exc:
        print(f"FATAL: could not open inventory connection: {exc}", file=sys.stderr)
        return 2

    exit_code = 0
    try:
        if run_all or args.census:
            census = refusal_census(conn)
            print("GROUP-FEED CENSUS — dealers served other rooftops' inventory, worst first")
            if not census:
                print("  (no refusals recorded yet)")
            for row in census[: args.limit]:
                top_reason = max(row["reasons"], key=row["reasons"].get) if row["reasons"] else "-"
                print(
                    f"  {row['rows_refused']:>7} rows refused  {row['events']:>4} events  "
                    f"{row['distinct_rooftops']:>3} rooftops seen  {row['dealer_id']}  "
                    f"[{top_reason}]  last={row['last_scanned_at']}"
                )

        if run_all or args.gaps:
            gaps = unregistered_rooftops(conn)
            named = [g for g in gaps if g["kind"] in ("name", "host")]
            located = [g for g in gaps if g["kind"] not in ("name", "host")]
            print("\nUNREGISTERED ROOFTOPS — feed-claimed stores with no registry row")
            if not named:
                print("  (none: every named rooftop the feeds claimed is registered)")
            for g in named[: args.limit]:
                print(
                    f"  {g['events']:>5}x  {g['identifier']}  "
                    f"(reported by {', '.join(g['dealers_reporting'][:3])})"
                )
            if located:
                print(
                    f"  ... plus {len(located)} location-only identifiers (address or "
                    f"'City, ST') the parser captured without a store name; those "
                    f"cannot be matched to the registry and are a capture gap, not a "
                    f"registry gap"
                )

        if run_all or args.alarm:
            verdict = zero_refusal_alarm(conn, days=args.days)
            print(
                f"\nREGRESSION ALARM — window {verdict['window_days']}d: "
                f"{verdict['refusals_in_window']} refusals, "
                f"{verdict['cars_scraped_in_window']} cars scraped"
            )
            if verdict["alarm"]:
                print(
                    "ALARM: the rooftop gate recorded ZERO refusals while scans ran. "
                    "Either every group feed was fixed at once, or the gate/ledger "
                    "broke and misattributed rows are flowing through unrecorded. "
                    "Check backend/parsers resolve_rooftop_attribution and "
                    "backend/scanner/rooftop_ledger.py before trusting attribution."
                )
                exit_code = ALARM_EXIT_CODE
            elif verdict["cars_scraped_in_window"] == 0:
                print("  quiet, but no scan evidence in the window either — nothing ran; not alarmed")
            else:
                print("  gate is alive")
    except Exception as exc:
        print(f"FATAL: report failed: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 2
    finally:
        try:
            conn.close()
        except Exception:
            pass
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
