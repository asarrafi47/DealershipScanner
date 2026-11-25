"""Dealership registry linkage + scan-run bookkeeping."""
import json
import logging
import sqlite3
import sys
from typing import Any
from urllib.parse import urlparse

from backend.db.repositories.base_repo import _placeholders, db_conn
from backend.db.repositories.schema_repo import ensure_scan_runs_table

_log = logging.getLogger(__name__)


def record_scan_outcomes(outcomes: list[Any], *, finished_at: str) -> int:
    """
    Persist per-dealer scanner ``run_dealer`` result dicts into ``scan_runs``.

    Safe to call with empty list; ignores non-dict entries. Returns rows inserted.
    """
    if not outcomes:
        return 0
    n = 0
    with db_conn() as conn:
        cur = conn.cursor()
        ensure_scan_runs_table(cur)
        for o in outcomes:
            if not isinstance(o, dict):
                continue
            did = str(o.get("dealer_id") or "").strip()
            if not did:
                continue
            inv_recovery = o.get("inventory_recovery") if isinstance(o.get("inventory_recovery"), dict) else {}
            summary = {
                "inventory_rows": o.get("inventory_rows"),
                "deduped_rows": o.get("deduped_rows"),
                "vdps_visited": o.get("vdps_visited"),
                "vehicles_vdp_enriched": o.get("vehicles_vdp_enriched"),
                "gallery_vdp_urls_added": o.get("gallery_vdp_urls_added"),
                "gallery_vision": o.get("gallery_vision"),
                "monroney_vision": o.get("monroney_vision"),
                "reconcile": o.get("reconcile"),
                "phase_secs": o.get("phase_secs"),
                "vins_count": len(o.get("vins") or []) if isinstance(o.get("vins"), list) else None,
                "recovery_winning_strategy": inv_recovery.get("winning_strategy"),
                "recipe_coverage": o.get("recipe_coverage"),
                "vdp_phase_timed_out": bool(o.get("vdp_phase_timed_out")),
                "description_probe": o.get("description_probe"),
                "vdp_prefetch": o.get("vdp_prefetch"),
                "capture_coverage": o.get("capture_coverage"),
                "vin_facts": o.get("vin_facts"),
                "filtered_count": o.get("filtered_count"),
                "intercept_count": o.get("intercept_count"),
                "recipe_fetch": o.get("recipe_fetch"),
                "http_only": bool(o.get("http_only")),
                # VIN ownership guard (2026-09-29): VINs this scan found but did not
                # write because another dealer owns them (active, fresh).
                "vin_owner_conflicts": int(o.get("vin_owner_conflicts") or 0),
                "vin_owner_conflict_owners": o.get("vin_owner_conflict_owners") or {},
            }
            err = o.get("error")
            err_s = str(err)[:2000] if err else None
            cur.execute(
                """
                INSERT INTO scan_runs (
                    dealer_id, dealer_name, finished_at, duration_seconds,
                    upserted, inventory_rows, deduped_rows, vdps_visited,
                    vehicles_vdp_enriched, error, provider, summary_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    did,
                    (o.get("dealer_name") or "")[:500] or None,
                    finished_at,
                    float(o.get("seconds") or 0.0),
                    int(o.get("upserted") or 0),
                    int(o.get("inventory_rows") or 0),
                    int(o.get("deduped_rows") or 0),
                    int(o.get("vdps_visited") or 0),
                    int(o.get("vehicles_vdp_enriched") or 0),
                    err_s,
                    (o.get("provider") or "")[:100] or None,
                    json.dumps(summary, ensure_ascii=False, default=str),
                ),
            )
            n += 1
        conn.commit()
    return n


def list_scan_runs(*, dealer_id: str | None = None, limit: int = 50) -> list[dict[str, Any]]:
    """Recent scan rows (newest first), optionally filtered by ``dealer_id``."""
    lim = max(1, min(200, int(limit)))
    with db_conn(row_factory=sqlite3.Row) as conn:
        cur = conn.cursor()
        ensure_scan_runs_table(cur)
        if dealer_id and str(dealer_id).strip():
            cur.execute(
                f"""
                SELECT * FROM scan_runs
                WHERE dealer_id = ?
                ORDER BY datetime(finished_at) DESC, id DESC
                LIMIT ?
                """,
                (str(dealer_id).strip(), lim),
            )
        else:
            cur.execute(
                f"""
                SELECT * FROM scan_runs
                ORDER BY datetime(finished_at) DESC, id DESC
                LIMIT ?
                """,
                (lim,),
            )
        rows = [dict(r) for r in cur.fetchall()]
    return rows


def dealer_last_scraped_at(dealer_id: str | None) -> str | None:
    """Newest ``cars.scraped_at`` over one dealer's rows (active or not), or ``None``.

    The "Inventory last checked" line on the dealer and car pages (N3.11). One
    read per page, served by ``idx_cars_dealer_listing`` (``dealer_id`` leads it,
    on both backends). ``scraped_at`` is TEXT on both backends and the scanner
    writes it as ``isoformat() + "Z"``, so MAX over the strings is MAX over time.
    The key and its lower-case form both match, as in ``cards_for_dealer``, so a
    dealer page opened with a mixed-case key finds the same rows as its grid.
    """
    did = str(dealer_id or "").strip()
    if not did:
        return None
    variants = tuple(sorted({did, did.lower()}))
    try:
        with db_conn() as conn:
            cur = conn.cursor()
            cur.execute(
                f"SELECT MAX(scraped_at) FROM cars WHERE dealer_id IN ({_placeholders(variants)})",
                variants,
            )
            row = cur.fetchone()
    except sqlite3.OperationalError as ex:
        # Same carve-out as get_car_by_id: an uninitialised SQLite file has no
        # cars table, so it has no scan date either. Anything else is a real error.
        if "no such table" not in str(ex).lower():
            raise
        return None
    value = row[0] if row else None
    return str(value) if value else None


def link_cars_to_dealership_registry(
    registry_id: int,
    website_url: str,
    *,
    dealer_id_slug: str | None = None,
) -> int:
    """
    Attach scraped cars to a Smart Import dealership row.

    Matches on ``dealer_url`` (normalized host / URL variants) and, when given, on
    ``dealer_id`` (same slug as ``dealers.json`` / ``scanner.js``), so links succeed even if
    URL text differs between Node insert and the registry row.
    """
    if not website_url or not registry_id:
        return 0
    w = (website_url or "").strip()
    base = w.rstrip("/")
    w_lower = w.lower()
    base_lower = base.lower()
    base_slash_lower = (base_lower + "/") if not base_lower.endswith("/") else base_lower
    host = ""
    try:
        host = (urlparse(w).netloc or "").lower().replace("www.", "")
    except ValueError:
        pass
    did = (dealer_id_slug or "").strip()
    total = 0
    with db_conn() as conn:
        cursor = conn.cursor()
        if host:
            cursor.execute(
                """
                UPDATE cars
                SET dealership_registry_id = ?
                WHERE dealership_registry_id IS NULL
                  AND (
                    LOWER(TRIM(dealer_url)) IN (?, ?, ?)
                    OR LOWER(dealer_url) LIKE ?
                  )
                """,
                (
                    registry_id,
                    w_lower,
                    base_lower,
                    base_slash_lower,
                    f"%{host}%",
                ),
            )
        else:
            cursor.execute(
                """
                UPDATE cars
                SET dealership_registry_id = ?
                WHERE dealership_registry_id IS NULL
                  AND LOWER(TRIM(dealer_url)) IN (?, ?)
                """,
                (registry_id, w_lower, base_lower),
            )
        total += cursor.rowcount
        if did:
            cursor.execute(
                """
                UPDATE cars
                SET dealership_registry_id = ?
                WHERE dealership_registry_id IS NULL
                  AND TRIM(dealer_id) = ?
                """,
                (registry_id, did),
            )
            total += cursor.rowcount
        conn.commit()
    return int(total)


# The process-once flag is owned by the facade (``backend.db.inventory_db``) so that
# tests setting ``inventory_db._registry_backfill_ran = True`` keep working; this
# module-level value is only the fallback when the facade has not been imported.
_registry_backfill_ran = False


def _registry_backfill_flag_module():
    facade = sys.modules.get("backend.db.inventory_db")
    return facade if facade is not None else sys.modules[__name__]


_LIKE_SPECIALS = frozenset("%_\\")


def _hosts_named_by_unstamped_cars(cursor) -> set[str]:
    """Hosts that appear after ``//`` in the ``dealer_url`` of an active, unstamped car.

    A token is the text between a ``//`` and the next ``/`` (or the end), with and without a
    leading ``www.``. Every pattern from :func:`dealer_url_like_patterns` (``%//h``, ``%//h/%``,
    ``%//www.h``, ``%//www.h/%``) can only match a URL that has such a token equal to ``h`` or
    ``www.h``, so a host missing from this set cannot match any unstamped car.
    """
    cursor.execute(
        """
        SELECT DISTINCT LOWER(COALESCE(dealer_url, '')) AS u
        FROM cars
        WHERE (COALESCE(listing_active, 1) = 1)
          AND (dealership_registry_id IS NULL
               OR CAST(dealership_registry_id AS INTEGER) <= 0)
        """
    )
    named: set[str] = set()
    for row in cursor.fetchall():
        url = row.get("u") if isinstance(row, dict) else row[0]
        url = str(url or "")
        i = url.find("//")
        while i != -1:
            start = i + 2
            end = url.find("/", start)
            token = url[start : end if end != -1 else len(url)]
            if token:
                named.add(token)
                if token.startswith("www."):
                    named.add(token[4:])
            i = url.find("//", i + 1)
    return named


def _host_may_match(host: str, named_hosts: set[str]) -> bool:
    """False only when the host's LIKE patterns provably match no unstamped car."""
    h = (host or "").strip().lower()
    if any(ch in _LIKE_SPECIALS for ch in h):
        return True  # a wildcard or escape in the host: let SQL decide, as before
    return h in named_hosts


def backfill_dealership_registry_ids(*, conn=None) -> int:
    """
    Set ``dealership_registry_id`` on active cars where host matches registry URLs.

    Idempotent; safe to run after scans or before listings geo load.
    """
    from backend.listings.dealer_registry_match import (
        dealer_url_like_patterns,
        registry_id_by_dealer_host,
    )

    total = 0
    if conn is not None:
        host_to_reg = registry_id_by_dealer_host(conn)
        cursor = conn.cursor()
        # One pass over ``cars`` to learn which hosts the still-unstamped rows name. Each
        # per-host UPDATE below is a full scan of ``cars`` (``LOWER(dealer_url) LIKE '%//host'``
        # cannot use an index), and ~620 of them ran on every worker start even when nothing was
        # left to stamp -- minutes of sequential scans on the shared database after each
        # deploy. A host no unstamped URL names matches zero rows, so its UPDATE is skipped.
        named_hosts = _hosts_named_by_unstamped_cars(cursor)
        for host, reg_id in host_to_reg.items():
            patterns = dealer_url_like_patterns(host)
            if not patterns:
                continue
            if not _host_may_match(host, named_hosts):
                continue
            # Anchored, not a bare ``%host%`` substring: registry id 170 is "Subaru of
            # America" at subaru.com, and the substring form stamped every *subaru.com
            # rooftop's cars with it (Irvine Subaru's 41 listings landed on a New Jersey
            # HQ row, taking the dealer off the picker entirely).
            where = " OR ".join("LOWER(IFNULL(dealer_url, '')) LIKE ?" for _ in patterns)
            cursor.execute(
                f"""
                UPDATE cars
                SET dealership_registry_id = ?
                WHERE (COALESCE(listing_active, 1) = 1)
                  AND (dealership_registry_id IS NULL
                       OR CAST(dealership_registry_id AS INTEGER) <= 0)
                  AND ({where})
                """,
                (reg_id, *patterns),
            )
            total += int(cursor.rowcount or 0)
        conn.commit()
        return total

    with db_conn() as c:
        return backfill_dealership_registry_ids(conn=c)


def ensure_dealership_registry_backfill() -> int:
    """Run host→registry backfill once per process (listings geo / first search)."""
    holder = _registry_backfill_flag_module()
    if getattr(holder, "_registry_backfill_ran", False):
        return 0
    holder._registry_backfill_ran = True
    try:
        n = backfill_dealership_registry_ids()
        if n:
            _log.info("Backfilled dealership_registry_id on %s listing(s)", n)
        return n
    except Exception as e:
        _log.warning("dealership_registry backfill skipped: %s", e)
        return 0
