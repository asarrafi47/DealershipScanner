"""
Post-scan dealer inventory reconciliation (soft-unlist stale VINs).

Called from ``scanner.py`` after a successful per-dealer upsert. Rows for VINs no longer
present in the scraped feed are marked ``listing_active = 0`` with ``listing_removed_at``
set; they are excluded from public ``search_cars`` / filter options. Re-upsert clears removal.
"""
from __future__ import annotations

import logging
import os
import re
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlparse

from backend.db.inventory_db import ensure_cars_table_columns, get_conn

logger = logging.getLogger(__name__)

_RE_UNKNOWN_VIN = re.compile(r"^unknown", re.I)


def normalize_scanner_vin(raw: Any) -> str | None:
    """
    Normalize VIN for feed comparison: strip, upper, exactly 17 chars.

    Returns None for invalid / placeholder VINs (non-17-char, ``unknown-*``, etc.).
    """
    if raw is None:
        return None
    s = str(raw).strip().upper()
    if len(s) != 17:
        return None
    if _RE_UNKNOWN_VIN.search(s):
        return None
    if not s.isalnum():
        return None
    return s


def normalized_vin_set_from_vehicles(vehicles: list[dict[str, Any]]) -> set[str]:
    """Build the set of valid normalized VINs from scraped vehicle dicts."""
    out: set[str] = set()
    for v in vehicles:
        nv = normalize_scanner_vin(v.get("vin"))
        if nv:
            out.add(nv)
    return out


def _normalize_dealer_host(dealer_url: str) -> str:
    try:
        h = (urlparse((dealer_url or "").strip()).netloc or "").lower()
        if h.startswith("www."):
            h = h[4:]
        return h
    except ValueError:
        return ""


def _dealer_scope_sql(dealer_id: str, dealer_url: str) -> tuple[str, list[Any]]:
    """
    SQL fragment (no leading AND) + params matching rows owned by this dealer run.

    Primary rule: ``TRIM(dealer_id)`` equals manifest ``dealer_id`` (same convention as
    ``upsert_vehicles`` / dealers.json). This is the normal path for scanner runs.

    Fallback when ``dealer_id`` is empty on the manifest row: match legacy SQLite rows whose
    ``dealer_id`` is blank but ``dealer_url`` shares the same host (scheme-stripped netloc,
    ``www`` removed). Use only when manifest ``dealer_id`` is missing — avoids cross-dealer
    collisions when ``dealer_id`` is populated.
    """
    did = (dealer_id or "").strip()
    if did:
        return "TRIM(IFNULL(dealer_id, '')) = ?", [did]
    host = _normalize_dealer_host(dealer_url)
    if not host:
        return "1 = 0", []
    return (
        "(TRIM(IFNULL(dealer_id, '')) = '' AND LOWER(IFNULL(dealer_url, '')) LIKE ?)",
        [f"%{host}%"],
    )


def _reconcile_enabled() -> bool:
    return os.environ.get("SCANNER_RECONCILE", "1").strip().lower() not in ("0", "false", "no", "off")


def _reconcile_min_rows() -> int:
    raw = (os.environ.get("SCANNER_RECONCILE_MIN_ROWS") or "8").strip()
    try:
        return max(1, int(raw))
    except ValueError:
        return 8


def _reconcile_min_coverage() -> float:
    """Share of the dealer's ACTIVE rows the scrape must re-see before unlisting.

    A partial capture (thin recipe replay, --scan-only on a JS-paginated SRP,
    a feed that answered one section) must not delist the rest of the lot:
    on 2026-09-09 a 443-row Garden Grove Honda capture marked 803 cars inactive.
    ``SCANNER_RECONCILE_MIN_COVERAGE`` (default 0.5); 0 disables the gate.
    """
    raw = (os.environ.get("SCANNER_RECONCILE_MIN_COVERAGE") or "0.5").strip()
    try:
        return min(1.0, max(0.0, float(raw)))
    except ValueError:
        return 0.5


BUCKET_UNKNOWN = "unknown"
CONDITION_BUCKETS = ("new", "used", BUCKET_UNKNOWN)


def condition_bucket(value: Any) -> str:
    """``new`` / ``used`` (certified counts as used) / ``unknown`` when blank.

    The one bucket rule of every retirement writer: this scanner reconcile, the
    delta scan and the dealer pipeline's ``reconcile_dealer`` (P1A.1). Blank is
    ``unknown``, never ``used``: assess's ``rows_used`` counts blanks as used and
    is a verdict input, not a retirement rule.
    """
    c = str(value or "").strip().lower()
    if not c:
        return BUCKET_UNKNOWN
    if c.startswith("new"):
        return "new"
    return "used"


def condition_buckets_from_vehicles(vehicles: list[dict[str, Any]]) -> set[str]:
    """The known condition buckets (``new`` / ``used``) this run actually returned rows for."""
    return {b for b in (condition_bucket(v.get("condition")) for v in vehicles) if b != BUCKET_UNKNOWN}


def _bucket_keep_reasons(
    active_by: dict[str, int],
    matched_by: dict[str, int],
    scraped_conditions: set[str] | None,
    min_cov: float,
) -> dict[str, str]:
    """Why each bucket's unseen rows stay listed (``""`` = the bucket may retire).

    * ``missing_condition``: the run returned no row of that condition (the
      zero-only guard of 2026-09-28).
    * ``low_bucket``: the run re-saw under *min_cov* of that bucket's active rows.
      A lot-wide share hides a one-sided run: 300 new + 5 used re-seen of 300 new
      + 200 used is 61% of the lot and 2.5% of the used cars.
    * Blank-condition rows retire only when both new and used qualify, since
      nothing says which side of the lot they belong to. A caller that passes no
      *scraped_conditions* (legacy, no condition evidence at all) keeps the old
      rule: blank rows are their own bucket under the coverage share.
    """
    def _low(b: str) -> bool:
        return min_cov > 0 and active_by[b] > 0 and matched_by[b] < min_cov * active_by[b]

    why: dict[str, str] = {}
    for b in ("new", "used"):
        if scraped_conditions is not None and b not in scraped_conditions:
            why[b] = "missing_condition"
        elif _low(b):
            why[b] = "low_bucket"
        else:
            why[b] = ""
    if scraped_conditions is None:
        why[BUCKET_UNKNOWN] = "low_bucket" if _low(BUCKET_UNKNOWN) else ""
    else:
        why[BUCKET_UNKNOWN] = why["new"] or why["used"]
    return why


def reconcile_dealer_inventory_after_scan(
    dealer_id: str,
    dealer_url: str,
    scraped_vins: set[str],
    stats: dict[str, Any],
    *,
    scraped_conditions: set[str] | None = None,
    _conn: Any | None = None,
) -> dict[str, Any]:
    """
    Mark active DB rows for this dealer whose VIN is not in *scraped_vins* as inactive.

    *scraped_vins* must already be normalized (``normalize_scanner_vin`` per vehicle).

    Mutates *stats* with ``reconcile`` key for downstream logging (optional).

    Returns a small result dict; skips work when safety gates fail (see logs).
    """
    out: dict[str, Any] = {
        "ran": False,
        "scraped_candidates": len(scraped_vins),
        "marked_inactive": 0,
        "skipped_reason": None,
    }

    def _finish() -> dict[str, Any]:
        stats["reconcile"] = dict(out)
        logger.info(
            "Inventory reconcile %s: scraped_candidates=%s marked_inactive=%s skipped_reason=%s",
            (dealer_id or "").strip() or "?",
            out["scraped_candidates"],
            out["marked_inactive"],
            out["skipped_reason"],
        )
        return out

    if not _reconcile_enabled():
        out["skipped_reason"] = "disabled"
        return _finish()

    deduped = int(stats.get("deduped_rows") or 0)
    min_rows = _reconcile_min_rows()
    if stats.get("error"):
        out["skipped_reason"] = "dealer_error"
        return _finish()
    if deduped < min_rows:
        out["skipped_reason"] = f"below_min_rows(deduped={deduped},min={min_rows})"
        return _finish()
    if not scraped_vins:
        out["skipped_reason"] = "no_valid_scraped_vins"
        return _finish()

    own_close = False
    if _conn is None:
        conn = get_conn()
        own_close = True
    else:
        conn = _conn
    try:
        cur = conn.cursor()
        ensure_cars_table_columns(cur)
        conn.commit()

        scope_sql, scope_params = _dealer_scope_sql(dealer_id, dealer_url)
        now = datetime.now(timezone.utc).replace(microsecond=0).isoformat()

        cur.execute(
            f"""
            SELECT id, vin, condition FROM cars
            WHERE ({scope_sql})
              AND (COALESCE(listing_active, 1) = 1)
              AND LENGTH(TRIM(vin)) = 17
              AND UPPER(TRIM(vin)) NOT LIKE 'UNKNOWN%%'
            """,
            tuple(scope_params),
        )
        rows = cur.fetchall()
        # Active rows (valid VIN) per condition bucket, and how many of them this
        # run re-saw. A one-condition replay (a "new" recipe section, a used-only
        # capture) re-sees half the lot and, at 50% coverage, passes the lot-wide
        # gate below and retires the other half: parksidekia-com 2026-09-28
        # returned 306 new / 0 used and un-listed 206 used cars; covertbuickgmc-com
        # 811 of 2,160. A run that returns a handful of one condition does the
        # same, so every bucket must clear the coverage share on its own (P1A.1).
        active_by: dict[str, int] = dict.fromkeys(CONDITION_BUCKETS, 0)
        matched_by: dict[str, int] = dict.fromkeys(CONDITION_BUCKETS, 0)
        unseen: list[tuple[int, str]] = []
        for row in rows:
            vnorm = normalize_scanner_vin(row[1])
            if not vnorm:
                continue
            bucket = condition_bucket(row[2] if len(row) > 2 else "")
            active_by[bucket] += 1
            if vnorm in scraped_vins:
                matched_by[bucket] += 1
            else:
                unseen.append((int(row[0]), bucket))
        active_known = sum(active_by.values())
        matched = sum(matched_by.values())

        min_cov = _reconcile_min_coverage()
        keep_why = _bucket_keep_reasons(active_by, matched_by, scraped_conditions, min_cov)
        stale_ids: list[int] = []
        kept_condition = 0
        kept_low = 0
        for rid, bucket in unseen:
            why = keep_why[bucket]
            if why == "missing_condition":
                kept_condition += 1
            elif why == "low_bucket":
                kept_low += 1
            else:
                stale_ids.append(rid)
        if kept_condition:
            out["kept_missing_condition"] = kept_condition
            logger.warning(
                "Inventory reconcile %s: run returned no %s rows; keeping %d active row(s) of that condition",
                (dealer_id or "").strip() or "?",
                "/".join(sorted({"new", "used"} - set(scraped_conditions or ()))) or "?",
                kept_condition,
            )
        if kept_low:
            out["kept_low_bucket"] = kept_low
            logger.warning(
                "Inventory reconcile %s: %s re-seen under %.0f%% of its active rows; keeping %d active row(s)",
                (dealer_id or "").strip() or "?",
                ", ".join(f"{b} {matched_by[b]}/{active_by[b]}" for b in CONDITION_BUCKETS if keep_why[b] == "low_bucket"),
                min_cov * 100,
                kept_low,
            )

        if min_cov > 0 and active_known >= min_rows and matched < min_cov * active_known:
            out["skipped_reason"] = (
                f"low_coverage(matched={matched},active={active_known},min={min_cov:.0%})"
            )
            logger.warning(
                "Inventory reconcile %s: scrape re-saw only %d of %d active VIN(s); "
                "refusing to unlist %d row(s) on a partial capture",
                (dealer_id or "").strip() or "?", matched, active_known, len(stale_ids),
            )
            return _finish()

        marked = 0
        batch_size = 400
        for i in range(0, len(stale_ids), batch_size):
            chunk = stale_ids[i : i + batch_size]
            if not chunk:
                continue
            ph = ",".join("?" * len(chunk))
            cur.execute(
                f"""
                UPDATE cars
                SET listing_active = 0, listing_removed_at = ?
                WHERE id IN ({ph})
                """,
                (now, *chunk),
            )
            rc = cur.rowcount
            marked += len(chunk) if rc is None or rc < 0 else int(rc)

        conn.commit()
        out["ran"] = True
        out["marked_inactive"] = marked
        out["skipped_reason"] = "ok"
        return _finish()
    finally:
        if own_close:
            conn.close()
