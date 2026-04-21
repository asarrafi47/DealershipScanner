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


def reconcile_dealer_inventory_after_scan(
    dealer_id: str,
    dealer_url: str,
    scraped_vins: set[str],
    stats: dict[str, Any],
    *,
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
            SELECT id, vin FROM cars
            WHERE ({scope_sql})
              AND (COALESCE(listing_active, 1) = 1)
              AND LENGTH(TRIM(vin)) = 17
              AND UPPER(TRIM(vin)) NOT LIKE 'UNKNOWN%%'
            """,
            tuple(scope_params),
        )
        rows = cur.fetchall()
        stale_ids: list[int] = []
        for row in rows:
            rid = int(row[0])
            vnorm = normalize_scanner_vin(row[1])
            if not vnorm or vnorm in scraped_vins:
                continue
            stale_ids.append(rid)

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
