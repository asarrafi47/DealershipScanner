"""reconcile_dealer: retire the active rows an accepted, full run did not return.

The one place the pipeline WRITES to cars (UPDATE listing_active = 0). Guarded by the
verdict, an absolute row floor and RECONCILE_MIN_SHARE of the baseline. Moved verbatim from backend/scripts/dealer_pipeline.py (audit F11)."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from backend.scanner.pipeline.constants import MIN_ROWS_UNKNOWN, RECONCILE_MIN_SHARE
from backend.scanner.pipeline.db import _rows


def reconcile_dealer(conn, dealer_id: str, since_iso: str, baseline: int, rows_this_run: int, verdict: str,
                     *, dry_run: bool = False) -> dict[str, Any]:
    """Retire the dealer's active rows this run did not return.

    The scanner has never done this (``listing_removed_at`` was NULL on all
    194,811 active rows on 2026-09-26; 45,265 of them last seen before
    September), so sold cars stay listed and every "listed before" count is
    inflated. Guarded: only after a run the assess step accepted (ok / thin /
    inaccurate — not no_rows / error) that returned at least
    ``RECONCILE_MIN_SHARE`` of the rows seen in the baseline window, so a
    section-scoped or truncated replay can never un-list a lot. Rows are
    retired, never deleted (price history / attribution key off ``cars.id``).
    """
    out = {"eligible": False, "stale": 0, "retired": 0, "reason": ""}
    if verdict not in ("ok", "thin", "inaccurate"):
        out["reason"] = f"verdict {verdict}"
        return out
    # The 30-day baseline is 0 for a dealer nobody scanned lately, and a
    # share-of-zero guard passes anything: on 2026-09-26 an 8-row run retired
    # 382 of Gunn Honda's cars. Fall back to the rows seen in the last 90 days,
    # then to every active row, and always demand an absolute floor.
    if not baseline:
        wider = _rows(conn, "SELECT COUNT(*) AS n FROM cars WHERE dealer_id = ? AND COALESCE(listing_active,1)=1 AND scraped_at < ? AND scraped_at >= ?",
                      (dealer_id, since_iso, (datetime.fromisoformat(since_iso.replace("Z", "+00:00")) - timedelta(days=90)).isoformat()))[0]["n"]
        baseline = int(wider or 0) or int(_rows(conn, "SELECT COUNT(*) AS n FROM cars WHERE dealer_id = ? AND COALESCE(listing_active,1)=1 AND scraped_at < ?",
                                               (dealer_id, since_iso))[0]["n"] or 0)
        out["baseline_fallback"] = baseline
    if rows_this_run < MIN_ROWS_UNKNOWN:
        out["reason"] = f"{rows_this_run} rows < absolute floor {MIN_ROWS_UNKNOWN}"
        return out
    if baseline and rows_this_run < baseline * RECONCILE_MIN_SHARE:
        out["reason"] = f"{rows_this_run} rows < {RECONCILE_MIN_SHARE:.0%} of baseline {baseline}"
        return out
    out["eligible"] = True
    stale = _rows(conn, "SELECT COUNT(*) AS n, MIN(scraped_at) AS oldest FROM cars WHERE dealer_id = ? AND COALESCE(listing_active,1)=1 AND scraped_at < ?",
                  (dealer_id, since_iso))[0]
    out["stale"] = int(stale["n"] or 0)
    out["oldest"] = stale.get("oldest")
    if out["stale"] and not dry_run:
        now = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
        conn.execute("UPDATE cars SET listing_active = 0, listing_removed_at = ? WHERE dealer_id = ? AND COALESCE(listing_active,1)=1 AND scraped_at < ?",
                     (now, dealer_id, since_iso))
        conn.commit()
        out["retired"] = out["stale"]
    return out
