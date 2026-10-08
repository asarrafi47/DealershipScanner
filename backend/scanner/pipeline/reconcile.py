"""reconcile_dealer: retire the active rows an accepted, full run did not return.

The one place the pipeline WRITES to cars (UPDATE listing_active = 0). Guarded by the
verdict, an absolute row floor, RECONCILE_MIN_SHARE of the baseline and, per condition
bucket (P1A.1), the same share of that bucket's own baseline. Moved verbatim from
backend/scripts/dealer_pipeline.py (audit F11)."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from backend.scanner.inventory_reconcile import BUCKET_UNKNOWN, CONDITION_BUCKETS, condition_bucket
from backend.scanner.pipeline.constants import MIN_ROWS_UNKNOWN, RECONCILE_MIN_SHARE
from backend.scanner.pipeline.db import _rows

_UPDATE_CHUNK = 400


def _days_before(iso: str, days: int) -> str:
    return (datetime.fromisoformat(iso.replace("Z", "+00:00")) - timedelta(days=days)).isoformat()


def bucket_counts(conn, dealer_id: str, *, since_iso: str | None = None, before_iso: str | None = None) -> dict[str, int]:
    """The dealer's active rows per condition bucket, scraped in ``[since_iso, before_iso)``
    (either bound open when ``None``): ``{"new": n, "used": n, "unknown": n}``.

    Grouped by the stored ``condition`` and folded through the scanner's
    ``condition_bucket``, so this path and inventory_reconcile can never disagree
    on what a bucket is (blank is ``unknown``; assess's ``rows_used`` counts it as
    used and is not a retirement input)."""
    sql = "SELECT condition, COUNT(*) AS n FROM cars WHERE dealer_id = ? AND COALESCE(listing_active,1)=1"
    params: list[Any] = [dealer_id]
    if since_iso:
        sql += " AND scraped_at >= ?"
        params.append(since_iso)
    if before_iso:
        sql += " AND scraped_at < ?"
        params.append(before_iso)
    out = dict.fromkeys(CONDITION_BUCKETS, 0)
    for r in _rows(conn, sql + " GROUP BY condition", tuple(params)):
        out[condition_bucket(r.get("condition"))] += int(r.get("n") or 0)
    return out


def reconcile_dealer(conn, dealer_id: str, since_iso: str, baseline: int, rows_this_run: int, verdict: str,
                     *, dry_run: bool = False, baseline_buckets: dict[str, int] | None = None,
                     rows_buckets: dict[str, int] | None = None) -> dict[str, Any]:
    """Retire the dealer's active rows this run did not return.

    The scanner has never done this (``listing_removed_at`` was NULL on all
    194,811 active rows on 2026-09-26; 45,265 of them last seen before
    September), so sold cars stay listed and every "listed before" count is
    inflated. Guarded: only after a run the assess step accepted (ok / thin /
    inaccurate — not no_rows / error) that returned at least
    ``RECONCILE_MIN_SHARE`` of the rows seen in the baseline window, so a
    section-scoped or truncated replay can never un-list a lot. Rows are
    retired, never deleted (price history / attribution key off ``cars.id``).

    *baseline_buckets* (the pre-run rows per condition bucket over the same
    window as *baseline*) and *rows_buckets* (the run's own rows per bucket) turn
    on the per-bucket guard (P1A.1): see :func:`_retire_by_bucket`. Every
    production call site passes both; a call with neither keeps the whole-lot
    behaviour of 2026-09-26 for callers that predate condition buckets.
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
                      (dealer_id, since_iso, _days_before(since_iso, 90)))[0]["n"]
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
    if baseline_buckets is not None or rows_buckets is not None:
        return _retire_by_bucket(conn, dealer_id, since_iso, out, baseline_buckets or {}, rows_buckets, dry_run=dry_run)
    if out["stale"] and not dry_run:
        now = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
        conn.execute("UPDATE cars SET listing_active = 0, listing_removed_at = ? WHERE dealer_id = ? AND COALESCE(listing_active,1)=1 AND scraped_at < ?",
                     (now, dealer_id, since_iso))
        conn.commit()
        out["retired"] = out["stale"]
    return out


def _retire_by_bucket(conn, dealer_id: str, since_iso: str, out: dict[str, Any], baseline_buckets: dict[str, int],
                      rows_buckets: dict[str, int] | None, *, dry_run: bool) -> dict[str, Any]:
    """The per-bucket guard (P1A.1). A lot-wide share hides a one-sided run: on
    2026-09-28 parksidekia-com returned 306 new / 0 used and the pipeline, after
    the scanner had kept the used cars, retired them anyway (12-13 one-condition
    batches, 1,399-1,480 rows never restored).

    * ``new`` / ``used`` retire only when the run returned rows of that bucket
      AND at least ``RECONCILE_MIN_SHARE`` of the bucket's baseline. A bucket
      whose baseline window is empty falls back to its last 90 days, then to
      all its stale rows, like the lot-wide baseline (Gunn Honda).
    * Blank-condition rows retire only when both new and used qualify.
    * Kept rows are counted as ``kept_missing_condition`` (the run returned none
      of that condition) or ``kept_low_bucket`` (too few of it).
    The UPDATE names the ids and repeats the stale predicate, so a row another
    process refreshed in between is never retired."""
    rows_b = rows_buckets if rows_buckets is not None else bucket_counts(conn, dealer_id, since_iso=since_iso)
    rows_b = {b: int(rows_b.get(b) or 0) for b in CONDITION_BUCKETS}
    base = {b: int(baseline_buckets.get(b) or 0) for b in CONDITION_BUCKETS}
    stale_ids: dict[str, list[int]] = {b: [] for b in CONDITION_BUCKETS}
    for r in _rows(conn, "SELECT id, condition FROM cars WHERE dealer_id = ? AND COALESCE(listing_active,1)=1 AND scraped_at < ?",
                   (dealer_id, since_iso)):
        stale_ids[condition_bucket(r.get("condition"))].append(int(r["id"]))
    fallback: dict[str, int] = {}
    for b in ("new", "used"):
        if not base[b] and stale_ids[b]:
            wider = bucket_counts(conn, dealer_id, since_iso=_days_before(since_iso, 90), before_iso=since_iso)[b]
            base[b] = int(wider or 0) or len(stale_ids[b])
            fallback[b] = base[b]
    why: dict[str, str] = {}
    for b in ("new", "used"):
        if rows_b[b] <= 0:
            why[b] = "missing_condition"
        elif rows_b[b] < RECONCILE_MIN_SHARE * base[b]:
            why[b] = "low_bucket"
        else:
            why[b] = ""
    why[BUCKET_UNKNOWN] = why["new"] or why["used"]
    retire = [i for b in CONDITION_BUCKETS if not why[b] for i in stale_ids[b]]
    out["kept_missing_condition"] = sum(len(stale_ids[b]) for b in CONDITION_BUCKETS if why[b] == "missing_condition")
    out["kept_low_bucket"] = sum(len(stale_ids[b]) for b in CONDITION_BUCKETS if why[b] == "low_bucket")
    out["retirable"] = len(retire)
    out["buckets"] = {b: {"baseline": base[b], "rows": rows_b[b], "stale": len(stale_ids[b]), "kept": why[b] or None}
                      for b in CONDITION_BUCKETS}
    if fallback:
        out["bucket_baseline_fallback"] = fallback
    if retire and not dry_run:
        now = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
        retired = 0
        for i in range(0, len(retire), _UPDATE_CHUNK):
            chunk = retire[i:i + _UPDATE_CHUNK]
            cur = conn.execute(
                f"UPDATE cars SET listing_active = 0, listing_removed_at = ? WHERE id IN ({','.join('?' * len(chunk))}) "
                "AND dealer_id = ? AND COALESCE(listing_active,1)=1 AND scraped_at < ?",
                (now, *chunk, dealer_id, since_iso))
            rc = getattr(cur, "rowcount", -1)
            retired += len(chunk) if rc is None or rc < 0 else int(rc)
        conn.commit()
        out["retired"] = retired
    return out
