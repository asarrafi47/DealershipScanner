"""reconcile_dealer: REPORT which active rows an accepted, full run did not return (P5A.7).

The pipeline no longer writes to cars. Retirement has one writer, the scanner's reconcile
(``plan_retirement`` + two-strike, P5A.1-P5A.4); this module re-derives the old guards
(verdict, absolute row floor, RECONCILE_MIN_SHARE of the baseline and, per condition
bucket, of that bucket's own baseline) over valid-VIN baselines and says what the old
writer would have retired next to what the scanner did retire (``scanner_retired``, from
``listing_retirements``), plus the D-N21 alarm. Rows without a valid VIN and backlog rows
(the old W2's job) are retired by the scanner's rule 8, not here."""
from __future__ import annotations

import logging
import statistics
from datetime import datetime, timedelta, timezone
from typing import Any

from backend.scanner.inventory_reconcile import BUCKET_UNKNOWN, CONDITION_BUCKETS, condition_bucket, no_retire_switch
from backend.scanner.pipeline.constants import (
    MIN_ROWS_UNKNOWN,
    RECONCILE_MIN_SHARE,
    RETIRE_ALARM_GAP_BASE,
    RETIRE_ALARM_GAP_CAP,
    RETIRE_ALARM_GAP_PER_DAY,
    RETIRE_ALARM_NIGHTLY,
)
from backend.scanner.pipeline.db import _rows

_log = logging.getLogger(__name__)

# P5A.7: a baseline counts only rows with a valid 17-character VIN (the policy's rule 4);
# the same shape normalize_scanner_vin accepts, minus the alphanumeric check.
_VALID_VIN = "LENGTH(TRIM(COALESCE(vin,''))) = 17 AND UPPER(TRIM(vin)) NOT LIKE 'UNKNOWN%'"
# the writers whose rows are "this run's retirements" (the pipeline has none of its own)
_SCANNER_WRITERS = ("scanner_reconcile", "delta_scan")


def _days_before(iso: str, days: int) -> str:
    return (datetime.fromisoformat(iso.replace("Z", "+00:00")) - timedelta(days=days)).isoformat()


def bucket_counts(conn, dealer_id: str, *, since_iso: str | None = None, before_iso: str | None = None) -> dict[str, int]:
    """The dealer's active valid-VIN rows per condition bucket, scraped in ``[since_iso, before_iso)``
    (either bound open when ``None``): ``{"new": n, "used": n, "unknown": n}``.

    Grouped by the stored ``condition`` and folded through the scanner's
    ``condition_bucket``, so this path and inventory_reconcile can never disagree
    on what a bucket is (blank is ``unknown``; assess's ``rows_used`` counts it as
    used and is not a retirement input)."""
    sql = f"SELECT condition, COUNT(*) AS n FROM cars WHERE dealer_id = ? AND COALESCE(listing_active,1)=1 AND {_VALID_VIN}"
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


def valid_vin_count(conn, dealer_id: str, *, since_iso: str | None = None, before_iso: str | None = None) -> int:
    """The dealer's active valid-VIN rows, scraped in ``[since_iso, before_iso)`` (either bound
    open when ``None``): the lot-wide ``known_before`` baseline of the run (P5A.7)."""
    sql = f"SELECT COUNT(*) AS n FROM cars WHERE dealer_id = ? AND COALESCE(listing_active,1)=1 AND {_VALID_VIN}"
    params: list[Any] = [dealer_id]
    if since_iso:
        sql += " AND scraped_at >= ?"
        params.append(since_iso)
    if before_iso:
        sql += " AND scraped_at < ?"
        params.append(before_iso)
    return int(_rows(conn, sql, tuple(params))[0]["n"] or 0)


def alarm_threshold(days: float, *, gap_aware: bool) -> float:
    """Share of a dealer's prior active rows above which a run's retirements raise the alarm.

    Nightly runs keep ``RETIRE_ALARM_NIGHTLY`` (10%). The first live prod retirement and its
    rehearsal (D-N21) use ``min(60%, 10% + 1.5% x days)``, days = run start minus the median
    ``scraped_at`` of the rows being retired: a dealer last scraped a month ago has turned
    over more than a day-old one."""
    if not gap_aware:
        return RETIRE_ALARM_NIGHTLY
    return min(RETIRE_ALARM_GAP_CAP, RETIRE_ALARM_GAP_BASE + RETIRE_ALARM_GAP_PER_DAY * max(0.0, float(days)))


def _gap_aware_env() -> bool:
    import os
    return (os.environ.get("PIPELINE_RETIRE_ALARM_GAP_AWARE") or "").strip().lower() in ("1", "true", "yes", "on")


def _ts(value: Any) -> datetime | None:
    try:
        dt = datetime.fromisoformat(str(value).strip().replace("Z", "+00:00").replace(" ", "T", 1))
    except ValueError:
        return None
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def scanner_retirements(conn, dealer_id: str, since_iso: str, *, gap_aware: bool | None = None) -> dict[str, Any]:
    """What the scanner retired for this dealer since the run started, from ``listing_retirements``,
    with the D-N21 alarm: ``{retired, prior_active, share, days, threshold, alarm, by_writer}``.

    Read only. A database without the table (SQLite tests, behind the chain) answers zero."""
    gap_aware = _gap_aware_env() if gap_aware is None else gap_aware
    marks = ",".join("?" * len(_SCANNER_WRITERS))
    try:
        got = _rows(
            conn,
            "SELECT r.writer AS writer, c.scraped_at AS scraped_at FROM listing_retirements r "
            "LEFT JOIN cars c ON c.id = r.car_id "
            f"WHERE r.dealer_id = ? AND r.created_at >= ? AND r.writer IN ({marks})",
            (dealer_id, since_iso, *_SCANNER_WRITERS),
        )
    except Exception:  # noqa: BLE001 - no provenance table yet: nothing to report
        return {"retired": 0, "alarm": False}
    by_writer: dict[str, int] = {}
    for r in got:
        by_writer[r["writer"]] = by_writer.get(r["writer"], 0) + 1
    retired = len(got)
    active = int(_rows(conn, "SELECT COUNT(*) AS n FROM cars WHERE dealer_id = ? AND COALESCE(listing_active,1)=1",
                       (dealer_id,))[0]["n"] or 0)
    prior = active + retired
    started = _ts(since_iso)
    seen = [t for t in (_ts(r.get("scraped_at")) for r in got) if t is not None]
    days = 0.0
    if started and seen:
        median = datetime.fromtimestamp(statistics.median(t.timestamp() for t in seen), timezone.utc)
        days = max(0.0, (started - median).total_seconds() / 86400)
    threshold = alarm_threshold(days, gap_aware=gap_aware)
    share = retired / prior if prior else 0.0
    return {"retired": retired, "prior_active": prior, "share": round(share, 4), "days": round(days, 1),
            "threshold": round(threshold, 4), "gap_aware": gap_aware, "alarm": bool(retired and share > threshold),
            "by_writer": by_writer}


def reconcile_dealer(conn, dealer_id: str, since_iso: str, baseline: int, rows_this_run: int, verdict: str,
                     *, dry_run: bool = False, baseline_buckets: dict[str, int] | None = None,
                     rows_buckets: dict[str, int] | None = None) -> dict[str, Any]:
    """Report what the run's guards let through; retire nothing (P5A.7, D-RC1 A).

    Same guards as the writer this replaced: only after a run the assess step accepted
    (ok / thin / inaccurate), with at least ``MIN_ROWS_UNKNOWN`` rows and
    ``RECONCILE_MIN_SHARE`` of the valid-VIN baseline (whole lot, and per condition bucket
    when *baseline_buckets* / *rows_buckets* are given, P1A.1). ``retired`` is always 0 here;
    ``retirable`` is what the old writer would have un-listed, ``invalid_vin_stale`` the
    stale rows without a valid VIN (the scanner's rule 8 retires those, never this report),
    and ``scanner_retired`` what the scanner did retire this run, with the D-N21 alarm.
    *dry_run* is kept for callers; it changes nothing now.
    """
    out: dict[str, Any] = {"eligible": False, "stale": 0, "retired": 0, "reason": "", "report_only": True}
    out["no_retire_switch"] = bool(no_retire_switch())
    out["scanner_retired"] = scanner_retirements(conn, dealer_id, since_iso)
    if verdict not in ("ok", "thin", "inaccurate"):
        out["reason"] = f"verdict {verdict}"
        return out
    # The 30-day baseline is 0 for a dealer nobody scanned lately, and a
    # share-of-zero guard passes anything: on 2026-09-26 an 8-row run retired
    # 382 of Gunn Honda's cars. Fall back to the rows seen in the last 90 days,
    # then to every active row, and always demand an absolute floor.
    if not baseline:
        baseline = (valid_vin_count(conn, dealer_id, since_iso=_days_before(since_iso, 90), before_iso=since_iso)
                    or valid_vin_count(conn, dealer_id, before_iso=since_iso))
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
        return _report_by_bucket(conn, dealer_id, since_iso, out, baseline_buckets or {}, rows_buckets)
    valid_stale = valid_vin_count(conn, dealer_id, before_iso=since_iso)
    out["retirable"] = valid_stale
    out["invalid_vin_stale"] = out["stale"] - valid_stale
    return out


def _report_by_bucket(conn, dealer_id: str, since_iso: str, out: dict[str, Any], baseline_buckets: dict[str, int],
                      rows_buckets: dict[str, int] | None) -> dict[str, Any]:
    """The per-bucket guard (P1A.1), as a report. A lot-wide share hides a one-sided run: on
    2026-09-28 parksidekia-com returned 306 new / 0 used and the pipeline retired the used cars.

    * ``new`` / ``used`` are retirable only when the run returned rows of that bucket
      AND at least ``RECONCILE_MIN_SHARE`` of the bucket's valid-VIN baseline. A bucket
      whose baseline window is empty falls back to its last 90 days, then to all its
      stale valid-VIN rows (Gunn Honda).
    * Blank-condition rows are retirable only when both new and used qualify.
    * Held-back rows are counted as ``kept_missing_condition`` or ``kept_low_bucket``.
    * Stale rows without a valid VIN are only counted (``invalid_vin_stale``)."""
    rows_b = rows_buckets if rows_buckets is not None else bucket_counts(conn, dealer_id, since_iso=since_iso)
    rows_b = {b: int(rows_b.get(b) or 0) for b in CONDITION_BUCKETS}
    base = {b: int(baseline_buckets.get(b) or 0) for b in CONDITION_BUCKETS}
    stale_n: dict[str, int] = dict.fromkeys(CONDITION_BUCKETS, 0)
    invalid = 0
    for r in _rows(conn, f"SELECT condition, ({_VALID_VIN}) AS valid FROM cars WHERE dealer_id = ? AND COALESCE(listing_active,1)=1 AND scraped_at < ?",
                   (dealer_id, since_iso)):
        if r.get("valid"):
            stale_n[condition_bucket(r.get("condition"))] += 1
        else:
            invalid += 1
    fallback: dict[str, int] = {}
    for b in ("new", "used"):
        if not base[b] and stale_n[b]:
            wider = bucket_counts(conn, dealer_id, since_iso=_days_before(since_iso, 90), before_iso=since_iso)[b]
            base[b] = int(wider or 0) or stale_n[b]
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
    out["kept_missing_condition"] = sum(stale_n[b] for b in CONDITION_BUCKETS if why[b] == "missing_condition")
    out["kept_low_bucket"] = sum(stale_n[b] for b in CONDITION_BUCKETS if why[b] == "low_bucket")
    out["retirable"] = sum(stale_n[b] for b in CONDITION_BUCKETS if not why[b])
    out["invalid_vin_stale"] = invalid
    out["buckets"] = {b: {"baseline": base[b], "rows": rows_b[b], "stale": stale_n[b], "kept": why[b] or None}
                      for b in CONDITION_BUCKETS}
    if fallback:
        out["bucket_baseline_fallback"] = fallback
    return out
