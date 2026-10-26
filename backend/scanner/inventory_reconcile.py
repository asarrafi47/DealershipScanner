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


def no_retire_switch() -> bool:
    """The one no-retire switch (``SCANNER_NO_RETIRE=1``, P5A.4).

    Every writer that can un-list a car honours it: this reconcile (full scans and the delta
    scan, which calls it), ``attribution.disown`` and the dealer pipeline's ``reconcile_dealer``
    (W2 and the lifecycle retry). With it set the plan is still computed and reported, nothing
    is written. Unlike ``SCANNER_RECONCILE`` it is not about this module's gates: it is the
    owner's stop button for a fleet whose retirements are in doubt.
    """
    return os.environ.get("SCANNER_NO_RETIRE", "").strip().lower() in ("1", "true", "yes", "on")


def _reconcile_mode() -> str:
    """``SCANNER_RECONCILE``: ``on`` (default), ``off`` (``0``) or ``dry`` (plan, write nothing)."""
    raw = os.environ.get("SCANNER_RECONCILE", "1").strip().lower()
    if raw == "dry":
        return "dry"
    return "off" if raw in ("0", "false", "no", "off") else "on"


def _reconcile_enabled() -> bool:
    return _reconcile_mode() != "off"


def _reconcile_min_rows() -> int:
    raw = (os.environ.get("SCANNER_RECONCILE_MIN_ROWS") or "20").strip()
    try:
        return max(1, int(raw))
    except ValueError:
        return 20


def _reconcile_bucket_share() -> float:
    """Share of a condition bucket's prior rows the run must re-see before that bucket retires.

    ``SCANNER_RECONCILE_BUCKET_SHARE`` (default 0.6). The old ``SCANNER_RECONCILE_MIN_COVERAGE``
    was a lot-wide share over ALL active rows, ghosts included; it never becomes this one. Its
    "0 disables the gate" meaning is kept (a bucket then needs only one re-seen VIN); any other
    value is ignored with a warning.
    """
    legacy = (os.environ.get("SCANNER_RECONCILE_MIN_COVERAGE") or "").strip()
    if legacy:
        try:
            if float(legacy) <= 0:
                return 0.0
        except ValueError:
            pass
        logger.warning(
            "SCANNER_RECONCILE_MIN_COVERAGE=%s is deprecated and ignored (only 0 still disables the "
            "gate); the per-bucket share is SCANNER_RECONCILE_BUCKET_SHARE", legacy,
        )
    raw = (os.environ.get("SCANNER_RECONCILE_BUCKET_SHARE") or "0.6").strip()
    try:
        return min(1.0, max(0.0, float(raw)))
    except ValueError:
        return 0.6


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




def _scraped_at_text(dt: datetime) -> str:
    """*dt* in the text shape of ``cars.scraped_at`` (naive UTC isoformat + ``Z``), so the
    guarded UPDATE compares like with like on SQLite and Postgres."""
    return dt.astimezone(timezone.utc).replace(tzinfo=None).isoformat() + "Z"


def _delete_index_rows(car_ids: list[int]) -> None:
    """Drop retired cars from the incomplete-listings index (IL-05); never raises."""
    if not car_ids:
        return
    try:
        from backend.db.incomplete_listings_db import delete_incomplete_records

        delete_incomplete_records(car_ids)
    except Exception:  # noqa: BLE001 - the retirement is already committed
        logger.warning("Inventory reconcile: could not delete %d incomplete-index row(s)", len(car_ids), exc_info=True)


def reconcile_dealer_inventory_after_scan(
    dealer_id: str,
    dealer_url: str,
    scraped_vins: set[str],
    stats: dict[str, Any],
    *,
    scraped_conditions: set[str] | None = None,
    replay: dict[str, Any] | None = None,
    protected_vins: set[str] | None = None,
    run_started_iso: str | None = None,
    source: str = "recipe",
    _conn: Any | None = None,
) -> dict[str, Any]:
    """
    Retire active DB rows for this dealer that the run proves are gone (``plan_retirement``).

    *scraped_vins* must already be normalized (``normalize_scanner_vin`` per vehicle).
    *replay* is the walk report (``result['recipe_replay']``: a ``walks`` list), *protected_vins*
    the VINs refused without evidence (never retired), *run_started_iso* the wall-clock start of
    the run (rows refreshed since then are neither baseline nor candidates; without it the
    call time stands in), *source* ``recipe`` or ``recovery:<strategy>``. *scraped_conditions*
    is kept for callers that pass it: per-bucket coverage by VIN already covers a condition the
    run returned nothing for.

    Mutates *stats* with the ``reconcile`` key. ``SCANNER_RECONCILE=dry`` and the no-retire
    switch compute and record the plan and write nothing. Returns a small result dict; the
    legacy keys (``ran``, ``marked_inactive``, ``skipped_reason``) stay, the plan adds
    ``eligible``, ``reason``, ``retired``, ``kept`` and ``buckets``.
    """
    from backend.scanner.recipe_validation import one_condition_ok_ids
    from backend.scanner.retirement_policy import ActiveRow, Policy, RunEvidence, plan_retirement

    out: dict[str, Any] = {
        "ran": False,
        "scraped_candidates": len(scraped_vins),
        "marked_inactive": 0,
        "retired": 0,
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

    mode = _reconcile_mode()
    if mode == "off":
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
        started = None
        if run_started_iso:
            try:
                started = datetime.fromisoformat(str(run_started_iso).strip().replace("Z", "+00:00"))
                if started.tzinfo is None:
                    started = started.replace(tzinfo=timezone.utc)
            except ValueError:
                started = None
        started = started or datetime.now(timezone.utc)
        now = datetime.now(timezone.utc).replace(microsecond=0).isoformat()

        # Every active row, valid VIN or not: the policy counts baselines by valid VIN only and
        # retires a VIN-less row only through a retirable bucket (rule 8).
        cur.execute(
            f"""
            SELECT id, vin, condition, make, scraped_at FROM cars
            WHERE ({scope_sql})
              AND (COALESCE(listing_active, 1) = 1)
            """,
            tuple(scope_params),
        )
        rows = [
            ActiveRow(int(r[0]), normalize_scanner_vin(r[1]), condition_bucket(r[2]), r[3], r[4])
            for r in cur.fetchall()
        ]
        seen_makes = {
            " ".join(str(r.make).lower().split()) for r in rows if r.vin_norm in scraped_vins and r.make
        }
        evidence = RunEvidence(
            seen_vins=frozenset(scraped_vins),
            matched_makes=seen_makes,
            walks=tuple((replay or {}).get("walks") or ()),
            protected_vins=frozenset(protected_vins or ()),
            run_started_iso=started.isoformat(),
            source=source or "recipe",
            error=str(stats["error"]) if stats.get("error") else None,
            rows=deduped,
        )
        policy = Policy(
            min_rows=min_rows,
            bucket_share=_reconcile_bucket_share(),
            one_condition_ok=(dealer_id or "").strip() in one_condition_ok_ids(),
        )
        plan = plan_retirement(rows, evidence, policy, datetime.now(timezone.utc))
        out.update(
            eligible=plan.eligible,
            reason=plan.reason,
            kept=dict(plan.kept),
            buckets={b: dict(v) for b, v in plan.buckets.items()},
            walks=dict(plan.walks_summary),
        )
        if plan.kept["condition_missing"]:
            out["kept_missing_condition"] = plan.kept["condition_missing"]
            logger.warning(
                "Inventory reconcile %s: run returned no rows of a condition; keeping %d active row(s) of it",
                (dealer_id or "").strip() or "?", plan.kept["condition_missing"],
            )
        if plan.kept["bucket_low_coverage"]:
            out["kept_low_bucket"] = plan.kept["bucket_low_coverage"]
            logger.warning(
                "Inventory reconcile %s: %s; keeping %d active row(s)",
                (dealer_id or "").strip() or "?",
                ", ".join(
                    f"{b} {v['matched']}/{v['prior']}" for b, v in plan.buckets.items() if not v["retirable"] and v["matched"]
                ) or "buckets under the coverage share",
                plan.kept["bucket_low_coverage"],
            )
        stale_ids = [int(i) for i in plan.retire_ids]
        if scraped_conditions is not None and stale_ids:
            # The caller's condition evidence stays a veto: a run whose rows carry no "used" (or no
            # condition at all) is no evidence about the used cars even if a VIN matched across
            # buckets; blank-condition rows need both sides.
            absent = {"new", "used"} - set(scraped_conditions)
            by_id = {int(r.id): r.bucket for r in rows}
            vetoed = [i for i in stale_ids if by_id[i] in absent or (by_id[i] == BUCKET_UNKNOWN and absent)]
            if vetoed:
                stale_ids = [i for i in stale_ids if i not in set(vetoed)]
                out["kept"]["condition_missing"] += len(vetoed)
                plan.kept["condition_missing"] += len(vetoed)
                out["kept_missing_condition"] = plan.kept["condition_missing"]
        out["would_retire"] = len(stale_ids)
        if not plan.eligible:
            out["skipped_reason"] = plan.reason
            return _finish()
        if mode == "dry" or no_retire_switch():
            out["dry_run"] = True
            out["skipped_reason"] = "dry_run" if mode == "dry" else "no_retire_switch"
            return _finish()

        marked = 0
        retired_ids: list[int] = []
        batch_size = 400
        guard_ts = _scraped_at_text(started)
        for i in range(0, len(stale_ids), batch_size):
            chunk = stale_ids[i : i + batch_size]
            if not chunk:
                continue
            ph = ",".join("?" * len(chunk))
            # A row re-upserted since the plan was made (scraped_at moved past the run start)
            # is alive: the guard leaves it, and the rowcount says how many really retired.
            cur.execute(
                f"""
                UPDATE cars
                SET listing_active = 0, listing_removed_at = ?
                WHERE id IN ({ph})
                  AND ({scope_sql})
                  AND COALESCE(listing_active, 1) = 1
                  AND (scraped_at IS NULL OR scraped_at < ?)
                """,
                (now, *chunk, *scope_params, guard_ts),
            )
            rc = cur.rowcount
            n = len(chunk) if rc is None or rc < 0 else int(rc)
            marked += n
            if n == len(chunk):
                retired_ids.extend(chunk)

        conn.commit()
        out["ran"] = True
        out["marked_inactive"] = marked
        out["retired"] = marked
        out["skipped_reason"] = "ok"
        if own_close:
            _delete_index_rows(retired_ids)
        return _finish()
    finally:
        if own_close:
            conn.close()
