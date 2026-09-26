"""
Postgres-backed dealer scrape job queue (Sprint A2 scaffold).

Workers claim rows with ``FOR UPDATE SKIP LOCKED``. Scheduler enqueues refresh jobs.
"""
from __future__ import annotations

import json
import logging
import os
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

from backend.db import inventory_pg
from backend.db.inventory_pg import pg_connect, qmarks_to_percent_s

_log = logging.getLogger(__name__)


def _worker_id() -> str:
    return (os.environ.get("SCANNER_WORKER_ID") or "").strip() or f"worker-{uuid.uuid4().hex[:8]}"


def ensure_job_tables(conn) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS dealer_catalog (
            dealer_id TEXT PRIMARY KEY,
            registry_id BIGINT,
            provider TEXT,
            inventory_mode TEXT,
            inventory_endpoint TEXT,
            site_config_json TEXT NOT NULL DEFAULT '{}',
            last_vin_fingerprint TEXT,
            scan_interval_hours INTEGER NOT NULL DEFAULT 24,
            next_scan_at TEXT,
            onboarded_at TEXT,
            last_scan_at TEXT,
            updated_at TEXT NOT NULL
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS dealer_jobs (
            id BIGSERIAL PRIMARY KEY,
            dealer_id TEXT NOT NULL,
            job_type TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'queued',
            payload_json TEXT NOT NULL DEFAULT '{}',
            worker_id TEXT,
            created_at TEXT NOT NULL,
            started_at TEXT,
            finished_at TEXT,
            error TEXT,
            result_json TEXT NOT NULL DEFAULT '{}'
        )
        """
    )
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_dealer_jobs_status ON dealer_jobs(status, created_at)"
    )
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_dealer_catalog_next_scan ON dealer_catalog(next_scan_at)"
    )
    conn.commit()
    cur.close()


def init_job_queue_schema() -> None:
    if not inventory_pg.is_inventory_postgres():
        _log.warning("Job queue requires INVENTORY_DATABASE_URL (Postgres); skipping schema init.")
        return
    conn = pg_connect()
    try:
        ensure_job_tables(conn)
    finally:
        conn.close()


def enqueue_job(
    *,
    dealer_id: str,
    job_type: str,
    payload: dict[str, Any] | None = None,
) -> int | None:
    if not inventory_pg.is_inventory_postgres():
        return None
    did = (dealer_id or "").strip()
    jt = (job_type or "").strip().lower()
    if not did or jt not in ("onboard", "refresh", "rescan"):
        return None
    now = datetime.now(timezone.utc).isoformat()
    conn = pg_connect()
    try:
        cur = conn.cursor()
        cur.execute(
            qmarks_to_percent_s(
                """
                INSERT INTO dealer_jobs (dealer_id, job_type, status, payload_json, created_at)
                VALUES (?, ?, 'queued', ?, ?)
                RETURNING id
                """
            ),
            (did, jt, json.dumps(payload or {}, ensure_ascii=False), now),
        )
        row = cur.fetchone()
        conn.commit()
        return int(row[0]) if row else None
    finally:
        conn.close()


def _worker_job_types() -> list[str]:
    """SCANNER_WORKER_JOB_TYPES="onboard" limits a worker to those job types: the
    discovery image (Dockerfile.discovery, the only one with a browser) takes
    onboard jobs, the HTTP-only scanner workers take the rest."""
    raw = (os.environ.get("SCANNER_WORKER_JOB_TYPES") or "").strip().lower()
    return [t.strip() for t in raw.split(",") if t.strip()]


def claim_next_job() -> dict[str, Any] | None:
    if not inventory_pg.is_inventory_postgres():
        return None
    wid = _worker_id()
    now = datetime.now(timezone.utc).isoformat()
    types = _worker_job_types()
    conn = pg_connect()
    try:
        cur = conn.cursor()
        type_clause = ""
        params: tuple = ()
        if types:
            type_clause = " AND job_type IN (" + ",".join("?" * len(types)) + ")"
            params = tuple(types)
        cur.execute(
            qmarks_to_percent_s(
                f"""
                SELECT id, dealer_id, job_type, payload_json
                FROM dealer_jobs
                WHERE status = 'queued'{type_clause}
                ORDER BY created_at ASC
                LIMIT 1
                FOR UPDATE SKIP LOCKED
                """
            ),
            params,
        )
        row = cur.fetchone()
        if not row:
            conn.commit()
            return None
        job_id, dealer_id, job_type, payload_json = row
        cur.execute(
            qmarks_to_percent_s(
                """
                UPDATE dealer_jobs
                SET status = 'running', worker_id = ?, started_at = ?
                WHERE id = ?
                """
            ),
            (wid, now, int(job_id)),
        )
        conn.commit()
        payload: dict[str, Any] = {}
        try:
            payload = json.loads(payload_json or "{}")
        except json.JSONDecodeError:
            payload = {}
        return {
            "id": int(job_id),
            "dealer_id": str(dealer_id),
            "job_type": str(job_type),
            "payload": payload,
        }
    finally:
        conn.close()


def finish_job(
    job_id: int,
    *,
    ok: bool,
    error: str | None = None,
    result: dict[str, Any] | None = None,
) -> None:
    if not inventory_pg.is_inventory_postgres():
        return
    now = datetime.now(timezone.utc).isoformat()
    status = "done" if ok else "failed"
    conn = pg_connect()
    try:
        cur = conn.cursor()
        cur.execute(
            qmarks_to_percent_s(
                """
                UPDATE dealer_jobs
                SET status = ?, finished_at = ?, error = ?, result_json = ?
                WHERE id = ?
                """
            ),
            (
                status,
                now,
                (error or "")[:2000] or None,
                json.dumps(result or {}, ensure_ascii=False),
                int(job_id),
            ),
        )
        conn.commit()
    finally:
        conn.close()


def _stale_running_max_age_sec() -> int:
    """How long a ``running`` row may sit before it is presumed orphaned.

    Defaults to the worker's own subprocess timeout (``SCANNER_JOB_TIMEOUT_SEC``,
    3600s) plus a 15-minute grace for post-scan bookkeeping. Override with
    ``SCANNER_STALE_JOB_SEC`` when a deployment runs longer per-dealer jobs.
    """
    raw = (os.environ.get("SCANNER_STALE_JOB_SEC") or "").strip()
    if raw:
        try:
            return max(60, int(raw))
        except ValueError:
            pass
    try:
        timeout = int(os.environ.get("SCANNER_JOB_TIMEOUT_SEC", "3600"))
    except ValueError:
        timeout = 3600
    return max(60, timeout) + 900


def reap_stale_running_jobs(*, max_age_sec: int | None = None) -> int:
    """Fail ``running`` jobs whose worker died without calling :func:`finish_job`.

    A worker killed mid-job (OOM, redeploy, SIGKILL) leaves its row ``running``
    forever. ``schedule_due_refresh_jobs`` treats ``running`` as active and never
    enqueues that dealer again, so one lost worker silently removes a dealer from
    the refresh cadence for good. Marking the row ``failed`` with a recognisable
    error lets the scheduler re-enqueue on the next due tick and keeps the
    failure visible in the ops hub. Returns the number of rows reaped.
    """
    if not inventory_pg.is_inventory_postgres():
        return 0
    age = int(max_age_sec) if max_age_sec is not None else _stale_running_max_age_sec()
    now = datetime.now(timezone.utc)
    cutoff = (now - timedelta(seconds=age)).isoformat()
    conn = pg_connect()
    try:
        cur = conn.cursor()
        cur.execute(
            qmarks_to_percent_s(
                """
                UPDATE dealer_jobs
                SET status = 'failed', finished_at = ?,
                    error = ?
                WHERE status = 'running'
                  AND started_at IS NOT NULL
                  AND started_at < ?
                RETURNING id, dealer_id, worker_id, started_at
                """
            ),
            (now.isoformat(), f"stale_running: no finish_job within {age}s (worker lost)", cutoff),
        )
        rows = cur.fetchall() or []
        conn.commit()
    finally:
        conn.close()
    for job_id, dealer_id, worker_id, started_at in rows:
        _log.warning(
            "Reaped stale running job id=%s dealer=%s worker=%s started=%s",
            job_id, dealer_id, worker_id, started_at,
        )
    return len(rows)


def _default_scan_interval_hours() -> int:
    try:
        return max(1, int(os.environ.get("SCANNER_DEFAULT_INTERVAL_HOURS", "24")))
    except (TypeError, ValueError):
        return 24


# dealer_scan_status reasons (V008) meaning "the site itself is gone/unreachable":
# re-scanning on the normal cadence just repeats a failure that has already been
# diagnosed. These get a long backoff — never a permanent exclusion, domains come back.
_LONG_BACKOFF_REASONS = frozenset({"dns_fail", "redirect_offsite"})


def _unreachable_backoff_days() -> int:
    try:
        return max(1, int(os.environ.get("SCANNER_UNREACHABLE_BACKOFF_DAYS", "7")))
    except (TypeError, ValueError):
        return 7


def _parse_status_timestamp(value: Any) -> datetime | None:
    """Normalize dealer_scan_status.checked_at (psycopg datetime or ISO text) to aware UTC."""
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, str) and value.strip():
        try:
            dt = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
        except ValueError:
            return None
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    return None


def get_dealer_scan_reason(dealer_id: str) -> str | None:
    """Recorded diagnosis class for a dealer from dealer_scan_status (V008), if any."""
    if not inventory_pg.is_inventory_postgres():
        return None
    did = (dealer_id or "").strip()
    if not did:
        return None
    try:
        conn = pg_connect()
    except Exception:
        return None
    try:
        cur = conn.cursor()
        cur.execute(
            qmarks_to_percent_s(
                "SELECT reason FROM dealer_scan_status WHERE dealer_key = ? LIMIT 1"
            ),
            (did,),
        )
        row = cur.fetchone()
        if not row or not row[0]:
            return None
        return str(row[0]).strip().lower() or None
    except Exception as exc:  # table may predate V008 in some environments
        _log.debug("dealer_scan_status lookup failed for %s: %s", did, exc)
        return None
    finally:
        conn.close()


def record_catalog_after_success(
    *,
    dealer_id: str,
    job_type: str,
    payload: dict[str, Any] | None = None,
    scan_interval_hours: int | None = None,
) -> None:
    """Upsert ``dealer_catalog`` after a successful onboard/refresh job (A3)."""
    if not inventory_pg.is_inventory_postgres():
        return
    did = (dealer_id or "").strip()
    if not did:
        return
    jt = (job_type or "").strip().lower()
    if jt not in ("onboard", "refresh", "rescan"):
        return
    hours = max(1, int(scan_interval_hours or _default_scan_interval_hours()))
    now = datetime.now(timezone.utc)
    now_iso = now.isoformat()
    nxt = (now + timedelta(hours=hours)).isoformat()
    pl = payload or {}
    endpoint = (pl.get("url") or pl.get("inventory_endpoint") or "").strip() or None
    conn = pg_connect()
    try:
        cur = conn.cursor()
        cur.execute(
            qmarks_to_percent_s("SELECT dealer_id FROM dealer_catalog WHERE dealer_id = ?"),
            (did,),
        )
        exists = cur.fetchone() is not None
        if exists:
            cur.execute(
                qmarks_to_percent_s(
                    """
                    UPDATE dealer_catalog
                    SET last_scan_at = ?, next_scan_at = ?, updated_at = ?,
                        scan_interval_hours = COALESCE(scan_interval_hours, ?),
                        inventory_endpoint = COALESCE(?, inventory_endpoint)
                    WHERE dealer_id = ?
                    """
                ),
                (now_iso, nxt, now_iso, hours, endpoint, did),
            )
        else:
            cur.execute(
                qmarks_to_percent_s(
                    """
                    INSERT INTO dealer_catalog (
                        dealer_id, inventory_endpoint, scan_interval_hours,
                        next_scan_at, onboarded_at, last_scan_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """
                ),
                (did, endpoint, hours, nxt, now_iso, now_iso, now_iso),
            )
        conn.commit()
    finally:
        conn.close()


def schedule_due_refresh_jobs() -> int:
    """Enqueue refresh jobs for catalogs past ``next_scan_at`` (no duplicate queued/running)."""
    if not inventory_pg.is_inventory_postgres():
        return 0
    now = datetime.now(timezone.utc)
    now_iso = now.isoformat()
    conn = pg_connect()
    enqueued = 0
    try:
        cur = conn.cursor()
        try:
            cur.execute(
                qmarks_to_percent_s(
                    """
                    SELECT c.dealer_id, c.scan_interval_hours, s.reason, s.checked_at
                    FROM dealer_catalog c
                    LEFT JOIN dealer_scan_status s ON s.dealer_key = c.dealer_id
                    WHERE c.next_scan_at IS NOT NULL AND c.next_scan_at <= ?
                    """
                ),
                (now_iso,),
            )
            rows = cur.fetchall() or []
        except Exception as exc:
            # dealer_scan_status may not exist (migration V008 not applied here);
            # fall back to the status-blind cadence rather than stalling the scheduler.
            _log.debug("dealer_scan_status join unavailable, scheduling blind: %s", exc)
            conn.rollback()
            cur = conn.cursor()
            cur.execute(
                qmarks_to_percent_s(
                    """
                    SELECT dealer_id, scan_interval_hours
                    FROM dealer_catalog
                    WHERE next_scan_at IS NOT NULL AND next_scan_at <= ?
                    """
                ),
                (now_iso,),
            )
            rows = [(d, h, None, None) for d, h in (cur.fetchall() or [])]
        backoff = timedelta(days=_unreachable_backoff_days())
        for dealer_id, interval_hours, scan_reason, checked_at in rows:
            did = str(dealer_id or "").strip()
            if not did:
                continue
            reason = str(scan_reason or "").strip().lower()
            if reason in _LONG_BACKOFF_REASONS:
                checked = _parse_status_timestamp(checked_at)
                if checked is not None and (now - checked) < backoff:
                    # Diagnosed unreachable (dns_fail / redirect_offsite) recently:
                    # park until the end of the backoff window instead of re-enqueueing
                    # the same failure on the normal cadence. NOT a permanent exclusion —
                    # once checked_at ages past the window the dealer schedules again.
                    cur.execute(
                        qmarks_to_percent_s(
                            "UPDATE dealer_catalog SET next_scan_at = ? WHERE dealer_id = ?"
                        ),
                        ((checked + backoff).isoformat(), did),
                    )
                    continue
            cur.execute(
                qmarks_to_percent_s(
                    """
                    SELECT 1 FROM dealer_jobs
                    WHERE dealer_id = ? AND job_type = 'refresh'
                      AND status IN ('queued', 'running')
                    LIMIT 1
                    """
                ),
                (did,),
            )
            if cur.fetchone():
                continue
            cur.execute(
                qmarks_to_percent_s(
                    """
                    INSERT INTO dealer_jobs (dealer_id, job_type, status, payload_json, created_at)
                    VALUES (?, 'refresh', 'queued', '{}', ?)
                    """
                ),
                (did, now_iso),
            )
            hours = max(1, int(interval_hours or 24))
            nxt = (now + timedelta(hours=hours)).isoformat()
            cur.execute(
                qmarks_to_percent_s(
                    "UPDATE dealer_catalog SET next_scan_at = ? WHERE dealer_id = ?"
                ),
                (nxt, did),
            )
            enqueued += 1
        conn.commit()
    finally:
        conn.close()
    return enqueued


def list_dealer_catalog(*, limit: int = 50) -> list[dict[str, Any]]:
    if not inventory_pg.is_inventory_postgres():
        return []
    lim = max(1, min(int(limit), 200))
    conn = pg_connect()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT dealer_id, provider, inventory_mode, scan_interval_hours,
                   next_scan_at, last_scan_at, onboarded_at, updated_at
            FROM dealer_catalog
            ORDER BY updated_at DESC NULLS LAST
            LIMIT %s
            """,
            (lim,),
        )
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]
    finally:
        conn.close()


def get_job(job_id: int) -> dict[str, Any] | None:
    if not inventory_pg.is_inventory_postgres():
        return None
    try:
        jid = int(job_id)
    except (TypeError, ValueError):
        return None
    if jid < 1:
        return None
    conn = pg_connect()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT id, dealer_id, job_type, status, worker_id, created_at, started_at,
                   finished_at, error, payload_json, result_json
            FROM dealer_jobs
            WHERE id = %s
            """,
            (jid,),
        )
        row = cur.fetchone()
        if not row:
            return None
        cols = [d[0] for d in cur.description]
        return dict(zip(cols, row))
    finally:
        conn.close()


def _has_active_job(*, dealer_id: str, job_type: str) -> bool:
    conn = pg_connect()
    try:
        cur = conn.cursor()
        cur.execute(
            qmarks_to_percent_s(
                """
                SELECT 1 FROM dealer_jobs
                WHERE dealer_id = ? AND job_type = ?
                  AND status IN ('queued', 'running')
                LIMIT 1
                """
            ),
            (dealer_id, job_type),
        )
        return cur.fetchone() is not None
    finally:
        conn.close()


def _parse_job_result(row: dict[str, Any]) -> dict[str, Any]:
    try:
        result = json.loads(row.get("result_json") or "{}")
        return result if isinstance(result, dict) else {}
    except json.JSONDecodeError:
        return {}


def _job_retry_eligible(row: dict[str, Any]) -> tuple[bool, str]:
    """Allow retry for hard failures and soft failures (done with 0 / low-confidence scrape)."""
    status = (row.get("status") or "").strip().lower()
    if status == "failed":
        return True, ""
    if status == "done":
        result = _parse_job_result(row)
        from backend.scanner.scrape_confidence import soft_scrape_failure

        if soft_scrape_failure(str(row.get("job_type") or ""), result):
            return True, ""
    return False, "not_failed"


def retry_failed_job(job_id: int) -> tuple[bool, str, dict[str, Any]]:
    """Re-queue a failed job with the same dealer_id, job_type, and payload."""
    if not inventory_pg.is_inventory_postgres():
        return False, "postgres_required", {}
    row = get_job(job_id)
    if not row:
        return False, "not_found", {}
    status = (row.get("status") or "").strip().lower()
    eligible, err = _job_retry_eligible(row)
    if not eligible:
        return False, err, {"status": status}
    did = (row.get("dealer_id") or "").strip()
    jt = (row.get("job_type") or "").strip().lower()
    if not did or jt not in ("onboard", "refresh", "rescan"):
        return False, "invalid_job", {}
    if _has_active_job(dealer_id=did, job_type=jt):
        return False, "already_active", {"dealer_id": did, "job_type": jt}
    payload: dict[str, Any] = {}
    try:
        payload = json.loads(row.get("payload_json") or "{}")
    except json.JSONDecodeError:
        payload = {}
    new_id = enqueue_job(dealer_id=did, job_type=jt, payload=payload)
    if not new_id:
        return False, "enqueue_failed", {}
    return True, "", {"job_id": new_id, "source_job_id": int(row["id"]), "dealer_id": did}


def diagnose_job_row(row: dict[str, Any], *, use_llm: bool = True) -> dict[str, Any]:
    from backend.scanner.job_diagnosis import diagnose_failed_job

    payload: dict[str, Any] = {}
    try:
        payload = json.loads(row.get("payload_json") or "{}")
    except json.JSONDecodeError:
        payload = {}

    # The audit's recorded per-dealer diagnosis (V008) outranks anything inferable from
    # this one job's failure text. A needs_browser_probe dealer 403s every bare HTTP
    # client by TLS fingerprint, so a plain re-queue is guaranteed to fail identically —
    # retry with the resilient browser profile instead.
    #
    # But only when this job did NOT already run resilient: nothing ever clears the
    # dealer_scan_status flag, so without this check every failure at the dealer —
    # parser tracebacks included — got the canned TLS verdict and the ops UI
    # re-enqueued the same doomed job forever while the real error never surfaced.
    scan_reason = get_dealer_scan_reason(str(row.get("dealer_id") or ""))
    already_resilient = str(payload.get("profile") or "").strip().lower() == "resilient"
    if scan_reason == "needs_browser_probe" and not already_resilient:
        return {
            "source": "rule",
            "summary": (
                "Dealer is classed needs_browser_probe in dealer_scan_status "
                "(403/TLS-fingerprint rejection) — a bare HTTP retry will fail the same "
                "way; retrying with the resilient browser profile."
            ),
            "category": "dealer_site",
            "root_cause": (
                "dealer_scan_status.reason=needs_browser_probe: edge protection rejects "
                "non-browser TLS fingerprints; verdict unknown until a real browser probes."
            ),
            "retry_recommended": True,
            "retry_strategy": "retry_with_profile",
            "retry_actions": [{"type": "set_profile", "value": "resilient"}],
            "code_fix_hint": None,
            "confidence": 0.9,
            "dealer_scan_reason": scan_reason,
        }

    result: dict[str, Any] = {}
    try:
        result = json.loads(row.get("result_json") or "{}")
    except json.JSONDecodeError:
        result = {}
    cached = result.get("ai_diagnosis")
    if isinstance(cached, dict) and cached.get("summary") and not use_llm:
        return cached
    sc = result.get("scrape_confidence")
    if isinstance(sc, dict) and (sc.get("level") or "").lower() == "low":
        return {
            "source": "rule",
            "summary": sc.get("reason")
            or "Last scrape finished with low confidence — inventory may be incomplete or blocked.",
            "category": "dealer_site",
            "root_cause": f"Scrape path={sc.get('path') or 'unknown'}, vehicles={sc.get('vehicle_count', 0)}.",
            "retry_recommended": True,
            "retry_strategy": "retry_with_profile",
            "retry_actions": [{"type": "set_profile", "value": "resilient"}],
            "code_fix_hint": "Uses persistent dealer profile on retry.",
            "confidence": float(sc.get("score") or 0.72),
        }
    diagnosis = diagnose_failed_job(
        error=str(row.get("error") or ""),
        log_tail=str(result.get("log_tail") or ""),
        job_type=str(row.get("job_type") or ""),
        dealer_id=str(row.get("dealer_id") or ""),
        payload=payload,
        use_llm=use_llm,
    )
    return diagnosis


def _merge_result_diagnosis(job_id: int, diagnosis: dict[str, Any]) -> None:
    if not inventory_pg.is_inventory_postgres():
        return
    row = get_job(job_id)
    if not row:
        return
    result: dict[str, Any] = {}
    try:
        result = json.loads(row.get("result_json") or "{}")
    except json.JSONDecodeError:
        result = {}
    result["ai_diagnosis"] = diagnosis
    conn = pg_connect()
    try:
        cur = conn.cursor()
        cur.execute(
            qmarks_to_percent_s("UPDATE dealer_jobs SET result_json = ? WHERE id = ?"),
            (json.dumps(result, ensure_ascii=False), int(job_id)),
        )
        conn.commit()
    finally:
        conn.close()


def smart_retry_failed_job(job_id: int, *, use_llm: bool = True) -> tuple[bool, str, dict[str, Any]]:
    """Diagnose a failed job, then re-queue with AI/rule-guided retry hints."""
    if not inventory_pg.is_inventory_postgres():
        return False, "postgres_required", {}
    row = get_job(job_id)
    if not row:
        return False, "not_found", {}
    status = (row.get("status") or "").strip().lower()
    eligible, err = _job_retry_eligible(row)
    if not eligible:
        return False, err, {"status": status}
    did = (row.get("dealer_id") or "").strip()
    jt = (row.get("job_type") or "").strip().lower()
    if not did or jt not in ("onboard", "refresh", "rescan"):
        return False, "invalid_job", {}
    if _has_active_job(dealer_id=did, job_type=jt):
        return False, "already_active", {"dealer_id": did, "job_type": jt}

    from backend.scanner.job_diagnosis import apply_diagnosis_to_payload

    diagnosis = diagnose_job_row(row, use_llm=use_llm)
    _merge_result_diagnosis(int(row["id"]), diagnosis)

    if not diagnosis.get("retry_recommended", True):
        return False, "retry_not_recommended", {"diagnosis": diagnosis, "dealer_id": did}
    if diagnosis.get("retry_strategy") == "needs_code_fix" and diagnosis.get("confidence", 0) >= 0.85:
        return False, "needs_code_fix", {"diagnosis": diagnosis, "dealer_id": did}

    base_payload: dict[str, Any] = {}
    try:
        base_payload = json.loads(row.get("payload_json") or "{}")
    except json.JSONDecodeError:
        base_payload = {}
    payload = apply_diagnosis_to_payload(base_payload, diagnosis)

    new_id = enqueue_job(dealer_id=did, job_type=jt, payload=payload)
    if not new_id:
        return False, "enqueue_failed", {"diagnosis": diagnosis}
    return True, "", {
        "job_id": new_id,
        "source_job_id": int(row["id"]),
        "dealer_id": did,
        "diagnosis": diagnosis,
    }


def list_recent_jobs(
    *, limit: int = 30, status: str | None = None, dealer_id: str | None = None
) -> list[dict[str, Any]]:
    """Recent ``dealer_jobs`` rows, newest first.

    ``status`` and ``dealer_id`` are optional equality filters for the admin
    jobs board (``/api/admin/dealer-jobs``) — previously only ``limit`` could
    be narrowed, so a busy queue had no way to isolate e.g. just the failed
    jobs for one dealer.
    """
    if not inventory_pg.is_inventory_postgres():
        return []
    lim = max(1, min(int(limit), 100))
    conn = pg_connect()
    try:
        cur = conn.cursor()
        clauses: list[str] = []
        params: list[Any] = []
        status = (status or "").strip()
        if status:
            clauses.append("status = %s")
            params.append(status)
        dealer_id = (dealer_id or "").strip()
        if dealer_id:
            clauses.append("dealer_id = %s")
            params.append(dealer_id)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        params.append(lim)
        cur.execute(
            f"""
            SELECT id, dealer_id, job_type, status, worker_id, created_at, started_at, finished_at, error, result_json
            FROM dealer_jobs
            {where}
            ORDER BY id DESC
            LIMIT %s
            """,
            params,
        )
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]
    finally:
        conn.close()
