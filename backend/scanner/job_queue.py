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


def claim_next_job() -> dict[str, Any] | None:
    if not inventory_pg.is_inventory_postgres():
        return None
    wid = _worker_id()
    now = datetime.now(timezone.utc).isoformat()
    conn = pg_connect()
    try:
        cur = conn.cursor()
        cur.execute(
            qmarks_to_percent_s(
                """
                SELECT id, dealer_id, job_type, payload_json
                FROM dealer_jobs
                WHERE status = 'queued'
                ORDER BY created_at ASC
                LIMIT 1
                FOR UPDATE SKIP LOCKED
                """
            )
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


def _default_scan_interval_hours() -> int:
    try:
        return max(1, int(os.environ.get("SCANNER_DEFAULT_INTERVAL_HOURS", "24")))
    except (TypeError, ValueError):
        return 24


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
        rows = cur.fetchall() or []
        for dealer_id, interval_hours in rows:
            did = str(dealer_id or "").strip()
            if not did:
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

    result: dict[str, Any] = {}
    try:
        result = json.loads(row.get("result_json") or "{}")
    except json.JSONDecodeError:
        result = {}
    payload: dict[str, Any] = {}
    try:
        payload = json.loads(row.get("payload_json") or "{}")
    except json.JSONDecodeError:
        payload = {}
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


def list_recent_jobs(*, limit: int = 30) -> list[dict[str, Any]]:
    if not inventory_pg.is_inventory_postgres():
        return []
    lim = max(1, min(int(limit), 100))
    conn = pg_connect()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT id, dealer_id, job_type, status, worker_id, created_at, started_at, finished_at, error, result_json
            FROM dealer_jobs
            ORDER BY id DESC
            LIMIT %s
            """,
            (lim,),
        )
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]
    finally:
        conn.close()
