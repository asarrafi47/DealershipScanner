"""
Dev-only scan lab: manifest scan jobs, results browser, lightweight DB edits.

Temporary operator flow for city-scoped scans (e.g. 92694 / 25 mi). Public
``/listings`` and ``/car/<id>`` are unchanged; this module adds ``/dev/scan-lab/*``.
"""

from __future__ import annotations

import json
import logging
import os
import subprocess
import sys
import threading
import time
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from urllib.parse import unquote, urlparse

from backend.db.inventory_db import (
    _UPDATABLE_CAR_COLUMNS,
    _parse_car_gallery,
    _parse_car_history_highlights,
)
from backend.db.inventory_db import _placeholders as sql_placeholders
from backend.db.inventory_pg import qmarks_to_percent_s

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[2]

DEFAULT_MANIFEST_REL = "backend/dev/fixtures/manifest_92694_25mi.json"
DEFAULT_ZIP = "92694"
DEFAULT_RADIUS_MI = 25

_SUMMARY_CACHE_TTL_S = 45.0
_summary_cache: dict[str, Any] = {"ts": 0.0, "data": None}

scan_lab_jobs: dict[str, dict[str, Any]] = {}
scan_lab_lock = threading.Lock()

_CAR_LIST_COLUMNS = (
    "id",
    "vin",
    "year",
    "make",
    "model",
    "trim",
    "price",
    "mileage",
    "dealer_id",
    "dealer_name",
    "image_url",
    "data_quality_score",
    "scraped_at",
    "stock_number",
)


def invalidate_scan_lab_summary_cache() -> None:
    _summary_cache["ts"] = 0.0
    _summary_cache["data"] = None


def default_manifest_path() -> Path:
    raw = (os.environ.get("SCAN_LAB_MANIFEST") or DEFAULT_MANIFEST_REL).strip()
    p = Path(raw)
    if not p.is_absolute():
        p = PROJECT_ROOT / p
    return p


class ScanLabRefused(RuntimeError):
    """The lab DSN is missing or could be a real inventory database."""


# Databases a lab scan must never point at (D-PG7: the lab lives in its own
# scratch database, ``cars_lab``).
_PROTECTED_LAB_DBS = frozenset({"cars", "cars_cleanroom", "postgres", "template0", "template1"})


def _dsn_target(dsn: str) -> tuple[str, str, str]:
    u = urlparse(dsn)
    return (u.hostname or "", str(u.port or ""), unquote(u.path.lstrip("/")))


def scan_lab_dsn() -> str:
    """The scratch Postgres DSN (``SCAN_LAB_DATABASE_URL``), validated.

    Refused when unset, when it is not a ``postgresql://`` URL, when its
    database is a protected one (``cars``, ``cars_cleanroom``, ``cars_cr_*``),
    or when it names the same host/port/database as the inventory DSN in the
    environment, so a lab scan or edit cannot reach production ``cars``.
    """
    dsn = (os.environ.get("SCAN_LAB_DATABASE_URL") or "").strip()
    if not dsn:
        raise ScanLabRefused("set SCAN_LAB_DATABASE_URL to a scratch Postgres (e.g. .../cars_lab)")
    if not dsn.startswith(("postgresql://", "postgres://")):
        raise ScanLabRefused("SCAN_LAB_DATABASE_URL must be a postgresql:// URL")
    host, port, dbname = _dsn_target(dsn)
    if not dbname or dbname in _PROTECTED_LAB_DBS or dbname.startswith("cars_cr_"):
        raise ScanLabRefused(f"database {dbname!r} is protected; the lab uses its own scratch database")
    for key in ("INVENTORY_DATABASE_URL", "DATABASE_URL"):
        other = (os.environ.get(key) or "").strip()
        if other and _dsn_target(other) == (host, port, dbname):
            raise ScanLabRefused(f"SCAN_LAB_DATABASE_URL is the same database as {key}")
    return dsn


def scan_lab_database_name() -> str:
    """Database name for display; empty when the lab DSN is unset or refused."""
    try:
        return _dsn_target(scan_lab_dsn())[2]
    except ScanLabRefused:
        return ""


@contextmanager
def scan_lab_db_conn():
    """Postgres connection to the scratch lab database (never production)."""
    import psycopg

    conn = psycopg.connect(scan_lab_dsn(), connect_timeout=10)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _lab_exec(cur, sql: str, params: tuple | list = ()):
    cur.execute(qmarks_to_percent_s(sql), tuple(params))
    return cur


def load_manifest(path: Path | None = None) -> list[dict[str, Any]]:
    mp = path or default_manifest_path()
    if not mp.is_file():
        return []
    try:
        data = json.loads(mp.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return []
    if not isinstance(data, list):
        return []
    out: list[dict[str, Any]] = []
    for row in data:
        if isinstance(row, dict) and str(row.get("dealer_id") or "").strip():
            out.append(row)
    return out


def manifest_dealer_ids(manifest: list[dict[str, Any]]) -> list[str]:
    ids: list[str] = []
    seen: set[str] = set()
    for row in manifest:
        did = str(row.get("dealer_id") or "").strip()
        if did and did not in seen:
            seen.add(did)
            ids.append(did)
    return ids


def manifest_lab_config() -> dict[str, Any]:
    mp = default_manifest_path()
    manifest = load_manifest(mp)
    return {
        "manifest_path": str(mp),
        "manifest_exists": mp.is_file(),
        "dealer_count": len(manifest),
        "dealers": manifest,
        "dealer_ids": manifest_dealer_ids(manifest),
        "zip_code": (os.environ.get("SCAN_LAB_ZIP") or DEFAULT_ZIP).strip(),
        "radius_miles": int(os.environ.get("SCAN_LAB_RADIUS_MI") or DEFAULT_RADIUS_MI),
        "lab_database": scan_lab_database_name(),
    }


def _incomplete_id_set() -> set[int]:
    # Car ids are per database, so the flags come from the lab's own table, not production's.
    try:
        with scan_lab_db_conn() as conn:
            cur = conn.cursor()
            cur.execute("SELECT car_id FROM incomplete_listings WHERE COALESCE(pending, 0) = 0")
            return {int(r[0]) for r in cur.fetchall() if r and r[0] is not None}
    except Exception:
        return set()


def inventory_summary_for_manifest(
    *,
    manifest_path: Path | None = None,
    force_refresh: bool = False,
) -> dict[str, Any]:
    now = time.monotonic()
    if (
        not force_refresh
        and _summary_cache["data"] is not None
        and (now - float(_summary_cache["ts"])) < _SUMMARY_CACHE_TTL_S
    ):
        return dict(_summary_cache["data"])

    cfg = manifest_lab_config()
    mp = manifest_path or Path(cfg["manifest_path"])
    manifest = load_manifest(mp)
    dealer_ids = manifest_dealer_ids(manifest)
    zip_c = cfg["zip_code"]
    radius = cfg["radius_miles"]

    by_dealer: list[dict[str, Any]] = []
    total_cars = 0
    incomplete_cars = 0
    incomplete_ids = _incomplete_id_set()

    if dealer_ids:
        with scan_lab_db_conn() as conn:
            cur = conn.cursor()
            _lab_exec(
                cur,
                f"""
                SELECT dealer_id, MAX(dealer_name) AS dealer_name, COUNT(*) AS car_count
                FROM cars
                WHERE dealer_id IN ({sql_placeholders(dealer_ids)})
                  AND COALESCE(listing_active, 1) = 1
                GROUP BY dealer_id
                ORDER BY car_count DESC, dealer_id ASC
                """,
                tuple(dealer_ids),
            )
            dealer_rows = cur.fetchall()
            inc_by_dealer: dict[str, int] = {}
            if incomplete_ids:
                inc_list = [int(x) for x in incomplete_ids if int(x) > 0]
                if inc_list:
                    _lab_exec(
                        cur,
                        f"""
                        SELECT dealer_id, COUNT(*) AS incomplete_count
                        FROM cars
                        WHERE id IN ({sql_placeholders(inc_list)})
                          AND dealer_id IN ({sql_placeholders(dealer_ids)})
                          AND COALESCE(listing_active, 1) = 1
                        GROUP BY dealer_id
                        """,
                        tuple(inc_list) + tuple(dealer_ids),
                    )
                    inc_by_dealer = {
                        str(row[0]): int(row[1]) for row in cur.fetchall() if row[0]
                    }
            for row in dealer_rows:
                did = str(row[0] or "").strip()
                count = int(row[2] or 0)
                total_cars += count
                inc_n = int(inc_by_dealer.get(did, 0))
                incomplete_cars += inc_n
                by_dealer.append(
                    {
                        "dealer_id": did,
                        "dealer_name": row[1],
                        "car_count": count,
                        "incomplete_count": inc_n,
                    }
                )

    runs = list_scan_runs_for_manifest(manifest_path=mp, limit=100)
    latest_finished = runs[0].get("finished_at") if runs else None

    result = {
        "manifest_path": str(mp),
        "zip_code": zip_c,
        "radius_miles": radius,
        "dealer_count": len(dealer_ids),
        "total_cars": total_cars,
        "incomplete_cars": incomplete_cars,
        "by_dealer": by_dealer,
        "latest_scan_finished_at": latest_finished,
        "scan_run_rows": len(runs),
    }
    _summary_cache["ts"] = now
    _summary_cache["data"] = result
    return dict(result)


def _lab_scan_runs(*, limit: int) -> list[dict[str, Any]]:
    """Newest scan_runs rows of the lab database (a fresh lab has none)."""
    from psycopg.rows import dict_row

    try:
        with scan_lab_db_conn() as conn:
            cur = conn.cursor(row_factory=dict_row)
            cur.execute("SELECT * FROM scan_runs ORDER BY finished_at DESC NULLS LAST, id DESC LIMIT %s", (limit,))
            return [dict(r) for r in cur.fetchall()]
    except Exception:
        return []


def list_scan_runs_for_manifest(
    *,
    manifest_path: Path | None = None,
    limit: int = 80,
) -> list[dict[str, Any]]:
    mp = manifest_path or default_manifest_path()
    dealer_ids = set(manifest_dealer_ids(load_manifest(mp)))
    if not dealer_ids:
        return []
    lim = max(1, min(int(limit), 200))
    all_runs = _lab_scan_runs(limit=max(lim * 2, 80))
    out: list[dict[str, Any]] = []
    for row in all_runs:
        did = str(row.get("dealer_id") or "").strip()
        if did not in dealer_ids:
            continue
        summary: dict[str, Any] = {}
        try:
            summary = json.loads(row.get("summary_json") or "{}")
        except (json.JSONDecodeError, TypeError, ValueError):
            summary = {}
        row = dict(row)
        row["_summary"] = summary
        out.append(row)
        if len(out) >= lim:
            break
    return out


def _light_scan_lab_row(row: dict[str, Any], *, incomplete_ids: set[int]) -> dict[str, Any]:
    cid = row.get("id")
    try:
        car_id = int(cid)
    except (TypeError, ValueError):
        car_id = 0
    dq = row.get("data_quality_score")
    dq_display = None
    if dq is not None:
        try:
            dqf = float(dq)
            dq_display = int(round(dqf * 100)) if dqf <= 1.001 else round(dqf, 1)
        except (TypeError, ValueError):
            dq_display = None
    return {
        "id": car_id,
        "vin": row.get("vin"),
        "year": row.get("year"),
        "make": row.get("make"),
        "model": row.get("model"),
        "trim": row.get("trim"),
        "price": row.get("price"),
        "mileage": row.get("mileage"),
        "dealer_id": row.get("dealer_id"),
        "dealer_name": row.get("dealer_name"),
        "image_url": row.get("image_url"),
        "data_quality_score": row.get("data_quality_score"),
        "data_quality_display": dq_display,
        "scraped_at": row.get("scraped_at"),
        "stock_number": row.get("stock_number"),
        "public_incomplete": car_id > 0 and car_id in incomplete_ids,
    }


def list_scan_lab_cars(
    *,
    manifest_path: Path | None = None,
    dealer_id: str | None = None,
    q: str | None = None,
    limit: int = 50,
    offset: int = 0,
) -> dict[str, Any]:
    """
    Paginated lightweight car rows for manifest dealers only.

    Manifest dealers are already the 25 mi discovery set — no per-row geo filter.
    """
    cfg = manifest_lab_config()
    mp = manifest_path or Path(cfg["manifest_path"])
    allowed = manifest_dealer_ids(load_manifest(mp))
    if not allowed:
        return {"cars": [], "total": 0, "limit": limit, "offset": offset}

    filter_ids = allowed
    if dealer_id and str(dealer_id).strip():
        did = str(dealer_id).strip()
        filter_ids = [did] if did in allowed else []

    lim = max(1, min(int(limit), 200))
    off = max(0, int(offset))
    incomplete_ids = _incomplete_id_set()
    cols = ", ".join(_CAR_LIST_COLUMNS)
    params: list[Any] = list(filter_ids)
    where = (
        f"dealer_id IN ({sql_placeholders(filter_ids)}) AND COALESCE(listing_active, 1) = 1"
    )
    q_norm = (q or "").strip()
    if q_norm:
        like = f"%{q_norm.lower()}%"
        where += (
            " AND (LOWER(COALESCE(vin,'')) LIKE ?"
            " OR LOWER(COALESCE(stock_number,'')) LIKE ?"
            " OR LOWER(COALESCE(make,'') || ' ' || COALESCE(model,'')) LIKE ?)"
        )
        params.extend([like, like, like])

    with scan_lab_db_conn() as conn:
        cur = conn.cursor()
        _lab_exec(cur, f"SELECT COUNT(*) FROM cars WHERE {where}", tuple(params))
        total = int(cur.fetchone()[0] or 0)
        _lab_exec(
            cur,
            f"""
            SELECT {cols} FROM cars
            WHERE {where}
            ORDER BY COALESCE(scraped_at, first_seen_at) DESC NULLS LAST, id DESC
            LIMIT ? OFFSET ?
            """,
            tuple(params) + (lim, off),
        )
        col_names = [d[0] for d in cur.description]
        rows = [dict(zip(col_names, r)) for r in cur.fetchall()]

    return {
        "cars": [_light_scan_lab_row(r, incomplete_ids=incomplete_ids) for r in rows],
        "total": total,
        "limit": lim,
        "offset": off,
    }


def car_in_manifest_scope(car: dict[str, Any]) -> bool:
    did = str(car.get("dealer_id") or "").strip()
    return bool(did) and did in set(manifest_lab_config()["dealer_ids"])


def get_scan_lab_car_by_id(car_id: int, *, include_inactive: bool = True) -> dict[str, Any] | None:
    """Load a full car row from the scratch lab database (never production ``cars``)."""
    from psycopg.rows import dict_row

    with scan_lab_db_conn() as conn:
        cur = conn.cursor(row_factory=dict_row)
        if include_inactive:
            _lab_exec(cur, "SELECT * FROM cars WHERE id = ?", (car_id,))
        else:
            _lab_exec(
                cur,
                "SELECT * FROM cars WHERE id = ? AND (COALESCE(listing_active, 1) = 1)",
                (car_id,),
            )
        row = cur.fetchone()
    if not row:
        return None
    car = dict(row)
    _parse_car_gallery(car)
    _parse_car_history_highlights(car)
    return car


def get_scan_lab_car_row(car_id: int) -> dict[str, Any] | None:
    raw = get_scan_lab_car_by_id(car_id)
    if not raw:
        return None
    from backend.utils.car_serialize import redact_sensitive_car_row

    return redact_sensitive_car_row(dict(raw))


def patch_scan_lab_car(car_id: int, fields: dict[str, Any]) -> bool:
    if not fields:
        return False
    if not get_scan_lab_car_by_id(car_id):
        return False
    sets: list[str] = []
    vals: list[Any] = []
    for k, raw in fields.items():
        if k not in _UPDATABLE_CAR_COLUMNS:
            continue
        if k == "gallery" and isinstance(raw, list):
            raw = json.dumps(raw)
        sets.append(f"{k} = ?")
        vals.append(raw)
    if not sets:
        return False
    vals.append(car_id)
    with scan_lab_db_conn() as conn:
        _lab_exec(conn.cursor(), f"UPDATE cars SET {', '.join(sets)} WHERE id = ?", vals)
    invalidate_scan_lab_summary_cache()
    return True


def _append_scan_lab_log(job_id: str, text: str) -> None:
    with scan_lab_lock:
        job = scan_lab_jobs.get(job_id)
        if not job:
            return
        job["log"] = (job.get("log") or "") + text


def _scan_lab_subprocess_env(resolved_manifest: Path) -> dict[str, str]:
    """Env for the lab scanner subprocess.

    The inherited Postgres DSN is stripped: the lab must never write to the
    real inventory. Point the lab at a scratch Postgres with
    ``SCAN_LAB_DATABASE_URL``; ``_run_manifest_scan_job`` refuses to spawn the
    scanner without it. ``PROJECT_DOTENV_DISABLE=1`` keeps the child's own
    ``load_project_dotenv()`` from refilling the stripped DSN from ``.env``.
    """
    env = os.environ.copy()
    try:
        lab_dsn = scan_lab_dsn()
    except ScanLabRefused:
        lab_dsn = ""
    env.pop("SCAN_LAB_DATABASE_URL", None)
    env.pop("DATABASE_URL", None)
    env.pop("INVENTORY_DATABASE_URL", None)
    if lab_dsn:
        env["INVENTORY_DATABASE_URL"] = lab_dsn
    env["PROJECT_DOTENV_DISABLE"] = "1"
    env["DEALERS_MANIFEST_PATH"] = str(resolved_manifest)
    env.update({"SCANNER_SCAN_ONLY": "1", "SCANNER_MAX_DEALER_CONCURRENCY": "1", "SCANNER_VDP_DOWNLOAD_IMAGES": "0"})
    env.pop("SCANNER_ALLOW_BROWSER", None)
    env.pop("ANTHROPIC_API_KEY", None)
    return env


def _finish_scan_lab_job(job_id: str, log_parts: list[str], started: str, code: int) -> None:
    finished = datetime.now(timezone.utc).isoformat()
    _append_scan_lab_log(job_id, f"\n[scan-lab] Finished {finished} exit_code={code}\n")
    log_parts.append(f"\n[scan-lab] Finished {finished} exit_code={code}\n")
    invalidate_scan_lab_summary_cache()

    with scan_lab_lock:
        if job_id in scan_lab_jobs:
            scan_lab_jobs[job_id]["done"] = True
            scan_lab_jobs[job_id]["exit_code"] = code
            scan_lab_jobs[job_id]["log"] = "".join(log_parts)
            scan_lab_jobs[job_id]["finished_at"] = finished


def _run_manifest_scan_job(job_id: str, manifest_path: Path) -> None:
    log_parts: list[str] = []

    def append(s: str) -> None:
        log_parts.append(s)
        _append_scan_lab_log(job_id, s)

    started = datetime.now(timezone.utc).isoformat()
    append(f"[scan-lab] Started {started}\n")
    append(f"[scan-lab] Manifest: {manifest_path}\n")

    resolved_manifest = manifest_path.resolve()
    # HTTP-only scan with a small footprint (the old Mac-mini-lite preset ran a
    # browser; scans never do now — docs/HTTP_ONLY_SCANS_PLAN.md)
    env = _scan_lab_subprocess_env(resolved_manifest)
    if not (env.get("INVENTORY_DATABASE_URL") or "").strip():
        try:
            scan_lab_dsn()
            why = ""
        except ScanLabRefused as e:
            why = f" ({e})"
        append(f"[scan-lab] Refused: set SCAN_LAB_DATABASE_URL to a scratch Postgres; the lab never writes to the real inventory{why}.\n")
        _finish_scan_lab_job(job_id, log_parts, started, 2)
        return

    cmd = [
        sys.executable,
        str(PROJECT_ROOT / "scanner.py"),
        "--manifest",
        str(resolved_manifest),
        "--scan-only",
        "--dealer-concurrency", "1",
    ]
    append(f"[scan-lab] Command: {' '.join(cmd)}\n")
    append("[scan-lab] HTTP-only lite profile (dealer_concurrency=1, no browser, Claude/vision off)\n\n")

    code = 1
    try:
        proc = subprocess.Popen(
            cmd,
            cwd=str(PROJECT_ROOT),
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
        assert proc.stdout is not None
        for line in proc.stdout:
            append(line)
        code = proc.wait()
    except Exception as e:
        append(f"\n[scan-lab] Failed to start scanner: {e}\n")
        logger.exception("scan-lab manifest scan failed to start")

    _finish_scan_lab_job(job_id, log_parts, started, code)


def start_manifest_scan(*, manifest_path: Path | None = None) -> tuple[str | None, str | None]:
    mp = manifest_path or default_manifest_path()
    if not mp.is_file():
        return None, "manifest_not_found"
    job_id = uuid.uuid4().hex
    with scan_lab_lock:
        for job in scan_lab_jobs.values():
            if not job.get("done"):
                return None, "scan_already_running"
        scan_lab_jobs[job_id] = {
            "log": "",
            "done": False,
            "exit_code": None,
            "started_at": datetime.now(timezone.utc).isoformat(),
            "manifest_path": str(mp),
            "finished_at": None,
        }
    threading.Thread(
        target=_run_manifest_scan_job,
        args=(job_id, mp),
        name=f"scan-lab-{job_id[:8]}",
        daemon=True,
    ).start()
    return job_id, None


def get_scan_lab_job(job_id: str) -> dict[str, Any] | None:
    with scan_lab_lock:
        job = scan_lab_jobs.get(job_id)
        return dict(job) if job else None


def active_scan_lab_job() -> dict[str, Any] | None:
    with scan_lab_lock:
        for jid, job in scan_lab_jobs.items():
            if not job.get("done"):
                out = dict(job)
                out["job_id"] = jid
                return out
    return None


def db_table_counts() -> dict[str, int]:
    counts: dict[str, int] = {}
    with scan_lab_db_conn() as conn:
        cur = conn.cursor()
        for table in ("cars", "dealerships", "scan_runs", "dealer_scan_profile"):
            try:
                cur.execute(f"SELECT COUNT(*) FROM {table}")
                counts[table] = int(cur.fetchone()[0])
            except Exception:
                conn.rollback()  # a missing table aborts the transaction
                counts[table] = -1
    return counts
