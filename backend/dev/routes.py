"""
Developer dashboard at /dev: session-based admin login (separate from public users).
Smart URL import, scanner jobs, dealership registry tools.
"""

from __future__ import annotations

import ipaddress
import json
import logging
import os

from backend.config import Config
import posixpath
import re
import signal
import sqlite3
import subprocess
import threading
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from flask import Blueprint, jsonify, redirect, render_template, request, session, url_for
from pydantic import ValidationError

from backend.db.admin_users_db import (
    authenticate_admin,
    dev_public_registration_allowed,
    dev_users_db_path,
    save_dev_admin_user,
)
from backend.db.dealerships_db import (
    DB_PATH,
    deduplicate_dealerships,
    delete_dealership,
    geocode_missing_dealerships,
    insert_dealership,
    list_recent_dealerships,
)
from backend.dev.dealers import (
    DEALERS_PATH,
    load_dealers,
    normalize_manifest_url,
    slug_from_url,
    smart_import_manifest_display_name,
    upsert_dealer_manifest_row,
)
from backend.dev.pipeline_jobs import (
    PIPELINE_SUCCESS_VERDICTS,
    pipeline_command,
    pipeline_out_dir,
    read_triage_row,
    summarize_triage_row,
)
from backend.db.incomplete_listings_db import get_incomplete_listings_count
from backend.db.inventory_db import (
    get_car_by_id,
    get_car_by_vin,
    get_conn,
    get_dealership_issue_stats,
)
from backend.enrichment.knowledge_engine import prepare_car_detail_context
from backend.utils.car_serialize import (
    SENSITIVE_CAR_ROW_KEYS,
    build_detail_display_snapshot,
    redact_sensitive_car_row,
    serialize_car_for_api,
)
from backend.utils.client_ip import client_ip
from backend.utils.ip_rate_limit import allow_request
from backend.utils.registration_validation import registration_form_error
from backend.utils.outbound_url import validate_dev_scanner_url
from backend.schemas.dealership import DealerCreate

PROJECT_ROOT = Path(__file__).resolve().parents[2]

dev_bp = Blueprint("dev", __name__)

# Process-local fallback store for scanner jobs / bulk-import queues, used when
# DEV_JOB_STORE_SQLITE_PATH (below) is not set. NOTE: with GUNICORN_WORKERS>1
# (see scripts/docker-entrypoint-web.sh) these dicts are NOT shared across worker
# processes, so a status poll that lands on a different worker than the one that
# started the job sees no record of it and gets back job_expired/queue_expired
# even though the job is still running fine on its own worker. Set
# DEV_JOB_STORE_SQLITE_PATH to a path all workers can read/write (WAL-mode
# SQLite) to make job/queue state visible to every worker, the same way
# RATE_LIMIT_SQLITE_PATH shares rate-limit state (backend/utils/ip_rate_limit.py).
scanner_jobs: dict[str, dict[str, Any]] = {}
scanner_lock = threading.Lock()

import_queues: dict[str, dict[str, Any]] = {}

# Cap the job/queue stores so full scan logs don't grow worker RSS (or the
# shared SQLite file) without bound. Oldest entries are evicted first (dicts
# preserve insert order; the SQLite path orders by last-write time).
_MAX_SCANNER_JOBS = max(1, int(os.environ.get("DEV_MAX_SCANNER_JOBS", "500")))
_MAX_IMPORT_QUEUES = max(1, int(os.environ.get("DEV_MAX_IMPORT_QUEUES", "100")))

_DEV_JOB_STORE_SQLITE_PATH = (os.environ.get("DEV_JOB_STORE_SQLITE_PATH") or "").strip() or None
_dev_job_store_sqlite_lock = threading.Lock()


def _evict_old_entries(store: dict[str, Any], max_entries: int) -> None:
    """Trim an insertion-ordered job store to its most recent ``max_entries``.

    Callers must hold ``scanner_lock`` while mutating ``store``.
    """
    while len(store) > max_entries:
        del store[next(iter(store))]


def _dev_job_store_enabled() -> bool:
    return _DEV_JOB_STORE_SQLITE_PATH is not None


def _dev_job_store_conn() -> sqlite3.Connection:
    path = _DEV_JOB_STORE_SQLITE_PATH
    assert path
    d = os.path.dirname(path)
    if d:
        try:
            os.makedirs(d, exist_ok=True)
        except OSError:
            pass
    conn = sqlite3.connect(path, check_same_thread=False, timeout=5.0)
    try:
        conn.execute("PRAGMA journal_mode=WAL")
    except sqlite3.OperationalError:
        pass
    try:
        conn.execute("PRAGMA busy_timeout=5000")
    except sqlite3.OperationalError:
        pass
    conn.execute(
        "CREATE TABLE IF NOT EXISTS dev_job_store ("
        "store TEXT NOT NULL, id TEXT NOT NULL, data TEXT NOT NULL, updated_at REAL NOT NULL, "
        "PRIMARY KEY (store, id))"
    )
    return conn


def _dev_local_store(store: str) -> dict[str, dict[str, Any]]:
    return scanner_jobs if store == "jobs" else import_queues


def _dev_store_get(store: str, item_id: str) -> dict[str, Any] | None:
    """Read one job/queue record: from the shared SQLite store when configured,
    otherwise from the process-local dict named by ``store`` ("jobs" or "queues")."""
    if not _dev_job_store_enabled():
        with scanner_lock:
            item = _dev_local_store(store).get(item_id)
            return dict(item) if item is not None else None
    with _dev_job_store_sqlite_lock:
        try:
            conn = _dev_job_store_conn()
        except sqlite3.Error:
            return None
        try:
            row = conn.execute(
                "SELECT data FROM dev_job_store WHERE store = ? AND id = ?", (store, item_id)
            ).fetchone()
        finally:
            conn.close()
    if not row:
        return None
    try:
        return json.loads(row[0])
    except (json.JSONDecodeError, TypeError):
        return None


def _dev_store_put(store: str, item_id: str, data: dict[str, Any]) -> None:
    """Create/replace one job/queue record, then evict the oldest past the cap."""
    max_entries = _MAX_SCANNER_JOBS if store == "jobs" else _MAX_IMPORT_QUEUES
    if not _dev_job_store_enabled():
        with scanner_lock:
            local = _dev_local_store(store)
            local[item_id] = data
            _evict_old_entries(local, max_entries)
        return
    now = time.time()
    payload = json.dumps(data, default=str)
    with _dev_job_store_sqlite_lock:
        try:
            conn = _dev_job_store_conn()
        except sqlite3.Error:
            return
        try:
            conn.execute(
                "INSERT INTO dev_job_store (store, id, data, updated_at) VALUES (?, ?, ?, ?) "
                "ON CONFLICT(store, id) DO UPDATE SET data = excluded.data, updated_at = excluded.updated_at",
                (store, item_id, payload, now),
            )
            conn.execute(
                "DELETE FROM dev_job_store WHERE store = ? AND id NOT IN ("
                "SELECT id FROM dev_job_store WHERE store = ? ORDER BY updated_at DESC LIMIT ?)",
                (store, store, max_entries),
            )
            conn.commit()
        except sqlite3.Error:
            try:
                conn.rollback()
            except sqlite3.Error:
                pass
        finally:
            conn.close()


def _dev_store_patch(store: str, item_id: str, fields: dict[str, Any]) -> bool:
    """Merge ``fields`` into an existing job/queue record. Returns False if it's gone."""
    if not _dev_job_store_enabled():
        with scanner_lock:
            local = _dev_local_store(store)
            if item_id not in local:
                return False
            local[item_id].update(fields)
            return True
    with _dev_job_store_sqlite_lock:
        try:
            conn = _dev_job_store_conn()
        except sqlite3.Error:
            return False
        try:
            row = conn.execute(
                "SELECT data FROM dev_job_store WHERE store = ? AND id = ?", (store, item_id)
            ).fetchone()
            if not row:
                return False
            try:
                data = json.loads(row[0])
            except (json.JSONDecodeError, TypeError):
                data = {}
            data.update(fields)
            conn.execute(
                "UPDATE dev_job_store SET data = ?, updated_at = ? WHERE store = ? AND id = ?",
                (json.dumps(data, default=str), time.time(), store, item_id),
            )
            conn.commit()
            return True
        except sqlite3.Error:
            try:
                conn.rollback()
            except sqlite3.Error:
                pass
            return False
        finally:
            conn.close()


def _dev_queue_patch_item(queue_id: str, job_id: str, **fields: Any) -> None:
    """Merge ``fields`` into one item of a queue's ``items`` list (matched by job_id)."""
    if not _dev_job_store_enabled():
        with scanner_lock:
            q = import_queues.get(queue_id)
            if not q:
                return
            for it in q.get("items", []):
                if it.get("job_id") == job_id:
                    it.update(fields)
                    break
        return
    with _dev_job_store_sqlite_lock:
        try:
            conn = _dev_job_store_conn()
        except sqlite3.Error:
            return
        try:
            row = conn.execute(
                "SELECT data FROM dev_job_store WHERE store = 'queues' AND id = ?", (queue_id,)
            ).fetchone()
            if not row:
                return
            try:
                data = json.loads(row[0])
            except (json.JSONDecodeError, TypeError):
                return
            for it in data.get("items", []):
                if it.get("job_id") == job_id:
                    it.update(fields)
                    break
            conn.execute(
                "UPDATE dev_job_store SET data = ?, updated_at = ? WHERE store = 'queues' AND id = ?",
                (json.dumps(data, default=str), time.time(), queue_id),
            )
            conn.commit()
        except sqlite3.Error:
            try:
                conn.rollback()
            except sqlite3.Error:
                pass
        finally:
            conn.close()


LAST_SCRAPE_SAMPLES_PATH = PROJECT_ROOT / "debug" / "last_scrape_samples.json"

logger = logging.getLogger(__name__)

_MIN_PASSWORD_LEN = Config.MIN_PASSWORD_LENGTH
_DEV_LOGIN_RPM = int(os.environ.get("RATE_LIMIT_DEV_LOGIN_PER_MIN", "20"))
_DEV_REGISTER_RPM = int(os.environ.get("RATE_LIMIT_DEV_REGISTER_PER_MIN", "5"))
def _vector_reindex_background() -> None:
    try:
        from backend.vector.pgvector_service import reindex_all

        reindex_all()
    except Exception:
        logger.exception("pgvector reindex failed after scanner job")


def _spawn_vector_reindex_background() -> None:
    """Refresh pgvector embeddings from SQLite after a successful scanner run (non-blocking)."""
    threading.Thread(target=_vector_reindex_background, daemon=True).start()


def _admin_session_ok() -> bool:
    if session.get("admin_user_id"):
        return True
    from backend.utils.production_security import app_admin_dev_pass_through_allowed
    from backend.utils.roles import is_admin_role

    if not app_admin_dev_pass_through_allowed():
        return False
    return bool(session.get("user_id")) and is_admin_role(session.get("user_role"))


def _finalize_dev_session(*, user_id: int, username: str) -> bool:
    try:
        uid = int(user_id)
    except (TypeError, ValueError):
        return False
    if uid <= 0:
        return False
    uname = (username or "").strip() or f"user_{uid}"
    session["admin_user_id"] = uid
    session["admin_username"] = uname
    return True


def _safe_dev_next_url(next_url: str, *, default_endpoint: str = "dev.dev_dashboard") -> str:
    """Post-login redirect: same-origin ``/dev`` paths only (SEC-021)."""
    default = url_for(default_endpoint)
    raw = (next_url or "").strip()
    if not raw.startswith("/") or raw.startswith("//") or "\\" in raw:
        return default
    path = posixpath.normpath(urlparse(raw).path or "/")
    if path != "/dev" and not path.startswith("/dev/"):
        return default
    return raw


def _dev_client_ip_allowed(req) -> bool:
    """Optional IP allowlist for ``/dev`` (VPN or bastion); uses :func:`client_ip` (proxy-aware)."""
    raw = (os.environ.get("DEV_IP_ALLOWLIST") or "").strip()
    if not raw:
        return True
    ip_str = (client_ip(req) or "").strip()
    if not ip_str or ip_str == "unknown":
        return False
    if "%" in ip_str:
        ip_str = ip_str.split("%", 1)[0]
    try:
        addr = ipaddress.ip_address(ip_str)
    except ValueError:
        return False
    for part in raw.split(","):
        token = part.strip()
        if not token:
            continue
        if "/" in token:
            try:
                net = ipaddress.ip_network(token, strict=False)
                if addr in net:
                    return True
            except ValueError:
                pass
            continue
        try:
            if addr == ipaddress.ip_address(token):
                return True
        except ValueError:
            continue
    return False


@dev_bp.before_request
def _dev_require_admin() -> Any:
    from backend.utils.csrf import validate_csrf_form, validate_csrf_header

    ep = request.endpoint or ""
    # Legacy /dev/mfa/* URLs (2FA removed): always allow through to the redirect handler.
    if ep == "dev.dev_mfa_gone":
        return None
    if not _dev_client_ip_allowed(request):
        if request.path.startswith("/dev/api"):
            return jsonify({"ok": False, "error": "forbidden", "reason": "dev_ip_allowlist"}), 403
        from werkzeug.exceptions import Forbidden

        raise Forbidden()

    if request.method in ("POST", "PUT", "PATCH", "DELETE"):
        if ep in ("dev.admin_login", "dev.admin_register", "dev.admin_logout"):
            csrf_resp = validate_csrf_form()
            if csrf_resp is not None:
                return csrf_resp
        else:
            validate_csrf_header()

    if ep in (
        "dev.admin_login",
        "dev.admin_register",
        "dev.admin_logout",
    ):
        return None
    if _admin_session_ok():
        # Populate dev session label for templates when using app-admin pass-through
        if not session.get("admin_user_id") and session.get("user_id"):
            session.setdefault("admin_username", session.get("username") or "admin")
        return None
    if request.path.startswith("/dev/api"):
        return jsonify({"ok": False, "error": "unauthorized", "login_url": "/dev/login"}), 401
    return redirect(url_for("dev.admin_login", next=request.full_path))


def _json_body_no_token(data: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(data, dict):
        return {}
    return {k: v for k, v in data.items() if k != "token"}


def _dev_scanner_url_or_error(url: str) -> tuple[str | None, str | None]:
    """Return ``(normalized_url, error_code)`` for dev scanner subprocess endpoints."""
    normalized = (url or "").strip()
    err = validate_dev_scanner_url(normalized)
    if err:
        return None, err
    return normalized, None


def _scan_pipeline_status() -> tuple[bool, str]:
    """Whether the dev import / scan buttons can run, and the line the status panel shows."""
    script = PROJECT_ROOT / "backend" / "scripts" / "dealer_pipeline.py"
    if not script.is_file():
        return False, f"backend/scripts/dealer_pipeline.py is missing at {script}."
    if not DEALERS_PATH.is_file():
        return True, (
            f"python -m backend.scripts.dealer_pipeline (HTTP-only). Dealer manifest not found at "
            f"{DEALERS_PATH}; the first smart import creates it."
        )
    return True, f"python -m backend.scripts.dealer_pipeline (HTTP-only) · manifest {DEALERS_PATH}"


def _dev_status() -> dict[str, Any]:
    db_ok = False
    try:
        conn = get_conn()
        conn.cursor().execute("SELECT 1")
        conn.close()
        db_ok = True
    except (OSError, sqlite3.Error):
        pass
    from backend.utils.runtime_env import is_production_env

    prod = is_production_env()
    env_file = PROJECT_ROOT / ".env"
    admin_pw_set = bool((os.environ.get("ADMIN_PASSWORD") or "").strip())
    pipeline_ok, pipeline_line = _scan_pipeline_status()
    return {
        "db_connected": db_ok,
        "inventory_db_path": str(DB_PATH),
        "dev_users_db_path": dev_users_db_path(),
        "dev_registration_open": dev_public_registration_allowed(),
        "scan_pipeline_ok": pipeline_ok,
        "scan_pipeline_status_line": pipeline_line,
        "is_production": prod,
        "admin_password_configured": admin_pw_set or not prod,
        "dotenv_file_present": env_file.is_file(),
        "dotenv_file_path": str(env_file),
    }


def _dev_status_shell() -> dict[str, Any]:
    """Status for SSR. Every check is cheap now (no Node probe), so it is the full status."""
    return _dev_status()


# Pipeline stdout lines that start with a stage name become "discovery" steps in
# the dev UI (the old scanner.js printed DISCOVERY:{json} lines for the same panel).
_PIPELINE_STAGE_RE = re.compile(r"^(recipe|probe|scan|vpic|assess|lifecyc|log)\s+\S")
_HEADED_NOTE = (
    "[dev] 'headed' is ignored: scans are HTTP-only by policy (backend/scanner/browser_gate.py). "
    "When the HTTP recipe finds nothing, the pipeline runs the one sanctioned headless "
    "discovery capture itself.\n"
)


def _manifest_row_for_url(url: str) -> dict[str, Any] | None:
    """The dealers.json row whose url (or url-derived dealer_id) matches ``url``."""
    key_url = normalize_manifest_url(url).lower()
    if not key_url:
        return None
    slug = slug_from_url(key_url)
    try:
        rows = load_dealers(DEALERS_PATH)
    except (OSError, ValueError):
        return None
    for r in rows:
        if normalize_manifest_url(str(r.get("url") or "")).lower() == key_url:
            return r
    for r in rows:
        if str(r.get("dealer_id") or "").strip() == slug:
            return r
    return None


def _smart_import_display_name(url: str) -> str:
    """Dealer name for a new manifest row: the homepage's own name, else the title-cased host."""
    try:
        from backend.dev.dealer_url_infer import infer_dealer_from_url

        inferred = infer_dealer_from_url(url, timeout=15.0)
    except Exception:  # noqa: BLE001 - a name lookup must never fail the import
        inferred = {}
    name = str(((inferred or {}).get("dealer") or {}).get("name") or "").strip()
    if (inferred or {}).get("ok") and name and name != "Dealership":
        return name
    return smart_import_manifest_display_name(url, resolved=None, error_partial={}, discovery=[])


def _run_dealer_pipeline(
    job_id: str,
    dealer_id: str,
    *,
    append,
    discovery: list[dict[str, Any]],
    cancel_requested=None,
) -> tuple[int | None, dict[str, Any]]:
    """Run ``backend.scripts.dealer_pipeline`` for one dealer, streaming its log into the job.

    Returns ``(exit_code, summary)``; ``summary`` is the dealer's triage verdict
    (``backend.dev.pipeline_jobs.summarize_triage_row``) plus ``out_dir``.
    """
    out_dir = pipeline_out_dir(f"{dealer_id}_{job_id[:8]}")
    cmd = pipeline_command(dealer_id, out_dir, manifest_path=DEALERS_PATH)
    append(f"[dev] Running: python {' '.join(cmd[1:])}\n")
    try:
        proc = subprocess.Popen(
            cmd,
            cwd=str(PROJECT_ROOT),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            # Own process group: a cancel stops the pipeline and the scanner it spawned.
            start_new_session=True,
        )
    except FileNotFoundError as e:
        append(f"\n[dev] Failed to start the dealer pipeline: {e}\n")
        return 127, {**summarize_triage_row(None), "out_dir": str(out_dir)}
    except OSError as e:
        append(f"\n[dev] Dealer pipeline error: {e}\n")
        return -1, {**summarize_triage_row(None), "out_dir": str(out_dir)}

    if cancel_requested is not None:

        def watch_for_cancel() -> None:
            # Own thread so a pipeline with no output at all (a true hang) still
            # gets torn down promptly; the stdout loop below blocks on readline.
            while proc.poll() is None:
                if cancel_requested():
                    try:
                        os.killpg(proc.pid, signal.SIGTERM)
                    except OSError:
                        try:
                            proc.terminate()
                        except OSError:
                            pass
                    return
                time.sleep(0.5)

        threading.Thread(target=watch_for_cancel, daemon=True).start()

    if proc.stdout:
        for line in proc.stdout:
            append(line)
            if _PIPELINE_STAGE_RE.match(line):
                discovery.append({"step": line.split(None, 1)[0], "message": line.strip()[:300]})
                _dev_store_patch("jobs", job_id, {"discovery": list(discovery)})
    code = proc.wait()
    summary = {**summarize_triage_row(read_triage_row(out_dir, dealer_id)), "out_dir": str(out_dir)}
    append(
        f"\n[dev] Pipeline verdict: {summary.get('verdict') or 'none'} "
        f"({summary.get('rows', 0)} rows) {summary.get('reason') or ''}\n"
        f"[dev] Triage: {out_dir / 'triage.json'} · dealer logs: workspace/dealer_logs/{dealer_id}/\n"
    )
    return code, summary


def _pipeline_error(summary: dict[str, Any], code: int | None) -> dict[str, Any] | None:
    """``smart_error`` payload for the UI, or None when the pipeline landed rows."""
    verdict = summary.get("verdict")
    if code == 0 and verdict in PIPELINE_SUCCESS_VERDICTS:
        return None
    return {
        "reason": verdict or ("pipeline_exit_" + str(code)),
        "detail": summary.get("reason"),
        "retryable": False,
    }


def _run_scanner_job(job_id: str, url: str, headed: bool = False) -> None:
    """Scan a dealer that is already on the roster (dealers.json or the DB) by its URL.

    Was ``node scanner.js --url``; now the dealer pipeline for the dealer the URL
    names. A URL nobody knows yet goes through smart import, which adds the
    manifest row first.
    """
    log_parts: list[str] = []
    discovery: list[dict[str, Any]] = []

    def append(text: str) -> None:
        log_parts.append(text)
        _dev_store_patch("jobs", job_id, {"log": "".join(log_parts)})

    if headed:
        append(_HEADED_NOTE)
    row = _manifest_row_for_url(url)
    dealer_id = str((row or {}).get("dealer_id") or "") or slug_from_url(url)
    if not row:
        append(
            f"[dev] {url} is not in {DEALERS_PATH.name}; the pipeline will look up "
            f"dealer_id={dealer_id} in the database. Use Import & scan to add a new dealer.\n"
        )
    code, summary = _run_dealer_pipeline(job_id, dealer_id, append=append, discovery=discovery)
    _dev_store_patch(
        "jobs",
        job_id,
        {
            "done": True,
            "exit_code": code,
            "discovery": list(discovery),
            "dealer_id": dealer_id,
            "verdict": summary.get("verdict"),
            "rows": summary.get("rows"),
            "smart_error": _pipeline_error(summary, code),
        },
    )
    if _pipeline_error(summary, code) is None:
        _spawn_vector_reindex_background()


def _run_smart_import_job(job_id: str, url: str, headed: bool = False) -> None:
    """Add a dealer by URL and scan it: dealers.json upsert -> dealer pipeline.

    Replaced ``node scanner.js --smart-import`` (Puppeteer, three browser profiles).
    The pipeline synthesizes the recipe from the homepage, scans over HTTP, checks
    VINs against NHTSA and writes the dealer logs. The registry row (city/state) is
    not written here: nothing in the HTTP path resolves a street address yet, so
    add it with "Insert dealer" when needed (the manifest row is enough to scan).
    """
    log_parts: list[str] = []
    discovery: list[dict[str, Any]] = []

    def append(text: str) -> None:
        log_parts.append(text)
        _dev_store_patch("jobs", job_id, {"log": "".join(log_parts), "discovery": list(discovery)})

    def cancel_requested() -> bool:
        job = _dev_store_get("jobs", job_id)
        return bool(job and job.get("cancel_requested"))

    def finish(code: int | None, fields: dict[str, Any]) -> None:
        _dev_store_patch(
            "jobs",
            job_id,
            {"done": True, "exit_code": code, "discovery": list(discovery), "insert_id": None, **fields},
        )

    if headed:
        append(_HEADED_NOTE)
    if cancel_requested():
        append("\n[dev] Job cancelled before starting.\n")
        finish(None, {"smart_error": {"reason": "cancelled", "retryable": False}})
        return

    wurl = normalize_manifest_url(url)
    if not wurl:
        append("[dev] Could not normalize the URL for dealers.json.\n")
        finish(None, {"smart_error": {"reason": "invalid_url", "retryable": False}})
        return

    existing = _manifest_row_for_url(wurl)
    try:
        if existing:
            action, dealer_id = upsert_dealer_manifest_row(
                name=str(existing.get("name") or ""),
                website_url=wurl,
                provider=str(existing.get("provider") or "unknown"),
                dealer_id=str(existing.get("dealer_id") or "") or None,
                manifest_path=DEALERS_PATH,
            )
        else:
            name = _smart_import_display_name(wurl)
            discovery.append({"step": "name", "message": f"Found name: {name}"})
            # provider "unknown": recipe synthesis fingerprints the platform itself.
            action, dealer_id = upsert_dealer_manifest_row(
                name=name, website_url=wurl, provider="unknown", manifest_path=DEALERS_PATH
            )
    except (OSError, ValueError) as e:
        append(f"[dev] dealers.json upsert failed: {e}\n")
        finish(None, {"smart_error": {"reason": "manifest_write_failed", "detail": str(e), "retryable": False}})
        return
    append(f"[dev] dealers.json {action}: dealer_id={dealer_id} ({DEALERS_PATH})\n")

    code, summary = _run_dealer_pipeline(
        job_id, dealer_id, append=append, discovery=discovery, cancel_requested=cancel_requested
    )
    error = _pipeline_error(summary, code)
    if cancel_requested():
        append("\n[dev] Job cancelled.\n")
        error = {"reason": "cancelled", "retryable": False}
    finish(
        code,
        {
            "dealer_id": dealer_id,
            "verdict": summary.get("verdict"),
            "rows": summary.get("rows"),
            "insert_error": None,
            "smart_error": error,
            "cars_linked": None,
        },
    )
    if error is None:
        _spawn_vector_reindex_background()


@dev_bp.route("/login", methods=["GET", "POST"])
def admin_login():
    if _admin_session_ok():
        return redirect(url_for("dev.dev_dashboard"))
    registered = (request.args.get("registered") or "").strip().lower() in ("1", "true", "yes")
    if request.method == "POST":
        ip = client_ip(request)
        if not allow_request(f"dev_login:{ip}", max_events=_DEV_LOGIN_RPM, window_seconds=60.0):
            return (
                render_template(
                    "admin_login.html",
                    error="Too many sign-in attempts. Try again in a minute.",
                    registration_open=dev_public_registration_allowed(),
                ),
                429,
            )
        login_input = (request.form.get("login") or "").strip()
        password = (request.form.get("password") or "").strip()
        auth = authenticate_admin(login_input, password)
        if auth:
            uid, uname = auth
            session.clear()
            raw_next = (request.form.get("next") or request.args.get("next") or "").strip()
            nxt = _safe_dev_next_url(raw_next)
            if not _finalize_dev_session(user_id=int(uid), username=str(uname)):
                return redirect(url_for("dev.admin_login"))
            return redirect(nxt)
        return render_template(
            "admin_login.html",
            error="Invalid username/email or password.",
            registration_open=dev_public_registration_allowed(),
        )
    return render_template(
        "admin_login.html",
        registered_ok=registered,
        registration_open=dev_public_registration_allowed(),
    )


@dev_bp.route("/register", methods=["GET", "POST"])
def admin_register():
    if _admin_session_ok():
        return redirect(url_for("dev.dev_dashboard"))
    if not dev_public_registration_allowed():
        msg = (
            "Dev account registration is disabled. In production, set ALLOW_DEV_PUBLIC_REGISTER=1 to allow "
            "new operator sign-up, or use ADMIN_PASSWORD bootstrap. In development, set DEV_DISABLE_PUBLIC_REGISTER=1 to turn this off."
        )
        if request.method == "POST":
            return render_template("dev_register.html", error=msg, registration_allowed=False), 403
        return render_template("dev_register.html", error=msg, registration_allowed=False)
    if request.method == "POST":
        ip = client_ip(request)
        if not allow_request(f"dev_register:{ip}", max_events=_DEV_REGISTER_RPM, window_seconds=60.0):
            return (
                render_template(
                    "dev_register.html",
                    error="Too many registration attempts. Try again later.",
                    registration_allowed=True,
                ),
                429,
            )
        username = request.form.get("username", "")
        email = request.form.get("email", "")
        password = request.form.get("password", "")
        err = registration_form_error(
            username, email, password, min_password_len=_MIN_PASSWORD_LEN
        )
        if err:
            return render_template("dev_register.html", error=err, registration_allowed=True)
        try:
            save_dev_admin_user(username, email, password)
        except sqlite3.IntegrityError:
            return render_template(
                "dev_register.html",
                error="That username or email is already registered for dev access.",
                registration_allowed=True,
            )
        return redirect(url_for("dev.admin_login", registered=1))
    return render_template("dev_register.html", registration_allowed=True)


@dev_bp.post("/logout")
def admin_logout():
    session.pop("admin_user_id", None)
    session.pop("admin_username", None)
    for k in (
        "admin_mfa_ok",
        "admin_mfa_pending_user_id",
        "admin_mfa_pending_login",
        "admin_mfa_next",
        "admin_mfa_pending_method",
        "dev_mfa_test_last_code",
        "dev_mfa_totp_setup_secret",
        "dev_mfa_totp_setup_otpauth",
    ):
        session.pop(k, None)
    return redirect(url_for("dev.admin_login"))


@dev_bp.route("/mfa/verify", methods=["GET", "POST"])
@dev_bp.route("/mfa/setup", methods=["GET", "POST"])
@dev_bp.route("/mfa/qr", methods=["GET"])
def dev_mfa_gone() -> Any:
    """2FA for /dev was removed. Old bookmarks, tabs, and session redirects go here; send them to the dashboard or login."""
    if _admin_session_ok():
        return redirect(url_for("dev.dev_dashboard"), code=302)
    return redirect(url_for("dev.admin_login", next="/dev/"), code=302)


@dev_bp.route("/")
def dev_dashboard():
    return render_template(
        "dev.html",
        dealerships=list_recent_dealerships(10),
        dealership_stats=get_dealership_issue_stats(limit=10),
        status=_dev_status_shell(),
        admin_username=session.get("admin_username") or "",
        incomplete_cars=[],
        incomplete_count=get_incomplete_listings_count(),
        incomplete_issues_summary=[],
    )


@dev_bp.route("/api/status")
def api_dev_status():
    return jsonify({"ok": True, **_dev_status()})


@dev_bp.route("/api/dealers")
def api_dev_dealers():
    return jsonify({"ok": True, "dealerships": list_recent_dealerships(10)})


@dev_bp.route("/api/dealership-stats")
def api_dev_dealership_stats():
    return jsonify({"ok": True, "stats": get_dealership_issue_stats(limit=10)})


@dev_bp.route("/api/incomplete-cars")
def api_incomplete_cars():
    from backend.dealer.admin.incomplete_listings_api import incomplete_cars_response

    return incomplete_cars_response()


@dev_bp.route("/api/incomplete-export", methods=["POST"])
@dev_bp.route("/api/incomplete-cars/log-issue", methods=["POST"])
def api_incomplete_cars_log_issue():
    from backend.dealer.admin.incomplete_listings_api import export_incomplete_issue

    payload = request.get_json(silent=True) or {}
    issue = str(payload.get("issue") or "").strip()
    return export_incomplete_issue(issue)


@dev_bp.route("/api/incomplete-cars/<int:car_id>", methods=["DELETE"])
def api_delete_incomplete_car(car_id: int):
    from backend.dealer.admin.incomplete_listings_api import delete_incomplete_car

    return delete_incomplete_car(car_id)


@dev_bp.route("/api/audit-last-scrape")
def api_audit_last_scrape():
    """Latest DB rows plus optional raw JSON samples from the last scanner run.

    Requires an authenticated ``/dev`` admin session (``before_request``); not public.
    """
    conn = get_conn()
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute(
        """
        SELECT c.*,
               d.id AS dealership_table_id,
               d.is_active AS registry_is_active
        FROM cars c
        LEFT JOIN dealerships d ON d.id = c.dealership_registry_id
        ORDER BY c.id DESC
        LIMIT 5
        """
    )
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()

    samples_meta: dict[str, Any] = {}
    samples_list: list[dict[str, Any]] = []
    if LAST_SCRAPE_SAMPLES_PATH.is_file():
        try:
            samples_meta = json.loads(LAST_SCRAPE_SAMPLES_PATH.read_text(encoding="utf-8"))
            samples_list = samples_meta.get("samples") or []
        except (OSError, json.JSONDecodeError):
            samples_meta = {}
            samples_list = []

    diagnostics: list[dict[str, Any]] = []
    for row in rows:
        vin = row.get("vin")
        match = next((s for s in samples_list if s.get("vin") == vin), None)
        diagnostics.append(
            {
                "vin": vin,
                "parsed_database_row": redact_sensitive_car_row(row),
                "raw_json_sample": match.get("raw_json_sample") if match else None,
                "parsed_snapshot_from_scanner": match.get("parsed_snapshot") if match else None,
            }
        )

    return jsonify(
        {
            "ok": True,
            "diagnostics": diagnostics,
            "last_scrape_samples_file": str(LAST_SCRAPE_SAMPLES_PATH),
            "last_scrape_samples_generated_at": samples_meta.get("generated_at"),
            "note": "debug/last_scrape_samples.json was written by the retired scanner.js; the "
            "Python scanner does not write it, so raw samples appear only if an old file exists. "
            "cars has no is_active; registry_is_active is from dealerships.",
        }
    )


@dev_bp.route("/api/insert-dealer", methods=["POST"])
def api_insert_dealer():
    try:
        body = DealerCreate.model_validate(_json_body_no_token(request.get_json()))
    except ValidationError as e:
        return jsonify({"ok": False, "errors": e.errors()}), 422
    new_id = insert_dealership(body.row_dict())
    return jsonify({"ok": True, "id": new_id})


@dev_bp.route("/api/dealer/<int:dealer_id>", methods=["DELETE"])
def api_delete_dealer(dealer_id: int):
    if delete_dealership(dealer_id):
        return jsonify({"ok": True})
    return jsonify({"ok": False, "error": "not found"}), 404


@dev_bp.route("/api/test-scanner", methods=["POST"])
def api_test_scanner():
    data = _json_body_no_token(request.get_json())
    url, url_err = _dev_scanner_url_or_error(data.get("url") or "")
    headed = bool(data.get("headed"))
    if url_err:
        return jsonify({"ok": False, "error": url_err}), 400
    assert url

    job_id = uuid.uuid4().hex
    _dev_store_put(
        "jobs",
        job_id,
        {
            "log": "",
            "discovery": [],
            "done": False,
            "exit_code": None,
            "insert_id": None,
            "insert_error": None,
            "smart_error": None,
        },
    )

    thread = threading.Thread(
        target=_run_scanner_job,
        args=(job_id, url, headed),
        daemon=True,
    )
    thread.start()
    return jsonify({"ok": True, "job_id": job_id})


@dev_bp.route("/api/smart-import", methods=["POST"])
def api_smart_import():
    data = _json_body_no_token(request.get_json())
    url, url_err = _dev_scanner_url_or_error(data.get("url") or "")
    headed = bool(data.get("headed"))
    if url_err:
        return jsonify({"ok": False, "error": url_err}), 400
    assert url

    job_id = uuid.uuid4().hex
    _dev_store_put(
        "jobs",
        job_id,
        {
            "log": "",
            "discovery": [],
            "done": False,
            "exit_code": None,
            "insert_id": None,
            "insert_error": None,
            "smart_error": None,
            "cars_linked": None,
            "cancel_requested": False,
        },
    )

    threading.Thread(
        target=_run_smart_import_job,
        args=(job_id, url, headed),
        daemon=True,
    ).start()
    return jsonify({"ok": True, "job_id": job_id})


@dev_bp.route("/api/scanner-job/<job_id>")
def api_scanner_job(job_id: str):
    job = _dev_store_get("jobs", job_id)
    if not job:
        return jsonify(
            {
                "ok": False,
                "error": "job_expired",
                "message": "Server restarted — refresh /dev and start the job again.",
            }
        )
    return jsonify(
        {
            "ok": True,
            "log": job.get("log", ""),
            "discovery": job.get("discovery", []),
            "done": job.get("done", False),
            "exit_code": job.get("exit_code"),
            "insert_id": job.get("insert_id"),
            "insert_error": job.get("insert_error"),
            "smart_error": job.get("smart_error"),
            "cars_linked": job.get("cars_linked"),
            "dealer_id": job.get("dealer_id"),
            "verdict": job.get("verdict"),
            "rows": job.get("rows"),
        }
    )


def _run_bulk_import_queue(queue_id: str) -> None:
    q = _dev_store_get("queues", queue_id)
    if not q:
        return
    headed = bool(q.get("headed"))
    try:
        for it in q["items"]:
            job_id = it["job_id"]
            # An admin may mark a still-pending item "skipped" (see
            # api_import_queue_skip_item) while an earlier item in the queue
            # is stuck running — re-read the item's current status right
            # before starting so that item never blocks on a job nobody
            # wants run anymore (a worker other than the one running this
            # background thread may have recorded the skip).
            current = _dev_store_get("queues", queue_id) or q
            current_it = next(
                (x for x in current.get("items", []) if x.get("job_id") == job_id), it
            )
            if current_it.get("status") == "skipped":
                continue
            _dev_queue_patch_item(queue_id, job_id, status="processing")
            _run_smart_import_job(job_id, it["url"], headed)
            job = _dev_store_get("jobs", job_id) or {}
            final_status = "skipped" if job.get("cancel_requested") else "completed"
            _dev_queue_patch_item(queue_id, job_id, status=final_status)
    finally:
        _dev_store_patch("queues", queue_id, {"done": True})


@dev_bp.route("/api/smart-import-bulk", methods=["POST"])
def api_smart_import_bulk():
    data = _json_body_no_token(request.get_json())
    urls = data.get("urls")
    if isinstance(urls, str):
        urls = [u.strip() for u in urls.splitlines() if u.strip()]
    if not urls or not isinstance(urls, list):
        return jsonify({"ok": False, "error": "urls required (array or newline-separated string)"}), 400

    queue_id = uuid.uuid4().hex
    items: list[dict[str, Any]] = []
    for u in urls:
        normalized, url_err = _dev_scanner_url_or_error(u or "")
        if url_err:
            return jsonify({"ok": False, "error": url_err, "url": (u or "").strip()[:200]}), 400
        if not normalized:
            continue
        u = normalized
        jid = uuid.uuid4().hex
        items.append({"job_id": jid, "url": u, "status": "pending"})
        _dev_store_put(
            "jobs",
            jid,
            {
                "log": "",
                "discovery": [],
                "done": False,
                "exit_code": None,
                "insert_id": None,
                "insert_error": None,
                "smart_error": None,
                "cars_linked": None,
                "queue_id": queue_id,
                "cancel_requested": False,
            },
        )
    if not items:
        return jsonify({"ok": False, "error": "no valid urls"}), 400

    headed = bool(data.get("headed"))
    _dev_store_put("queues", queue_id, {"items": items, "done": False, "headed": headed})
    threading.Thread(target=_run_bulk_import_queue, args=(queue_id,), daemon=True).start()
    return jsonify({"ok": True, "queue_id": queue_id, "items": items})


@dev_bp.route("/api/import-queue/<queue_id>")
def api_import_queue(queue_id: str):
    q = _dev_store_get("queues", queue_id)
    if not q:
        return jsonify(
            {
                "ok": False,
                "error": "queue_expired",
                "message": "Server restarted — refresh /dev and start the job again.",
                "items": [],
                "queue_done": True,
            }
        )
    out: list[dict[str, Any]] = []
    for it in q["items"]:
        jid = it["job_id"]
        job = _dev_store_get("jobs", jid) or {}
        st = it.get("status", "pending")
        out.append(
            {
                "job_id": jid,
                "url": it["url"],
                "queue_status": st,
                "done": job.get("done", False),
                "insert_id": job.get("insert_id"),
                "insert_error": job.get("insert_error"),
                "smart_error": job.get("smart_error"),
                "cars_linked": job.get("cars_linked"),
                "dealer_id": job.get("dealer_id"),
                "verdict": job.get("verdict"),
                "rows": job.get("rows"),
            }
        )
    return jsonify({"ok": True, "queue_done": q.get("done"), "items": out})


@dev_bp.route("/api/import-queue/<queue_id>/skip-item", methods=["POST"])
def api_import_queue_skip_item(queue_id: str):
    """Skip one queued item so a stuck job doesn't block everything behind it.

    A ``pending`` item is marked ``skipped`` and ``_run_bulk_import_queue``
    passes over it without ever starting it. A ``processing`` item (the
    currently stuck one) instead has its underlying job's
    ``cancel_requested`` flag set — ``_run_smart_import_job``'s watchdog
    terminates that job's subprocess so the queue moves on to the next item.
    """
    data = _json_body_no_token(request.get_json())
    job_id = (data.get("job_id") or "").strip()
    if not job_id:
        return jsonify({"ok": False, "error": "job_id required"}), 400
    q = _dev_store_get("queues", queue_id)
    if not q:
        return jsonify({"ok": False, "error": "queue_expired"}), 404
    it = next((x for x in q.get("items", []) if x.get("job_id") == job_id), None)
    if not it:
        return jsonify({"ok": False, "error": "item_not_found"}), 404
    status = it.get("status", "pending")
    if status == "pending":
        _dev_queue_patch_item(queue_id, job_id, status="skipped")
        status = "skipped"
    elif status == "processing":
        _dev_store_patch("jobs", job_id, {"cancel_requested": True})
    return jsonify({"ok": True, "status": status})


@dev_bp.route("/api/geocode-missing", methods=["POST"])
def api_geocode_missing():
    result = geocode_missing_dealerships()
    return jsonify({"ok": True, **result})


@dev_bp.route("/api/deduplicate", methods=["POST"])
def api_deduplicate():
    result = deduplicate_dealerships()
    return jsonify({"ok": True, **result})


def _spawn_inventory_enrich(
    *,
    limit: int | None,
    vision_only: bool,
    max_workers: int | None = None,
) -> None:
    from backend.enrichment.service import InventoryEnricher

    try:
        enricher = InventoryEnricher()
        kwargs: dict[str, Any] = {"limit": limit, "vision_only": vision_only}
        if max_workers is not None:
            kwargs["max_workers"] = max_workers
        enricher.run_all(**kwargs)
    except Exception:
        import logging

        logging.getLogger("dev_routes").exception("Inventory enrichment background job failed")


@dev_bp.route("/api/cars/<int:car_id>/spec-backfill", methods=["POST"])
def api_car_spec_backfill(car_id: int):
    """
    Run EPA/VDP/search spec backfill for one vehicle (dev session + CSRF header).

    JSON body (optional): ``{"dry_run": true, "use_vdp": true, "use_search": true, "search_pause_s": 1.2}``
    """
    from backend.enrichment.spec_backfill import run_spec_backfill_for_car

    data = request.get_json(silent=True) or {}
    dry = bool(data.get("dry_run"))
    use_vdp = bool(data.get("use_vdp", True))
    use_search = bool(data.get("use_search", True))
    pause = data.get("search_pause_s", 1.2)
    try:
        pause_f = float(pause)
    except (TypeError, ValueError):
        pause_f = 1.2
    r = run_spec_backfill_for_car(
        car_id,
        use_vdp=use_vdp,
        use_search=use_search,
        dry_run=dry,
        search_pause_s=max(0.0, pause_f),
    )
    code = 404 if r.message == "not_found" else 200
    return (
        jsonify(
            {
                "ok": r.ok,
                "car_id": r.car_id,
                "updated_fields": r.updated_fields,
                "tiers": r.tiers,
                "message": r.message,
            }
        ),
        code,
    )


@dev_bp.route("/api/dev/enrich_all", methods=["POST"])
def api_dev_enrich_all():
    """
    Kick off inventory enrichment (EPA Master Catalog + optional Ollama vision) in a
    background thread. Full URL: ``POST /dev/api/dev/enrich_all``.

    JSON body (optional): ``{"limit": 10, "vision_only": false, "workers": 4}``
    """
    data = request.get_json(silent=True) or {}
    limit = data.get("limit")
    if limit is not None:
        try:
            limit = int(limit)
            if limit < 1:
                limit = None
        except (TypeError, ValueError):
            limit = None
    vision_only = bool(data.get("vision_only"))
    max_workers = data.get("workers")
    if max_workers is not None:
        try:
            max_workers = int(max_workers)
            if max_workers < 1:
                max_workers = None
        except (TypeError, ValueError):
            max_workers = None
    threading.Thread(
        target=_spawn_inventory_enrich,
        kwargs={"limit": limit, "vision_only": vision_only, "max_workers": max_workers},
        name="inventory-enrich",
        daemon=True,
    ).start()
    return jsonify(
        {
            "ok": True,
            "started": True,
            "limit": limit,
            "vision_only": vision_only,
            "workers": max_workers,
            "message": "Enrichment started in background; check server logs for progress.",
        }
    ), 202


@dev_bp.route("/api/car-debug", methods=["GET"])
def api_car_debug():
    """
    Dev-only: compare SQLite row vs serializer output for one vehicle.
    Query: ?vin=... or ?car_id=... (admin session required via /dev).
    """
    vin_q = (request.args.get("vin") or "").strip()
    car_id_raw = request.args.get("car_id") or request.args.get("id")
    raw: dict[str, Any] | None = None
    if vin_q:
        raw = get_car_by_vin(vin_q) or get_car_by_vin(vin_q.upper())
    elif car_id_raw:
        try:
            raw = get_car_by_id(int(car_id_raw))
        except (TypeError, ValueError):
            raw = None
    if not raw:
        return jsonify({"ok": False, "error": "not_found"}), 404
    ctx = prepare_car_detail_context(raw)
    vs = ctx.get("verified_specs") or {}
    detail_payload = serialize_car_for_api(raw, include_verified=False, verified_specs=vs)
    listing_payload = serialize_car_for_api(raw, include_verified=False)
    display_snapshot = build_detail_display_snapshot(vs, detail_payload)
    payload = {
        "ok": True,
        "vin": raw.get("vin"),
        "car_id": raw.get("id"),
        "note": "in-memory pre-upsert is not stored; use SCANNER_TRACE_VIN during scan logs, or compare raw_db_row here after upsert.",
        "raw_db_row": redact_sensitive_car_row(raw),
        "redacted_row_keys": sorted(SENSITIVE_CAR_ROW_KEYS & set(raw.keys())),
        "verified_specs": vs,
        "serialized_car_detail": detail_payload,
        "serialized_listing_style": listing_payload,
        "display_values_car_detail_template": display_snapshot,
    }
    if (raw.get("make") or "").strip().upper() == "BMW":
        payload["bmw_trace_env"] = (
            "Optional: set BMW_TRACE_VINS=VIN[,VIN2] for INFO logs from "
            "analytics_ep merge + serialize_car_for_api (condition/interior merge trace)."
        )
    return jsonify(payload)


from backend.dev.scan_lab_routes import register_scan_lab_routes

register_scan_lab_routes(dev_bp)
