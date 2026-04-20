"""
Developer dashboard at /dev: session-based admin login (separate from public users).
Smart URL import, scanner jobs, dealership registry tools.
"""

from __future__ import annotations

import json
import logging
import os
import posixpath
import re
import shutil
import sqlite3
import subprocess
import sys
import threading
import uuid
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
from backend.dev_dealers import (
    normalize_manifest_url,
    smart_import_manifest_display_name,
    smart_import_scrape_succeeded,
    upsert_dealer_manifest_row,
)
from backend.db.inventory_db import (
    get_car_by_id,
    get_car_by_vin,
    get_conn,
    get_incomplete_cars,
    link_cars_to_dealership_registry,
)
from backend.knowledge_engine import prepare_car_detail_context
from backend.utils.car_serialize import build_detail_display_snapshot, serialize_car_for_api
from backend.utils.client_ip import client_ip
from backend.utils.ip_rate_limit import allow_request
from backend.utils.registration_validation import registration_form_error
from schemas.dealership import DealerCreate

PROJECT_ROOT = Path(__file__).resolve().parent.parent

dev_bp = Blueprint("dev", __name__)

scanner_jobs: dict[str, dict[str, Any]] = {}
scanner_lock = threading.Lock()

import_queues: dict[str, dict[str, Any]] = {}

SCANNER_PROFILES = ("default", "resilient", "bare")

LAST_SCRAPE_SAMPLES_PATH = PROJECT_ROOT / "debug" / "last_scrape_samples.json"

logger = logging.getLogger(__name__)

_MIN_PASSWORD_LEN = max(8, int(os.environ.get("MIN_PASSWORD_LENGTH", "8")))
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
    return bool(session.get("admin_user_id"))


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


@dev_bp.before_request
def _dev_require_admin() -> Any:
    from backend.utils.csrf import validate_csrf_form, validate_csrf_header

    ep = request.endpoint or ""
    if request.method in ("POST", "PUT", "PATCH", "DELETE"):
        if ep in ("dev.admin_login", "dev.admin_register", "dev.admin_logout"):
            validate_csrf_form()
        else:
            validate_csrf_header()

    if ep in ("dev.admin_login", "dev.admin_register", "dev.admin_logout"):
        return None
    if _admin_session_ok():
        return None
    if request.path.startswith("/dev/api"):
        return jsonify({"ok": False, "error": "unauthorized", "login_url": "/dev/login"}), 401
    return redirect(url_for("dev.admin_login", next=request.full_path))


def _json_body_no_token(data: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(data, dict):
        return {}
    return {k: v for k, v in data.items() if k != "token"}


def _dev_status() -> dict[str, Any]:
    from backend.utils.project_env import load_project_dotenv

    load_project_dotenv()

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
    return {
        "db_connected": db_ok,
        "inventory_db_path": str(DB_PATH),
        "dev_users_db_path": dev_users_db_path(),
        "dev_registration_open": dev_public_registration_allowed(),
        "node_executable": shutil.which("node"),
        "is_production": prod,
        "admin_password_configured": admin_pw_set or not prod,
        "dotenv_file_present": env_file.is_file(),
        "dotenv_file_path": str(env_file),
    }


def _run_scanner_job(job_id: str, url: str, headed: bool = False) -> None:
    log_parts: list[str] = []

    def append(text: str) -> None:
        log_parts.append(text)
        with scanner_lock:
            if job_id in scanner_jobs:
                scanner_jobs[job_id]["log"] = "".join(log_parts)

    code: int | None = None
    try:
        cmd = ["node", str(PROJECT_ROOT / "scanner.js"), "--url", url]
        if headed:
            cmd.append("--headed")
        proc = subprocess.Popen(
            cmd,
            cwd=str(PROJECT_ROOT),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
        if proc.stdout:
            for line in proc.stdout:
                append(line)
        code = proc.wait()
    except FileNotFoundError as e:
        append(f"\n[dev] Failed to start scanner: {e}\n")
        code = 127
    except OSError as e:
        append(f"\n[dev] Scanner error: {e}\n")
        code = -1

    with scanner_lock:
        if job_id in scanner_jobs:
            scanner_jobs[job_id]["done"] = True
            scanner_jobs[job_id]["exit_code"] = code

    if code == 0:
        _spawn_vector_reindex_background()


def _parse_prefixed_json(line: str, prefix: str) -> dict[str, Any] | None:
    s = line.strip()
    if not s.startswith(prefix):
        return None
    raw = s[len(prefix) :].strip()
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return None


def _run_smart_import_job(job_id: str, url: str, headed: bool = False) -> None:
    log_parts: list[str] = []
    discovery: list[dict[str, Any]] = []
    resolved_result: dict[str, Any] | None = None
    last_error: dict[str, Any] | None = None
    final_code: int | None = None
    error_partial: dict[str, Any] = {}

    def append(text: str) -> None:
        log_parts.append(text)
        with scanner_lock:
            if job_id in scanner_jobs:
                scanner_jobs[job_id]["log"] = "".join(log_parts)
                scanner_jobs[job_id]["discovery"] = list(discovery)

    for attempt, profile in enumerate(SCANNER_PROFILES):
        append(f"\n--- Scanner attempt {attempt + 1}/{len(SCANNER_PROFILES)} (profile={profile}) ---\n")
        attempt_result: dict[str, Any] | None = None
        attempt_error: dict[str, Any] | None = None
        try:
            cmd = [
                "node",
                str(PROJECT_ROOT / "scanner.js"),
                "--url",
                url,
                "--smart-import",
                "--profile",
                profile,
            ]
            if headed:
                cmd.append("--headed")
            proc = subprocess.Popen(
                cmd,
                cwd=str(PROJECT_ROOT),
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
            )
            if proc.stdout:
                for line in proc.stdout:
                    append(line)
                    if line.startswith("DISCOVERY:"):
                        payload = _parse_prefixed_json(line, "DISCOVERY:")
                        if isinstance(payload, dict):
                            discovery.append(payload)
                            with scanner_lock:
                                if job_id in scanner_jobs:
                                    scanner_jobs[job_id]["discovery"] = list(discovery)
                    if "SMART_IMPORT_RESULT:" in line:
                        idx = line.index("SMART_IMPORT_RESULT:")
                        raw = line[idx + len("SMART_IMPORT_RESULT:") :].strip()
                        try:
                            attempt_result = json.loads(raw)
                        except json.JSONDecodeError:
                            pass
                    if "SMART_IMPORT_ERROR:" in line:
                        idx = line.index("SMART_IMPORT_ERROR:")
                        raw = line[idx + len("SMART_IMPORT_ERROR:") :].strip()
                        try:
                            attempt_error = json.loads(raw)
                            par = attempt_error.get("partial") if isinstance(attempt_error, dict) else None
                            if isinstance(par, dict):
                                for pk, pv in par.items():
                                    if pv not in (None, "", [], {}):
                                        error_partial[str(pk)] = pv
                        except json.JSONDecodeError:
                            attempt_error = {"reason": "parse_error", "raw": raw[:200]}

            final_code = proc.wait()
        except FileNotFoundError as e:
            append(f"\n[dev] Failed to start scanner: {e}\n")
            final_code = 127
            last_error = {"reason": "node_missing", "retryable": False}
            break
        except OSError as e:
            append(f"\n[dev] Scanner error: {e}\n")
            final_code = -1
            last_error = {"reason": "os_error", "detail": str(e), "retryable": False}
            break

        with scanner_lock:
            if job_id in scanner_jobs:
                scanner_jobs[job_id]["exit_code"] = final_code

        last_error = attempt_error or last_error

        if attempt_result:
            resolved_result = attempt_result
            break
        if attempt_error and not attempt_error.get("retryable"):
            last_error = attempt_error
            break
        if attempt < len(SCANNER_PROFILES) - 1:
            append("\n[dev] Retrying with a different Puppeteer profile…\n")

    insert_error: list | None = None
    insert_id: int | None = None
    cars_linked = 0
    manifest_written = False
    full_log = "".join(log_parts)

    if resolved_result:
        try:
            body = DealerCreate.model_validate(resolved_result)
            rd = body.row_dict()
            try:
                action, manifest_id = upsert_dealer_manifest_row(
                    name=str(rd.get("name") or ""),
                    website_url=str(rd.get("website_url") or ""),
                    provider="dealer_dot_com",
                )
                append(
                    f"[dev] dealers.json {action}: dealer_id={manifest_id} "
                    f"(python scanner.py --dealer-id)\n"
                )
                manifest_written = True
            except ValueError as e:
                append(f"[dev] dealers.json upsert skipped (registry path): {e}\n")
            insert_id = insert_dealership(rd)
            cars_linked = link_cars_to_dealership_registry(insert_id, str(body.website_url))
        except ValidationError as e:
            insert_error = e.errors()

    scrape_ok = smart_import_scrape_succeeded(final_code, full_log)
    if scrape_ok and not manifest_written:
        wurl = normalize_manifest_url(url)
        if not wurl:
            append("[dev] dealers.json skipped (manifest-only): could not normalize job URL\n")
        else:
            try:
                disp = smart_import_manifest_display_name(
                    url,
                    resolved=resolved_result,
                    error_partial=error_partial,
                    discovery=discovery,
                )
                action, manifest_id = upsert_dealer_manifest_row(
                    name=disp,
                    website_url=wurl,
                    provider="dealer_dot_com",
                )
                manifest_written = True
                append(
                    f"[dev] dealers.json {action} (manifest-only; registry row skipped until "
                    f"city/state are resolved): dealer_id={manifest_id}\n"
                )
            except ValueError as e:
                append(f"[dev] dealers.json manifest-only upsert skipped: {e}\n")
    elif final_code == 0 and not manifest_written:
        append(
            "[dev] dealers.json skipped: process exited 0 but log shows no persisted vehicles "
            "(expected SCAN_VEHICLE_COUNT > 0 or Upserted N > 0).\n"
        )

    with scanner_lock:
        if job_id not in scanner_jobs:
            return
        scanner_jobs[job_id]["done"] = True
        scanner_jobs[job_id]["exit_code"] = final_code
        scanner_jobs[job_id]["discovery"] = list(discovery)
        scanner_jobs[job_id]["insert_id"] = insert_id
        scanner_jobs[job_id]["insert_error"] = insert_error
        scanner_jobs[job_id]["smart_error"] = last_error if not resolved_result else None
        scanner_jobs[job_id]["cars_linked"] = cars_linked

    if final_code == 0:
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
            session["admin_user_id"] = uid
            session["admin_username"] = uname
            raw_next = (request.form.get("next") or request.args.get("next") or "").strip()
            next_url = _safe_dev_next_url(raw_next)
            return redirect(next_url)
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
    return redirect(url_for("dev.admin_login"))


@dev_bp.route("/")
def dev_dashboard():
    return render_template(
        "dev.html",
        dealerships=list_recent_dealerships(10),
        status=_dev_status(),
        admin_username=session.get("admin_username") or "",
        incomplete_cars=get_incomplete_cars(),
    )


@dev_bp.route("/api/status")
def api_dev_status():
    return jsonify({"ok": True, **_dev_status()})


@dev_bp.route("/api/dealers")
def api_dev_dealers():
    return jsonify({"ok": True, "dealerships": list_recent_dealerships(10)})


@dev_bp.route("/api/incomplete-cars")
def api_incomplete_cars():
    from backend.utils.car_serialize import serialize_car_for_api

    cars = get_incomplete_cars()
    safe = [serialize_car_for_api(c, include_verified=False) for c in cars]
    return jsonify({"ok": True, "cars": safe, "count": len(safe)})


@dev_bp.route("/api/incomplete-cars/<int:car_id>", methods=["DELETE"])
def api_delete_incomplete_car(car_id: int):
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM cars WHERE id = ?", (car_id,))
    deleted = cursor.rowcount
    conn.commit()
    conn.close()
    if deleted:
        return jsonify({"ok": True})
    return jsonify({"ok": False, "error": "not found"}), 404


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
                "parsed_database_row": row,
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
            "note": "Run node scanner.js to refresh debug/last_scrape_samples.json. "
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
    url = (data.get("url") or "").strip()
    headed = bool(data.get("headed"))
    if not url:
        return jsonify({"ok": False, "error": "url is required"}), 400

    job_id = uuid.uuid4().hex
    with scanner_lock:
        scanner_jobs[job_id] = {
            "log": "",
            "discovery": [],
            "done": False,
            "exit_code": None,
            "insert_id": None,
            "insert_error": None,
            "smart_error": None,
        }

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
    url = (data.get("url") or "").strip()
    headed = bool(data.get("headed"))
    if not url:
        return jsonify({"ok": False, "error": "url is required"}), 400

    job_id = uuid.uuid4().hex
    with scanner_lock:
        scanner_jobs[job_id] = {
            "log": "",
            "discovery": [],
            "done": False,
            "exit_code": None,
            "insert_id": None,
            "insert_error": None,
            "smart_error": None,
            "cars_linked": None,
        }

    threading.Thread(
        target=_run_smart_import_job,
        args=(job_id, url, headed),
        daemon=True,
    ).start()
    return jsonify({"ok": True, "job_id": job_id})


@dev_bp.route("/api/scanner-job/<job_id>")
def api_scanner_job(job_id: str):
    with scanner_lock:
        job = scanner_jobs.get(job_id)
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
        }
    )


def _run_bulk_import_queue(queue_id: str) -> None:
    q = import_queues.get(queue_id)
    if not q:
        return
    headed = bool(q.get("headed"))
    try:
        for it in q["items"]:
            it["status"] = "processing"
            _run_smart_import_job(it["job_id"], it["url"], headed)
            it["status"] = "completed"
    finally:
        q["done"] = True


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
        u = (u or "").strip()
        if not u:
            continue
        jid = uuid.uuid4().hex
        items.append({"job_id": jid, "url": u, "status": "pending"})
        with scanner_lock:
            scanner_jobs[jid] = {
                "log": "",
                "discovery": [],
                "done": False,
                "exit_code": None,
                "insert_id": None,
                "insert_error": None,
                "smart_error": None,
                "cars_linked": None,
                "queue_id": queue_id,
            }
    if not items:
        return jsonify({"ok": False, "error": "no valid urls"}), 400

    headed = bool(data.get("headed"))
    import_queues[queue_id] = {"items": items, "done": False, "headed": headed}
    threading.Thread(target=_run_bulk_import_queue, args=(queue_id,), daemon=True).start()
    return jsonify({"ok": True, "queue_id": queue_id, "items": items})


@dev_bp.route("/api/import-queue/<queue_id>")
def api_import_queue(queue_id: str):
    q = import_queues.get(queue_id)
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
        with scanner_lock:
            job = scanner_jobs.get(jid, {})
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
            }
        )
    return jsonify({"ok": True, "queue_done": q.get("done"), "items": out})


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
    from backend.enrichment_service import InventoryEnricher

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
    from backend.spec_backfill import run_spec_backfill_for_car

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
        "raw_db_row": {k: raw[k] for k in sorted(raw.keys())},
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
