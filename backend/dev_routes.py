"""
Developer dashboard at /dev: session-based admin login (separate from public users).
Smart URL import, scanner jobs, dealership registry tools.
"""

from __future__ import annotations

import json
import shutil
import sqlite3
import subprocess
import threading
import uuid
from pathlib import Path
from typing import Any

from flask import Blueprint, jsonify, redirect, render_template, request, session, url_for
from pydantic import ValidationError

from backend.db.admin_users_db import authenticate_admin
from backend.db.dealerships_db import (
    DB_PATH,
    deduplicate_dealerships,
    delete_dealership,
    geocode_missing_dealerships,
    insert_dealership,
    list_recent_dealerships,
)
from backend.db.inventory_db import get_conn, link_cars_to_dealership_registry
from schemas.dealership import DealerCreate

PROJECT_ROOT = Path(__file__).resolve().parent.parent

dev_bp = Blueprint("dev", __name__)

scanner_jobs: dict[str, dict[str, Any]] = {}
scanner_lock = threading.Lock()

import_queues: dict[str, dict[str, Any]] = {}

SCANNER_PROFILES = ("default", "resilient", "bare")

LAST_SCRAPE_SAMPLES_PATH = PROJECT_ROOT / "debug" / "last_scrape_samples.json"


def _chroma_reindex_background() -> None:
    try:
        from backend.vector.chroma_service import reindex_all

        reindex_all()
    except Exception:
        pass


def _spawn_chroma_reindex_background() -> None:
    """Refresh Chroma from SQLite after a successful scanner run (non-blocking)."""
    threading.Thread(target=_chroma_reindex_background, daemon=True).start()


def _admin_session_ok() -> bool:
    return bool(session.get("admin_user_id"))


@dev_bp.before_request
def _dev_require_admin() -> Any:
    if request.endpoint in ("dev.admin_login", "dev.admin_logout"):
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
    db_ok = False
    try:
        conn = get_conn()
        conn.cursor().execute("SELECT 1")
        conn.close()
        db_ok = True
    except (OSError, sqlite3.Error):
        pass
    return {
        "db_connected": db_ok,
        "inventory_db_path": str(DB_PATH),
        "node_executable": shutil.which("node"),
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
        _spawn_chroma_reindex_background()


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
    if resolved_result:
        try:
            body = DealerCreate.model_validate(resolved_result)
            insert_id = insert_dealership(body.row_dict())
            cars_linked = link_cars_to_dealership_registry(insert_id, str(body.website_url))
        except ValidationError as e:
            insert_error = e.errors()

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
        _spawn_chroma_reindex_background()


@dev_bp.route("/login", methods=["GET", "POST"])
def admin_login():
    if _admin_session_ok():
        return redirect(url_for("dev.dev_dashboard"))
    if request.method == "POST":
        login_input = (request.form.get("login") or "").strip()
        password = (request.form.get("password") or "").strip()
        auth = authenticate_admin(login_input, password)
        if auth:
            uid, uname = auth
            session["admin_user_id"] = uid
            session["admin_username"] = uname
            next_url = (request.form.get("next") or request.args.get("next") or "").strip()
            if not next_url.startswith("/dev"):
                next_url = url_for("dev.dev_dashboard")
            return redirect(next_url)
        return render_template(
            "admin_login.html",
            error="Invalid username/email or password.",
        )
    return render_template("admin_login.html")


@dev_bp.route("/logout")
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
    )


@dev_bp.route("/api/status")
def api_dev_status():
    return jsonify({"ok": True, **_dev_status()})


@dev_bp.route("/api/dealers")
def api_dev_dealers():
    return jsonify({"ok": True, "dealerships": list_recent_dealerships(10)})


@dev_bp.route("/api/audit-last-scrape")
def api_audit_last_scrape():
    """Latest DB rows plus optional raw JSON samples from the last scanner run (token-gated)."""
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
        return jsonify({"ok": False, "error": "unknown job"}), 404
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
        return jsonify({"ok": False, "error": "unknown queue"}), 404
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


def _spawn_inventory_enrich(*, limit: int | None, vision_only: bool) -> None:
    from backend.enrichment_service import InventoryEnricher

    try:
        enricher = InventoryEnricher()
        enricher.run_all(limit=limit, vision_only=vision_only)
    except Exception:
        import logging

        logging.getLogger("dev_routes").exception("Inventory enrichment background job failed")


@dev_bp.route("/api/dev/enrich_all", methods=["POST"])
def api_dev_enrich_all():
    """
    Kick off inventory enrichment (EPA Master Catalog + optional Ollama vision) in a
    background thread. Full URL: ``POST /dev/api/dev/enrich_all``.

    JSON body (optional): ``{"limit": 10, "vision_only": false}``
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
    threading.Thread(
        target=_spawn_inventory_enrich,
        kwargs={"limit": limit, "vision_only": vision_only},
        name="inventory-enrich",
        daemon=True,
    ).start()
    return jsonify(
        {
            "ok": True,
            "started": True,
            "limit": limit,
            "vision_only": vision_only,
            "message": "Enrichment started in background; check server logs for progress.",
        }
    ), 202
