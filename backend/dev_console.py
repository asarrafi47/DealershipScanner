"""Developer-only HTTP UI for backend maintenance (dealers manifest, etc.)."""
from __future__ import annotations

import json
import logging
import os
import posixpath
import subprocess
import sys
import threading
from functools import wraps
from pathlib import Path
from urllib.parse import urlparse

from flask import (
    Blueprint,
    jsonify,
    redirect,
    render_template,
    request,
    session,
    url_for,
)

from backend.dev_dealers import DEALERS_PATH, load_dealers, save_dealers, validate_dealers, PROVIDERS

bp = Blueprint("dev_console", __name__)
logger = logging.getLogger(__name__)


@bp.before_request
def _dev_console_csrf() -> None:
    from backend.utils.csrf import validate_csrf_form, validate_csrf_header

    ep = request.endpoint or ""
    if request.method not in ("POST", "PUT", "PATCH", "DELETE"):
        return
    if ep in ("dev_console.dev_login", "dev_console.dev_logout"):
        validate_csrf_form()
    else:
        validate_csrf_header()


_PROJECT_ROOT = Path(__file__).resolve().parent.parent
_SCANNER_SCRIPT = _PROJECT_ROOT / "scanner.py"


def dev_console_enabled() -> bool:
    v = (os.environ.get("DEV_CONSOLE") or "").strip().lower()
    return v in ("1", "true", "yes", "on")


def _secret_configured() -> bool:
    return bool((os.environ.get("DEV_CONSOLE_SECRET") or "").strip())


def _session_ok() -> bool:
    return bool(session.get("dev_console_ok"))


_DISABLED_API = (
    "Developer console is disabled for this server process. "
    "Set environment variable DEV_CONSOLE=1 (e.g. export DEV_CONSOLE=1) and restart."
)


def _safe_manifest_next_url(next_url: str, *, default: str) -> str:
    """Post-login redirect: same-origin /dev/manifest paths only (SEC-021 style)."""
    raw = (next_url or "").strip()
    if not raw.startswith("/") or raw.startswith("//") or "\\" in raw:
        return default
    path = posixpath.normpath(urlparse(raw).path or "/")
    if path != "/dev/manifest" and not path.startswith("/dev/manifest/"):
        return default
    return raw


def require_dev_access(*, api: bool = False):
    def decorator(view):
        @wraps(view)
        def wrapped(*args, **kwargs):
            if not dev_console_enabled():
                if api:
                    return jsonify({"ok": False, "error": _DISABLED_API}), 404
                return render_template("dev_disabled.html"), 200
            if _secret_configured() and not _session_ok():
                if api:
                    return jsonify({"ok": False, "error": "Unauthorized"}), 401
                return redirect(url_for("dev_console.dev_login", next=request.path))
            return view(*args, **kwargs)

        return wrapped

    return decorator


@bp.route("/dev/manifest/login", methods=["GET", "POST"])
def dev_login():
    if not dev_console_enabled():
        return render_template("dev_disabled.html"), 200
    if not _secret_configured():
        return redirect(url_for("dev_console.dev_home"))
    if _session_ok():
        return redirect(url_for("dev_console.dev_home"))
    error = None
    if request.method == "POST":
        provided = (request.form.get("secret") or "").strip()
        expected = (os.environ.get("DEV_CONSOLE_SECRET") or "").strip()
        if provided and provided == expected:
            session["dev_console_ok"] = True
            raw_next = request.args.get("next") or url_for("dev_console.dev_home")
            nxt = _safe_manifest_next_url(raw_next, default=url_for("dev_console.dev_home"))
            return redirect(nxt)
        error = "Invalid secret."
    return render_template("dev_login.html", error=error)


@bp.post("/dev/manifest/logout")
def dev_logout():
    if not dev_console_enabled():
        return render_template("dev_disabled.html"), 200
    session.pop("dev_console_ok", None)
    return redirect(url_for("dev_console.dev_login") if _secret_configured() else "/")


@bp.route("/dev/manifest")
@require_dev_access()
def dev_home():
    return render_template(
        "dev_manifest.html",
        providers=sorted(PROVIDERS),
        secret_gate=_secret_configured(),
    )


@bp.get("/api/dev/dealers")
@require_dev_access(api=True)
def api_dev_dealers_get():
    try:
        dealers = load_dealers()
    except (OSError, ValueError) as e:
        return jsonify({"ok": False, "error": str(e)}), 400
    return jsonify({"ok": True, "dealers": dealers})


@bp.post("/api/dev/infer-dealer")
@require_dev_access(api=True)
def api_dev_infer_dealer():
    data = request.get_json(silent=True) or {}
    raw_url = (data.get("url") or data.get("site_url") or "").strip()
    if not raw_url:
        return jsonify({"ok": False, "error": "Missing url"}), 400
    pasted = data.get("html")
    pasted_str = pasted.strip() if isinstance(pasted, str) else None
    try:
        timeout = float(os.environ.get("DEV_INFER_TIMEOUT", "20"))
    except ValueError:
        timeout = 20.0
    timeout = max(5.0, min(timeout, 60.0))
    try:
        from backend.dealer_url_infer import infer_dealer_from_url
    except ImportError as e:
        return jsonify(
            {
                "ok": False,
                "error": (
                    "Look-up dependencies missing (install beautifulsoup4 and requests). "
                    f"Details: {e}"
                ),
            }
        ), 503
    result = infer_dealer_from_url(raw_url, html=pasted_str, timeout=timeout)
    if not result.get("ok"):
        return jsonify({"ok": False, "error": result.get("error", "Inference failed")}), 400
    return jsonify(
        {
            "ok": True,
            "dealer": result["dealer"],
            "hints": result.get("hints") or [],
            "final_url": result.get("final_url"),
        }
    )


@bp.put("/api/dev/dealers")
@require_dev_access(api=True)
def api_dev_dealers_put():
    data = request.get_json(silent=True) or {}
    rows = data.get("dealers")
    if rows is None:
        return jsonify({"ok": False, "error": "Missing dealers array"}), 400
    try:
        validated = validate_dealers(rows)
        save_dealers(validated)
    except (OSError, ValueError) as e:
        return jsonify({"ok": False, "error": str(e)}), 400
    return jsonify({"ok": True, "dealers": validated})


def _run_scanner_subprocess(dealer_id: str) -> None:
    """Blocking: run inventory scanner for one manifest row (background thread)."""
    if not _SCANNER_SCRIPT.is_file():
        logger.error("scanner.py not found at %s", _SCANNER_SCRIPT)
        return
    logger.info("Scanner subprocess starting: dealer_id=%s", dealer_id)
    try:
        proc = subprocess.run(
            [sys.executable, str(_SCANNER_SCRIPT), "--dealer-id", dealer_id],
            cwd=str(_PROJECT_ROOT),
            env=os.environ.copy(),
        )
        logger.info(
            "Scanner subprocess finished: dealer_id=%s exit_code=%s",
            dealer_id,
            proc.returncode,
        )
    except Exception:
        logger.exception("Scanner subprocess failed for dealer_id=%s", dealer_id)


def _scan_dealer_error(
    code: str,
    message: str,
    *,
    dealer_id_received: str = "",
    status: int = 400,
) -> tuple:
    """Structured JSON for /api/dev/scan-dealer failures."""
    return (
        jsonify(
            {
                "ok": False,
                "code": code,
                "message": message,
                "dealer_id": dealer_id_received,
            }
        ),
        status,
    )


@bp.post("/api/dev/scan-dealer")
@require_dev_access(api=True)
def api_dev_scan_dealer():
    """Start ``python scanner.py --dealer-id …`` in a background thread (non-blocking HTTP)."""
    raw_body = request.get_data(cache=True) or b""

    if not raw_body.strip():
        return _scan_dealer_error(
            "missing_payload",
            "Request body is empty. Send JSON: {\"dealer_id\": \"<slug>\"} with Content-Type: application/json.",
            dealer_id_received="",
        )

    try:
        raw_text = raw_body.decode("utf-8")
    except UnicodeDecodeError:
        return _scan_dealer_error(
            "invalid_json_shape",
            "Request body must be UTF-8 text containing a JSON object.",
            dealer_id_received="",
        )

    try:
        parsed = json.loads(raw_text)
    except json.JSONDecodeError as e:
        return _scan_dealer_error(
            "invalid_json_shape",
            f"Request body is not valid JSON: {e}. Expected an object like {{\"dealer_id\": \"long-beach-bmw\"}}.",
            dealer_id_received="",
        )

    if not isinstance(parsed, dict):
        return _scan_dealer_error(
            "invalid_json_shape",
            "JSON body must be an object with a string field \"dealer_id\", e.g. "
            '{"dealer_id": "long-beach-bmw"}.',
            dealer_id_received="",
        )

    if "dealer_id" not in parsed:
        return _scan_dealer_error(
            "missing_dealer_id",
            'Missing required field "dealer_id". Send: {"dealer_id": "<slug-from-dealers-json>"}.',
            dealer_id_received="",
        )

    raw_id = parsed["dealer_id"]
    if not isinstance(raw_id, str):
        return _scan_dealer_error(
            "invalid_json_shape",
            '"dealer_id" must be a string (the dealer slug saved in dealers.json).',
            dealer_id_received=str(raw_id),
        )

    dealer_id_requested = raw_id.strip()
    dealer_id_key = dealer_id_requested.lower()
    if not dealer_id_key:
        return _scan_dealer_error(
            "missing_dealer_id",
            '"dealer_id" is empty after trimming. Enter the slug in the row, click Save changes, then Scan.',
            dealer_id_received="",
        )

    if not DEALERS_PATH.is_file():
        return _scan_dealer_error(
            "dealers_file_missing",
            f"dealers.json is missing at {DEALERS_PATH}. Click “Save changes” in the dev console to create it.",
            dealer_id_received=dealer_id_requested,
        )

    try:
        dealers = load_dealers()
    except (OSError, ValueError) as e:
        return _scan_dealer_error(
            "invalid_json_shape",
            f"dealers.json exists but could not be loaded: {e}",
            dealer_id_received=dealer_id_requested,
        )

    canonical_by_lower: dict[str, str] = {}
    for d in dealers:
        if not isinstance(d, dict):
            continue
        did = (d.get("dealer_id") or "").strip()
        if not did:
            continue
        canonical_by_lower.setdefault(did.lower(), did)

    if dealer_id_key not in canonical_by_lower:
        if not canonical_by_lower:
            return _scan_dealer_error(
                "dealer_id_not_found",
                "Dealer ID was not found in dealers.json (file has no dealer rows). "
                "Click “Save changes” to persist your table, then try Scan again.",
                dealer_id_received=dealer_id_requested,
            )
        return _scan_dealer_error(
            "dealer_id_not_found",
            "Dealer ID was not found in dealers.json. "
            "If you just added this row, click “Save changes” first. Otherwise check the slug matches the saved row.",
            dealer_id_received=dealer_id_requested,
        )

    dealer_id = canonical_by_lower[dealer_id_key]

    thread = threading.Thread(
        target=_run_scanner_subprocess,
        args=(dealer_id,),
        name=f"scanner-{dealer_id}",
        daemon=True,
    )
    thread.start()
    return (
        jsonify(
            {
                "ok": True,
                "code": "scan_accepted",
                "dealer_id": dealer_id,
                "message": (
                    f"Scan accepted for {dealer_id!r} (HTTP 202). "
                    "Running python scanner.py --dealer-id in the background; watch this server terminal for logs."
                ),
            }
        ),
        202,
    )


def register_dev_console(app) -> None:
    """Always attach routes so /dev can explain how to enable the console."""
    app.register_blueprint(bp)
