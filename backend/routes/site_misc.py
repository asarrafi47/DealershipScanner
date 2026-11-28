"""Infra routes: health check, favicon, locally-stored car images, and the draft legal pages."""

from __future__ import annotations

import re
from pathlib import Path

from flask import abort, current_app, jsonify, render_template, send_from_directory

# Project root: VERSION lives here, and so does BUILD_COMMIT in a deployed image.
_ROOT_DIR = Path(__file__).resolve().parent.parent.parent

# Serve locally-downloaded car images (written by image_downloader.py).
# Stored under <project_root>/car_images/<dealer_id>/<vin>/<file>.
_CAR_IMAGES_DIR = _ROOT_DIR / "car_images"

# A git object name: SHA-1 (40) or SHA-256 (64) hex, abbreviated forms allowed.
_COMMIT_RE = re.compile(r"[0-9a-f]{7,64}")


def _read_root_file(name: str) -> str:
    """Stripped text of a small file at the project root; "" when unreadable."""
    try:
        return (_ROOT_DIR / name).read_text(encoding="utf-8").strip()
    except (OSError, ValueError):
        return ""


def _app_version() -> str:
    return _read_root_file("VERSION") or "dev"


def _build_commit() -> str:
    """Full SHA of the deployed commit, or ``"unknown"``.

    deploy/railway/deploy_web.sh and deploy_scanner_nightly.sh (remediation
    P2B.2) write BUILD_COMMIT next to VERSION in the stage they upload. A dev
    checkout has no such file, and a malformed one is not echoed back.
    """
    commit = _read_root_file("BUILD_COMMIT")
    return commit if _COMMIT_RE.fullmatch(commit) else "unknown"


def health():
    # Railway's healthcheck (web's healthcheckPath setting, deploy/railway/service_settings.json) only needs the 200.
    return jsonify({"status": "ok", "version": _app_version(), "commit": _build_commit()}), 200


def favicon():
    # A real .ico: some crawlers and older Safari ignore SVG served at /favicon.ico.
    return send_from_directory(
        current_app.static_folder, "brand/favicon.ico", mimetype="image/x-icon"
    )


def serve_car_image(filename: str):
    # Block path traversal: reject any component that starts with '.' or contains separators
    parts = Path(filename).parts
    if not parts or any(p.startswith(".") or p in ("/", "\\") for p in parts):
        abort(400)
    safe_path = _CAR_IMAGES_DIR.joinpath(*parts).resolve()
    if not safe_path.is_relative_to(_CAR_IMAGES_DIR.resolve()):
        abort(400)
    return send_from_directory(str(_CAR_IMAGES_DIR), str(Path(*parts)))


def _legal_page(template: str):
    # Public drafts (card S14): served whether or not phone sign-in is on; each shows a DRAFT sentence
    # until the owner approves the wording (backend/auth/phone/legal.py).
    from backend.auth.phone import legal

    return render_template(template, **legal.page_context())


def terms():
    return _legal_page("terms.html")


def privacy():
    return _legal_page("privacy.html")


def sms_consent():
    return _legal_page("sms_consent.html")


def register(app) -> None:
    """Attach routes to ``app`` keeping the original bare endpoint names."""
    app.add_url_rule("/health", view_func=health)
    app.add_url_rule("/favicon.ico", view_func=favicon)
    app.add_url_rule("/car-images/<path:filename>", view_func=serve_car_image)
    app.add_url_rule("/terms", view_func=terms)
    app.add_url_rule("/privacy", view_func=privacy)
    app.add_url_rule("/sms-consent", view_func=sms_consent)
    # The footer shows the draft legal links only while phone sign-in is on (card S14): with the flag
    # off every page's footer stays byte for byte what it was.
    from backend.auth.phone import config as phone_config

    app.jinja_env.globals.setdefault("phone_login_enabled", phone_config.enabled)
