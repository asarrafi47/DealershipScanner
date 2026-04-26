"""
Dealer-facing inventory management at ``/inventory`` (session: app ``users`` table).
"""
from __future__ import annotations

import logging
import os
import re
import secrets
import sqlite3
from pathlib import Path
from typing import Any

from flask import (
    Blueprint,
    abort,
    flash,
    redirect,
    render_template,
    request,
    send_from_directory,
    session,
    url_for,
)

from backend.db import dealer_portal_db as ddb
from backend.db.users_db import check_user, create_org, get_user_by_login, get_user_totp, save_user
from backend.utils.client_ip import client_ip as _client_ip
from backend.utils.dealer_vin_prefill import build_vehicle_prefill_from_vin
from backend.utils.ip_rate_limit import allow_request
from backend.utils.mfa_otp import clear_session_otp
from backend.utils.registration_validation import registration_form_error
from backend.utils.roles import (
    MFA_INTENT_DEALER,
    MFA_INTENT_GENERAL,
    ROLE_ADMIN,
    ROLE_DEALERSHIP_OWNER,
    email_is_admin,
    is_admin_role,
    normalize_role,
)

_log = logging.getLogger(__name__)

bp = Blueprint("dealer_portal", __name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
UPLOAD_ROOT = Path(os.environ.get("DEALER_UPLOAD_ROOT", str(PROJECT_ROOT / "uploads" / "dealer"))).resolve()

_ALLOWED_MT = frozenset({"image/jpeg", "image/png", "image/webp"})
_MT_EXT = {"image/jpeg": ".jpg", "image/png": ".png", "image/webp": ".webp"}
_MAX_UPLOAD_BYTES = int(os.environ.get("DEALER_UPLOAD_MAX_BYTES", str(8 * 1024 * 1024)))
_MAX_FILES_PER_REQUEST = int(os.environ.get("DEALER_UPLOAD_MAX_FILES", "8"))
_MAX_GALLERY_IMAGES = int(os.environ.get("DEALER_GALLERY_MAX_IMAGES", "24"))
_DEALER_VIN_RPM = int(os.environ.get("RATE_LIMIT_DEALER_VIN_PER_MIN", "20"))
_DEALER_AUTH_RPM = int(os.environ.get("RATE_LIMIT_DEALER_AUTH_PER_MIN", "20"))
_MIN_DEALER_PASSWORD = max(8, int(os.environ.get("MIN_PASSWORD_LENGTH", "8")))


@bp.route("/dealer/login", methods=["GET", "POST"])
def dealer_login():
    if request.method == "POST":
        ip = _client_ip(request)
        if not allow_request(
            f"dealer_app_login:{ip}", max_events=_DEALER_AUTH_RPM, window_seconds=60.0
        ):
            return (
                render_template("dealer_login.html", error="Too many sign-in attempts. Try again in a minute."),
                429,
            )
        login_input = (request.form.get("login") or "").strip()
        password = (request.form.get("password") or "").strip()
        if check_user(login_input, password):
            u = get_user_by_login(login_input)
            if not u:
                return render_template("dealer_login.html", error="Invalid username or password.")
            if is_admin_role(normalize_role(u.get("role"))):
                from backend.main import (  # noqa: PLC0415
                    _finalize_app_session,
                    _post_mfa_success_redirect,
                )

                session.clear()
                session["mfa_intent"] = MFA_INTENT_GENERAL
                if _finalize_app_session(int(u["id"])):
                    return _post_mfa_success_redirect()
            session.clear()
            session["mfa_intent"] = MFA_INTENT_DEALER
            session["mfa_ok"] = False
            session["mfa_pending_user_id"] = int(u["id"])
            session["mfa_pending_login"] = (u.get("email") or u.get("username") or "").strip()
            clear_session_otp(session, kind="mfa")
            totp_row = get_user_totp(int(u["id"])) or {}
            if totp_row.get("enabled") and (totp_row.get("secret") or "").strip():
                session["mfa_pending_method"] = "totp"
                return redirect("/mfa/verify")
            session["mfa_pending_method"] = "choose"
            return redirect("/mfa/choose")
    return render_template("dealer_login.html")


@bp.route("/dealer/register", methods=["GET", "POST"])
def dealer_register():
    if request.method == "POST":
        ip = _client_ip(request)
        if not allow_request(
            f"dealer_app_register:{ip}", max_events=_DEALER_AUTH_RPM, window_seconds=60.0
        ):
            return (
                render_template("dealer_register.html", error="Too many registration attempts. Try again later."),
                429,
            )
        username = (request.form.get("username") or "").strip()
        email = (request.form.get("email") or "").strip()
        password = (request.form.get("password") or "")
        org_name = (request.form.get("org_name") or "").strip()
        err = registration_form_error(username, email, password, min_password_len=_MIN_DEALER_PASSWORD)
        if err:
            return render_template("dealer_register.html", error=err)
        is_admin = email_is_admin(email)
        if is_admin:
            role = ROLE_ADMIN
            org_id = None
        else:
            if not org_name:
                return render_template("dealer_register.html", error="Organization (dealership) name is required.")
            try:
                org_id = create_org(org_name)
            except sqlite3.IntegrityError:
                return render_template(
                    "dealer_register.html", error="That organization name is already in use."
                )
            except ValueError as e:
                return render_template("dealer_register.html", error=str(e))
            role = ROLE_DEALERSHIP_OWNER
        try:
            uid = save_user(username, email, password, role=role, org_id=org_id)
        except sqlite3.IntegrityError:
            return render_template("dealer_register.html", error="That username or email is already registered.")
        if is_admin:
            from backend.main import (  # noqa: PLC0415
                _finalize_app_session,
                _post_mfa_success_redirect,
            )

            session.clear()
            session["mfa_intent"] = MFA_INTENT_GENERAL
            if _finalize_app_session(int(uid)):
                return _post_mfa_success_redirect()
        session.clear()
        session["mfa_intent"] = MFA_INTENT_DEALER if not is_admin else MFA_INTENT_GENERAL
        session["mfa_ok"] = False
        session["mfa_pending_user_id"] = int(uid)
        session["mfa_pending_login"] = email or username
        clear_session_otp(session, kind="mfa")
        session["mfa_pending_method"] = "choose"
        return redirect("/mfa/choose")
    return render_template("dealer_register.html")


def _require_login() -> int:
    uid = session.get("user_id")
    if uid is None:
        if session.get("mfa_pending_user_id") and not session.get("mfa_ok"):
            return -1
        return 0
    if not session.get("mfa_ok"):
        return -1
    try:
        return int(uid)
    except (TypeError, ValueError):
        return 0


def _vehicle_upload_dir(user_id: int, vehicle_id: int) -> Path:
    p = UPLOAD_ROOT / str(user_id) / str(vehicle_id)
    p.mkdir(parents=True, exist_ok=True)
    return p


def _safe_upload_basename(name: str) -> str | None:
    if not name or "/" in name or "\\" in name or ".." in name:
        return None
    if not re.fullmatch(r"[A-Za-z0-9._-]{1,120}", name):
        return None
    return name


@bp.route("/inventory")
def inventory_dashboard():
    uid = _require_login()
    if uid == -1:
        return redirect(url_for("mfa_verify"))
    if not uid:
        return redirect(url_for("dealer_portal.dealer_login"))
    vehicles = ddb.list_vehicles_for_user(uid)
    return render_template(
        "inventory/dashboard.html",
        vehicles=vehicles,
        username=session.get("username") or "",
    )


@bp.route("/inventory/listings")
def inventory_listings():
    uid = _require_login()
    if uid == -1:
        return redirect(url_for("mfa_verify"))
    if not uid:
        return redirect(url_for("dealer_portal.dealer_login"))
    vehicles = ddb.list_vehicles_for_user(uid)
    return render_template(
        "inventory/listings.html",
        vehicles=vehicles,
        username=session.get("username") or "",
    )


@bp.route("/inventory/add-vin", methods=["POST"])
def inventory_add_vin():
    uid = _require_login()
    if uid == -1:
        abort(403)
    if not uid:
        abort(403)
    ip = _client_ip(request)
    if not allow_request(f"dealer_vin:{ip}", max_events=_DEALER_VIN_RPM, window_seconds=60.0):
        flash("Too many VIN lookups. Try again in a minute.", "error")
        return redirect(url_for("dealer_portal.inventory_listings"))
    vin_raw = (request.form.get("vin") or "").strip().upper()
    if not vin_raw:
        flash("Enter a VIN.", "error")
        return redirect(url_for("dealer_portal.inventory_listings"))
    row, err = build_vehicle_prefill_from_vin(vin_raw)
    if ddb.count_user_vehicles_with_vin(uid, vin_raw):
        flash("That VIN is already in your inventory.", "error")
        return redirect(url_for("dealer_portal.inventory_listings"))
    try:
        ddb.insert_vehicle(uid, row)
    except sqlite3.IntegrityError:
        flash("That VIN is already in your inventory.", "error")
        return redirect(url_for("dealer_portal.inventory_listings"))
    except ValueError as e:
        flash(str(e), "error")
        return redirect(url_for("dealer_portal.inventory_listings"))
    if err:
        flash(f"Vehicle added with partial decode ({err}). Review and edit fields as needed.", "info")
    else:
        flash("Vehicle added from VIN decode.", "success")
    return redirect(url_for("dealer_portal.inventory_listings"))


@bp.route("/inventory/<int:vehicle_id>/upload", methods=["POST"])
def inventory_upload_photos(vehicle_id: int):
    uid = _require_login()
    if uid == -1:
        abort(403)
    if not uid:
        abort(403)
    car = ddb.get_vehicle(uid, vehicle_id)
    if not car:
        abort(404)
    files = request.files.getlist("photos")
    if not files:
        flash("Choose one or more images.", "error")
        return redirect(url_for("dealer_portal.inventory_listings"))
    gallery = list(car.get("gallery") or [])
    if len(gallery) >= _MAX_GALLERY_IMAGES:
        flash("Gallery is full; delete images before adding more.", "error")
        return redirect(url_for("dealer_portal.inventory_listings"))
    saved = 0
    upload_dir = _vehicle_upload_dir(uid, vehicle_id)
    for i, f in enumerate(files):
        if saved >= _MAX_FILES_PER_REQUEST:
            break
        if len(gallery) >= _MAX_GALLERY_IMAGES:
            break
        if not f or not f.filename:
            continue
        mt = (f.mimetype or "").split(";")[0].strip().lower()
        if mt not in _ALLOWED_MT:
            flash(f"Skipped unsupported type ({mt or 'unknown'}). Use JPEG, PNG, or WebP.", "error")
            continue
        ext = _MT_EXT.get(mt)
        if not ext:
            continue
        fname = secrets.token_urlsafe(16).replace("-", "")[:24] + ext
        dest = upload_dir / fname
        f.save(dest)
        try:
            if dest.stat().st_size > _MAX_UPLOAD_BYTES:
                dest.unlink(missing_ok=True)
                flash("An image exceeded the size limit and was discarded.", "error")
                continue
        except OSError:
            _log.warning("Could not stat uploaded file %s", dest)
        rel = f"/dealer-uploads/{uid}/{vehicle_id}/{fname}"
        gallery.append(rel)
        saved += 1
    if saved:
        ddb.update_vehicle_gallery(uid, vehicle_id, gallery)
        flash(f"Saved {saved} photo(s).", "success")
    return redirect(url_for("dealer_portal.inventory_listings"))


@bp.route("/inventory/<int:vehicle_id>/update", methods=["POST"])
def inventory_update_vehicle(vehicle_id: int):
    uid = _require_login()
    if uid == -1:
        abort(403)
    if not uid:
        abort(403)
    if not ddb.get_vehicle(uid, vehicle_id):
        abort(404)

    def _opt_int(key: str) -> Any:
        raw = (request.form.get(key) or "").strip()
        if not raw:
            return None
        try:
            return int(raw)
        except ValueError:
            return None

    def _opt_float(key: str) -> Any:
        raw = (request.form.get(key) or "").strip()
        if not raw:
            return None
        try:
            return float(raw)
        except ValueError:
            return None

    fields: dict[str, Any] = {}
    for k in (
        "title",
        "make",
        "model",
        "trim",
        "transmission",
        "drivetrain",
        "fuel_type",
        "exterior_color",
        "interior_color",
        "body_style",
        "engine_description",
        "stock_number",
        "notes",
    ):
        v = (request.form.get(k) or "").strip()
        if v:
            fields[k] = v[:2000] if k == "notes" else v[:500]
    yi = _opt_int("year")
    if yi is not None:
        fields["year"] = yi
    mi = _opt_int("mileage")
    if mi is not None:
        fields["mileage"] = mi
    ci = _opt_int("cylinders")
    if ci is not None:
        fields["cylinders"] = ci
    pr = _opt_float("price")
    if pr is not None:
        fields["price"] = pr

    if fields:
        ddb.update_vehicle_fields(uid, vehicle_id, fields)
        flash("Vehicle updated.", "success")
    return redirect(url_for("dealer_portal.inventory_listings"))


@bp.route("/inventory/<int:vehicle_id>/delete", methods=["POST"])
def inventory_delete_vehicle(vehicle_id: int):
    uid = _require_login()
    if uid == -1:
        abort(403)
    if not uid:
        abort(403)
    car = ddb.get_vehicle(uid, vehicle_id)
    if not car:
        abort(404)
    for url in car.get("gallery") or []:
        if not isinstance(url, str) or not url.startswith(f"/dealer-uploads/{uid}/{vehicle_id}/"):
            continue
        tail = url.split("/")[-1]
        bn = _safe_upload_basename(tail)
        if bn:
            p = UPLOAD_ROOT / str(uid) / str(vehicle_id) / bn
            try:
                p.unlink(missing_ok=True)
            except OSError:
                pass
    if ddb.delete_vehicle(uid, vehicle_id):
        flash("Vehicle removed.", "success")
    return redirect(url_for("dealer_portal.inventory_listings"))


@bp.route("/dealer-uploads/<int:user_id>/<int:vehicle_id>/<path:filename>")
def dealer_upload_file(user_id: int, vehicle_id: int, filename: str):
    uid = _require_login()
    if uid == -1:
        abort(403)
    if not uid or uid != user_id:
        abort(403)
    if not ddb.get_vehicle(uid, vehicle_id):
        abort(404)
    base = _safe_upload_basename(filename)
    if not base:
        abort(404)
    d = UPLOAD_ROOT / str(user_id) / str(vehicle_id) / base
    if not d.is_file():
        abort(404)
    try:
        d.resolve().relative_to((UPLOAD_ROOT / str(user_id) / str(vehicle_id)).resolve())
    except ValueError:
        abort(404)
    return send_from_directory(
        str(UPLOAD_ROOT / str(user_id) / str(vehicle_id)),
        base,
        max_age=86400,
    )