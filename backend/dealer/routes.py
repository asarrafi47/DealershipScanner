"""
Dealer-facing inventory management at ``/inventory`` (session: app ``users`` table).
"""
from __future__ import annotations

import logging
import os

from backend.config import Config
import re
import secrets
import sqlite3
from pathlib import Path
from typing import Any

from flask import (
    Blueprint,
    abort,
    flash,
    make_response,
    redirect,
    render_template,
    request,
    send_from_directory,
    session,
    url_for,
)

from backend.db import dealer_portal_db as ddb
from backend.db.users_db import (
    check_user,
    submitted_password_attempts,
    create_org,
    get_user_by_login,
    get_user_profile,
    save_user,
    sync_env_admin_user_row,
    user_exists_by_email,
    user_exists_by_username,
)
from backend.utils.client_ip import client_ip as _client_ip
from backend.utils.dealer_vin_prefill import build_vehicle_prefill_from_vin
from backend.utils.ip_rate_limit import allow_request, refund_event
from backend.utils.registration_validation import (
    normalize_registration_email,
    normalize_registration_username,
    registration_form_error,
    username_format_error,
)
from backend.utils.roles import (
    ROLE_DEALERSHIP_ADMIN,
    ROLE_DEALERSHIP_MEMBER,
    ROLE_DEALERSHIP_OWNER,
    REGISTRATION_ADMIN_BLOCKED_MSG,
    is_admin_role,
    normalize_role,
    registration_blocked_by_env_admin,
)
from backend.utils.runtime_env import is_production_env

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
_MIN_DEALER_PASSWORD = Config.MIN_PASSWORD_LENGTH

# Card S19: the sentences a seller or a visitor sees (plain words, no codes).
DEALER_LOGIN_ERROR = "Invalid username or password."
SIGNUP_CLOSED_MSG = "Dealer sign-up is not available right now."
SIGNUP_PAUSED_MSG = "Dealer sign-up is paused right now. Please try again later."
SIGNUP_TURNSTILE_MSG = "Please complete the check that you are not a robot, then try again."
SIGNUP_HOURLY_MSG = "Too many dealer sign-ups from your network. Please try again in an hour."
INVENTORY_ROLE_MSG = "Only dealership accounts can manage cars here for now."
POSTING_SOON_MSG = "Posting cars publicly opens soon. Your car is saved."
VIN_TAKEN_MSG = "This VIN is already listed on the site; it was saved but not published."

# Roles allowed into /inventory until card S22 opens it to phone-verified private sellers.
_INVENTORY_ROLES = frozenset({ROLE_DEALERSHIP_OWNER, ROLE_DEALERSHIP_ADMIN, ROLE_DEALERSHIP_MEMBER})
_REFUSED = -1  # _require_login: signed in, active, but the role may not use /inventory

# 3 successful dealer sign-ups an hour per address (memory, per process: the site runs one
# worker). A slot is reserved before the Turnstile check and given back when no account is made.
_SIGNUPS_PER_HOUR = 3
_SIGNUP_WINDOW_S = 3600.0
_TURNSTILE_FIELD = "cf-turnstile-response"


def _signup_key(ip: str) -> str:
    from backend.auth.pages import _login_ip_key  # noqa: PLC0415

    return f"dealer_signup_hour:{_login_ip_key(ip)}"


def _turnstile_keys_set() -> bool:
    from backend.auth.phone import config as phone_config  # noqa: PLC0415

    return bool(phone_config.turnstile_site_key() and phone_config.turnstile_secret_key())


def _turnstile_site_key() -> str:
    from backend.auth.phone import config as phone_config  # noqa: PLC0415

    return phone_config.turnstile_site_key()


def dealer_signup_closed() -> bool:
    """In production without both Turnstile keys, dealer sign-up is closed (fails closed)."""
    return is_production_env() and not _turnstile_keys_set()


def _signup_paused() -> bool:
    try:
        from backend import settings_store  # noqa: PLC0415

        return settings_store.get_bool("seller.signup_paused")
    except Exception:
        _log.exception("dealer register: could not read seller.signup_paused")
        return False


def _register_page(*, error: str | None = None, status: int = 200):
    closed = dealer_signup_closed()
    return (
        render_template(
            "dealer_register.html",
            error=error,
            signup_closed=closed,
            turnstile_site_key="" if closed else _turnstile_site_key(),
        ),
        status,
    )


@bp.route("/dealer/login", methods=["GET", "POST"])
def dealer_login():
    if request.method == "POST":
        from backend.auth.pages import (  # noqa: PLC0415
            _LOCKED_MSG, _login_ip_key, _refund_login_slot, _reserve_login_slot,
        )

        ip = _client_ip(request)
        if not allow_request(
            f"dealer_app_login:{_login_ip_key(ip)}", max_events=_DEALER_AUTH_RPM, window_seconds=60.0
        ):
            return (
                render_template("dealer_login.html", error="Too many sign-in attempts. Try again in a minute."),
                429,
            )
        login_input = (request.form.get("login") or "").strip()
        attempts = submitted_password_attempts(request.form.get("password"))
        if not login_input or not attempts:
            return render_template("dealer_login.html", error=DEALER_LOGIN_ERROR)
        # Same per-account lockout as /login and /api/auth/login (SEC-A-010).
        slot = _reserve_login_slot(login_input)
        if slot is None:
            return render_template("dealer_login.html", error=_LOCKED_MSG), 429
        if any(check_user(login_input, pw) for pw in attempts):
            _refund_login_slot(slot)
            u = get_user_by_login(login_input)
            if not u:
                return render_template("dealer_login.html", error=DEALER_LOGIN_ERROR)
            sync_env_admin_user_row(int(u["id"]))
            u = get_user_by_login(login_input)
            if not u:
                return render_template("dealer_login.html", error=DEALER_LOGIN_ERROR)
            from backend.auth.session import finalize_app_session as _finalize_app_session, post_login_redirect as _post_login_redirect  # noqa: PLC0415
            session.clear()
            if not _finalize_app_session(int(u["id"])):
                return render_template("dealer_login.html", error=DEALER_LOGIN_ERROR)
            return _post_login_redirect()
        # Wrong password (or unknown account): the same generic sentence as every other failure.
        return render_template("dealer_login.html", error=DEALER_LOGIN_ERROR)
    return render_template("dealer_login.html")


def _dealer_register_post(ip: str):
    """The sign-up itself, after the door checks. Returns (response, account_created)."""
    from backend.auth.phone.turnstile import verify_turnstile  # noqa: PLC0415

    if not verify_turnstile(request.form.get(_TURNSTILE_FIELD), ip).ok:
        return _register_page(error=SIGNUP_TURNSTILE_MSG, status=400), False
    username = normalize_registration_username(request.form.get("username") or "")
    email = normalize_registration_email(request.form.get("email") or "")
    password = (request.form.get("password") or "")
    org_name = (request.form.get("org_name") or "").strip()
    err = registration_form_error(
        username, email, password, min_password_len=_MIN_DEALER_PASSWORD
    ) or username_format_error(username)
    if err:
        return _register_page(error=err), False
    if registration_blocked_by_env_admin(email, username):
        return _register_page(error=REGISTRATION_ADMIN_BLOCKED_MSG), False
    if user_exists_by_email(email) or user_exists_by_username(username):
        return _register_page(error="That username or email is already registered."), False
    if not org_name:
        return _register_page(error="Organization (dealership) name is required."), False
    try:
        org_id = create_org(org_name)
    except sqlite3.IntegrityError:
        return _register_page(error="That organization name is already in use."), False
    except ValueError as e:
        return _register_page(error=str(e)), False
    try:
        uid = save_user(username, email, password, role=ROLE_DEALERSHIP_OWNER, org_id=org_id)
    except sqlite3.IntegrityError:
        return _register_page(error="That username or email is already registered."), False
    from backend.auth.session import finalize_app_session as _finalize_app_session, post_login_redirect as _post_login_redirect  # noqa: PLC0415
    session.clear()
    if not _finalize_app_session(int(uid)):
        return _register_page(error="Registration failed. Try again."), True
    return _post_login_redirect(), True


@bp.route("/dealer/register", methods=["GET", "POST"])
def dealer_register():
    if request.method == "POST":
        ip = _client_ip(request)
        if not allow_request(
            f"dealer_app_register:{ip}", max_events=_DEALER_AUTH_RPM, window_seconds=60.0
        ):
            return _register_page(error="Too many registration attempts. Try again later.", status=429)
        # Card S19 (3): checked before any form field, so a closed or paused door and a bot
        # learn nothing about which usernames or emails exist.
        if _signup_paused():
            return _register_page(error=SIGNUP_PAUSED_MSG, status=503)
        if dealer_signup_closed():
            return _register_page(status=503)  # the page itself says SIGNUP_CLOSED_MSG
        signup_key = _signup_key(ip)
        if not allow_request(signup_key, max_events=_SIGNUPS_PER_HOUR, window_seconds=_SIGNUP_WINDOW_S):
            return _register_page(error=SIGNUP_HOURLY_MSG, status=429)
        created = False
        try:
            response, created = _dealer_register_post(ip)
        finally:
            if not created:
                refund_event(signup_key)  # only accounts actually made count towards the 3
        return response
    return _register_page()


def _require_login() -> int:
    """The signed-in account id; 0 when signed out, suspended or missing; ``_REFUSED`` (-1)
    when the account may not use /inventory (card S19 (2): ``dealership_*`` or ``admin`` only;
    a ``general_user``, a legacy ``dealer_staff`` or a missing role is refused)."""
    uid = session.get("user_id")
    if uid is None:
        return 0
    try:
        uid = int(uid)
    except (TypeError, ValueError):
        return 0
    # Re-fetch the account from the DB on every call rather than trusting the
    # session-cached id alone — a suspended/deleted dealer-portal user's
    # existing cookie must lose inventory access immediately, not after its
    # 14-day expiry (mirrors operator_api._require_site_admin_api).
    profile = get_user_profile(uid)
    if not profile or not profile.get("is_active", True):
        return 0
    role = normalize_role(profile.get("role"))
    if not (role in _INVENTORY_ROLES or is_admin_role(role)):
        return _REFUSED
    return uid


def _refused_response():
    """403 with a plain sentence (no template: works for every /inventory route)."""
    from markupsafe import escape  # noqa: PLC0415

    body = (
        "<!DOCTYPE html><html lang=\"en\"><head><meta charset=\"UTF-8\">"
        "<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">"
        "<title>Not available</title></head><body>"
        f"<p>{escape(INVENTORY_ROLE_MSG)}</p><p><a href=\"/\">Back to the home page</a></p>"
        "</body></html>"
    )
    resp = make_response(body, 403)
    resp.headers["Content-Type"] = "text/html; charset=utf-8"
    return resp


def _tell_sync_result(result: str) -> None:
    """Card S19 (1) and (5): tell the seller when a saved car did not reach the public site."""
    from backend.dealer.portal_sync import SYNC_PRIVATE, SYNC_VIN_TAKEN  # noqa: PLC0415

    if result == SYNC_VIN_TAKEN:
        flash(VIN_TAKEN_MSG, "error")
    elif result == SYNC_PRIVATE:
        flash(POSTING_SOON_MSG, "info")


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
    if uid == _REFUSED:
        return _refused_response()
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
    if uid == _REFUSED:
        return _refused_response()
    if not uid:
        return redirect(url_for("dealer_portal.dealer_login"))
    vehicles = ddb.list_vehicles_for_user(uid)
    from backend.dealer.portal_sync import account_may_publish  # noqa: PLC0415

    return render_template(
        "inventory/listings.html",
        vehicles=vehicles,
        username=session.get("username") or "",
        posting_note="" if account_may_publish(uid) else POSTING_SOON_MSG,
    )


@bp.route("/inventory/add-vin", methods=["POST"])
def inventory_add_vin():
    uid = _require_login()
    if uid == _REFUSED:
        return _refused_response()
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
    # Duplicate check BEFORE the network VIN-decode lookup: a VIN already in
    # this dealer's inventory would be rejected after the decode call anyway
    # (insert_vehicle's UNIQUE(user_id, vin)), so checking first skips the
    # wasted network round-trip for a VIN we already know we'll refuse.
    if ddb.count_user_vehicles_with_vin(uid, vin_raw):
        flash("That VIN is already in your inventory.", "error")
        return redirect(url_for("dealer_portal.inventory_listings"))
    row, err = build_vehicle_prefill_from_vin(vin_raw)
    try:
        new_id = ddb.insert_vehicle(uid, row)
    except sqlite3.IntegrityError:
        flash("That VIN is already in your inventory.", "error")
        return redirect(url_for("dealer_portal.inventory_listings"))
    except ValueError as e:
        flash(str(e), "error")
        return redirect(url_for("dealer_portal.inventory_listings"))
    from backend.dealer.portal_sync import sync_portal_vehicle_to_public_listings

    _tell_sync_result(sync_portal_vehicle_to_public_listings(uid, ddb.get_vehicle(uid, new_id) or {}))
    if err:
        flash(f"Vehicle added with partial decode ({err}). Review and edit fields as needed.", "info")
    else:
        flash("Vehicle added from VIN decode.", "success")
    return redirect(url_for("dealer_portal.inventory_listings"))


@bp.route("/inventory/<int:vehicle_id>/upload", methods=["POST"])
def inventory_upload_photos(vehicle_id: int):
    uid = _require_login()
    if uid == _REFUSED:
        return _refused_response()
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
        from backend.dealer.portal_sync import sync_portal_vehicle_to_public_listings

        _tell_sync_result(sync_portal_vehicle_to_public_listings(uid, ddb.get_vehicle(uid, vehicle_id) or {}))
        flash(f"Saved {saved} photo(s).", "success")
    return redirect(url_for("dealer_portal.inventory_listings"))


@bp.route("/inventory/<int:vehicle_id>/photo/delete", methods=["POST"])
def inventory_delete_photo(vehicle_id: int):
    """Remove one photo from a vehicle's gallery without deleting the vehicle."""
    uid = _require_login()
    if uid == _REFUSED:
        return _refused_response()
    if not uid:
        abort(403)
    car = ddb.get_vehicle(uid, vehicle_id)
    if not car:
        abort(404)
    target = (request.form.get("url") or "").strip()
    gallery = list(car.get("gallery") or [])
    if not target or target not in gallery:
        flash("That photo was not found.", "error")
        return redirect(url_for("dealer_portal.inventory_listings"))
    gallery = [u for u in gallery if u != target]
    if target.startswith(f"/dealer-uploads/{uid}/{vehicle_id}/"):
        bn = _safe_upload_basename(target.split("/")[-1])
        if bn:
            p = UPLOAD_ROOT / str(uid) / str(vehicle_id) / bn
            try:
                p.unlink(missing_ok=True)
            except OSError:
                pass
    ddb.update_vehicle_gallery(uid, vehicle_id, gallery)
    from backend.dealer.portal_sync import sync_portal_vehicle_to_public_listings

    _tell_sync_result(sync_portal_vehicle_to_public_listings(uid, ddb.get_vehicle(uid, vehicle_id) or {}))
    flash("Photo removed.", "success")
    return redirect(url_for("dealer_portal.inventory_listings"))


@bp.route("/inventory/<int:vehicle_id>/update", methods=["POST"])
def inventory_update_vehicle(vehicle_id: int):
    uid = _require_login()
    if uid == _REFUSED:
        return _refused_response()
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
        from backend.dealer.portal_sync import sync_portal_vehicle_to_public_listings

        _tell_sync_result(sync_portal_vehicle_to_public_listings(uid, ddb.get_vehicle(uid, vehicle_id) or {}))
        flash("Vehicle updated.", "success")
    return redirect(url_for("dealer_portal.inventory_listings"))


@bp.route("/inventory/<int:vehicle_id>/delete", methods=["POST"])
def inventory_delete_vehicle(vehicle_id: int):
    uid = _require_login()
    if uid == _REFUSED:
        return _refused_response()
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
        from backend.dealer.portal_sync import remove_portal_vehicle_from_public_listings

        remove_portal_vehicle_from_public_listings(car.get("vin") or "", uid)
        flash("Vehicle removed.", "success")
    return redirect(url_for("dealer_portal.inventory_listings"))


@bp.route("/dealer-uploads/<int:user_id>/<int:vehicle_id>/<path:filename>")
def dealer_upload_file(user_id: int, vehicle_id: int, filename: str):
    uid = _require_login()
    if uid == _REFUSED:
        return _refused_response()
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