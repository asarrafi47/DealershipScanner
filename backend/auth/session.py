"""App login session: populate it after a successful login and pick the redirect.

Moved out of ``backend/main.py`` (monolith audit 2026-10-01, W1) so the Google,
Apple and dealer login flows import it directly instead of reaching back into
``backend.main`` lazily. Also holds the paid-org session checks the post-login
redirect and main's billing gate share.
"""

from __future__ import annotations

import logging
import math
import os
import time

from flask import current_app, g, redirect, request, session, url_for

from backend.auth.phone.accounts import public_email
from backend.billing import access as paid_access
from backend.billing.entitlements import entitlements_from_session
from backend.config import Config
from backend.db.users_db import get_user_profile
from backend.utils.roles import account_has_env_admin_privilege, is_dealer_portal_role, normalize_role

_log = logging.getLogger("auth_session")


def billing_enabled() -> bool:
    return Config.billing_stripe_enabled()


def org_subscription_active(status: str | None) -> bool:
    s = (status or "").strip().lower()
    return s in ("active", "trialing")


def require_paid_org_session() -> bool:
    if not billing_enabled():
        return True
    if not session.get("user_id"):
        return True
    if paid_access.is_site_admin():
        return True
    st = session.get("org_subscription_status")
    return bool(org_subscription_active(st))


def session_belongs_to_paid_org() -> bool:
    """Stripe subscription (when enabled) applies only to users tied to a dealership org."""
    if not session.get("user_id"):
        return False
    oid = session.get("org_id")
    if oid is None:
        return False
    try:
        return int(oid) > 0
    except (TypeError, ValueError):
        return False


def post_login_redirect():
    post_intent = session.pop("post_auth_intent", None)
    if (
        billing_enabled()
        and (not paid_access.is_site_admin())
        and session_belongs_to_paid_org()
        and (not require_paid_org_session())
    ):
        return redirect(url_for("billing.billing_required"))
    if post_intent == "premium" and billing_enabled():  # the site is free: nothing to offer
        return redirect(url_for("premium_page"))
    return redirect(url_for("app_home"))


def finalize_app_session(user_id: int, auth_method: str = "password") -> bool:
    from backend.db.users_db import get_org

    try:
        uid = int(user_id)
    except (TypeError, ValueError):
        return False
    u = get_user_profile(uid)
    if not u:
        return False
    from backend.db.users_db import _user_row_is_active

    if not _user_row_is_active(u):
        return False
    session.permanent = True
    session["user_id"] = int(u["id"])
    session["username"] = u["username"]
    session["user_email"] = public_email((u.get("email") or "").strip())
    session["user_role"] = normalize_role(u.get("role"))
    session["user_dealer_id"] = (u.get("dealer_id") or "").strip()
    rid = u.get("dealership_registry_id")
    session["user_dealership_registry_id"] = str(int(rid)) if rid is not None else ""
    session["org_id"] = int(u.get("org_id") or 0) if u.get("org_id") else 0
    session["user_is_premium"] = bool(u.get("is_premium"))
    session["subscription_plan_id"] = (u.get("subscription_plan_id") or "").strip() or None
    session["entitlements"] = sorted(entitlements_from_session(session))
    # entitlements_from_session self-heals user_is_premium to the raw DB column;
    # store the AccessContext rule instead so the flag does not flip on the next
    # request (access.build_access_context heals with the same rule).
    from backend.billing.access import session_premium_flag

    session["user_is_premium"] = session_premium_flag(
        u.get("is_premium"), session.get("subscription_plan_id")
    )
    for _stale in (
        "mfa_pending_user_id",
        "mfa_pending_login",
        "mfa_pending_method",
        "mfa_qr_attempt_id",
        "mfa_test_last_code",
        "mfa_next",
    ):
        session.pop(_stale, None)
    session["mfa_ok"] = True
    stamp_login(auth_method=auth_method, auth_role=session_class_for_user(u))
    session["org_subscription_status"] = None
    if session.get("org_id"):
        try:
            org = get_org(int(session["org_id"]))
            if org:
                session["org_subscription_status"] = (
                    (org.get("stripe_subscription_status") or "").strip().lower() or None
                )
        except Exception:
            session["org_subscription_status"] = None
    return True


# ---------------------------------------------------------------- session lifetimes (card S10)
#
# Shoppers: SESSION_DAYS_SHOPPER (30) days since the last request (sliding).
# Admins and /dev operators: SESSION_HOURS_ADMIN (12) hours from sign-in (absolute, never sliding).
# Dealers and other staff: SESSION_DAYS_STAFF (14) days from sign-in (absolute).
# The role comes from the users row on every request, never from the cookie.

CLASS_ADMIN = "admin"
CLASS_DEALER = "dealer"
CLASS_STAFF = "staff"
CLASS_SHOPPER = "shopper"
CLASS_DEV = "dev"

# Session keys written at sign-in (finalize_app_session, the /dev login and both OAuth completions).
AUTH_AT = "auth_at"  # epoch seconds (float) of the sign-in
AUTH_METHOD = "auth_method"  # password, google, apple, phone, legacy (stamped by the hook once)
AUTH_ROLE = "auth_role"  # the lifetime class at sign-in; the hook keeps it current
AUTH_SEEN_AT = "auth_seen_at"  # shoppers only: last request, for the sliding limit
DEV_AUTH_AT = "dev_auth_at"  # /dev operator sign-in (separate users table, separate key)
# Set only by sign_out_everywhere: the original sign-in time, so re-minting the session for the
# valid_after check never restarts an absolute (admin, dealer, staff) limit or the mfa_ok window.
AUTH_ORIGIN_AT = "auth_origin_at"

_SEEN_WRITE_EVERY_S = 300.0  # write auth_seen_at at most this often
_CLOCK_SKEW_S = 300.0  # an auth_at further in the future than this is inconsistent


def _env_number(name: str, default: float) -> float:
    raw = (os.environ.get(name) or "").strip()
    if not raw:
        return default
    try:
        val = float(raw)
    except ValueError:
        return default
    if not math.isfinite(val) or val <= 0:
        return default
    return val


def session_days_shopper() -> float:
    return _env_number("SESSION_DAYS_SHOPPER", 30.0)


def session_hours_admin() -> float:
    return _env_number("SESSION_HOURS_ADMIN", 12.0)


def session_days_staff() -> float:
    return _env_number("SESSION_DAYS_STAFF", 14.0)


def _now() -> float:
    return time.time()


def session_class_for_user(u: dict) -> str:
    """Lifetime class of a users row: admin, dealer, staff or shopper (stored role, not the cookie)."""
    raw = (u.get("role") or "").strip().lower()
    if raw == "admin" or account_has_env_admin_privilege(u.get("email"), u.get("username")):
        return CLASS_ADMIN
    if is_dealer_portal_role(raw):
        return CLASS_DEALER
    if raw == "dealer_staff":
        return CLASS_STAFF
    if not raw and (u.get("dealer_id") or u.get("dealership_registry_id") or u.get("org_id")):
        return CLASS_STAFF
    # general_user, legacy spellings such as "general", and anything normalize_role reads as a shopper
    return CLASS_SHOPPER


def stamp_login(*, auth_method: str, auth_role: str) -> None:
    """Write the sign-in time, method and lifetime class into the current session."""
    now = _now()
    session[AUTH_AT] = now
    session[AUTH_SEEN_AT] = now
    session[AUTH_METHOD] = (auth_method or "password").strip().lower() or "password"
    session[AUTH_ROLE] = auth_role


def _as_time(raw) -> float | None:
    if isinstance(raw, bool) or not isinstance(raw, (int, float)):
        return None
    val = float(raw)
    return val if math.isfinite(val) else None


def _end_session(reason: str) -> None:
    session.clear()
    g.session_ended = reason
    _log.info("event=session_ended reason=%s", reason)


def _epoch_refuses(uid: int, auth_at: float | None, *, strict: bool) -> str | None:
    """Why the valid_after mark refuses this session, or None. Fails closed only when ``strict``."""
    from backend.auth.phone import session_epoch

    try:
        valid_after = session_epoch.get_valid_after(uid, strict=strict)
    except session_epoch.EpochLookupFailed:
        return "epoch_lookup_failed" if strict else None
    if valid_after is None:
        return None
    if auth_at is None or auth_at < valid_after:
        return "revoked"
    return None


def _load_user(uid: int) -> dict | None:
    """The users row the hook judges (one seam, so route tests that fake a user id can opt in)."""
    from backend.db.users_db import get_user_profile

    return get_user_profile(uid)


def _check_dev_session(now: float) -> str | None:
    auth_at = _as_time(session.get(DEV_AUTH_AT))
    if auth_at is None:
        return "dev_no_auth_at"
    if auth_at > now + _CLOCK_SKEW_S:
        return "dev_inconsistent"
    if now - auth_at > session_hours_admin() * 3600.0:
        return "dev_expired"
    return None


def _check_app_session(now: float) -> str | None:
    try:
        uid = int(session.get("user_id"))
    except (TypeError, ValueError):
        return "bad_user_id"
    if uid <= 0:
        return "bad_user_id"
    from backend.db.users_db import _user_row_is_active

    try:
        u = _load_user(uid)
    except Exception as exc:
        # users table unreachable: refuse sessions that claim a staff role, keep shoppers.
        _log.warning("event=session_user_lookup_failed err=%s", type(exc).__name__)
        claimed = normalize_role(session.get("user_role"))
        if claimed != "general_user" or session.get(AUTH_ROLE) not in (None, CLASS_SHOPPER):
            return "user_lookup_failed"
        return None
    if not u:
        return "user_missing"
    if not _user_row_is_active(u):
        return "user_inactive"
    cls = session_class_for_user(u)
    strict = cls != CLASS_SHOPPER

    raw_at = session.get(AUTH_AT)
    auth_at = _as_time(raw_at)
    if raw_at is not None and auth_at is None:
        return "inconsistent"
    if auth_at is None:
        # Cookie from before this hook existed: admin and staff sign in again once; shoppers and
        # dealers keep their cookie and are stamped now (unless a valid_after row exists).
        if cls in (CLASS_ADMIN, CLASS_STAFF):
            return "no_auth_at"
        why = _epoch_refuses(uid, None, strict=strict)
        if why:
            return why
        stamp_login(auth_method="legacy", auth_role=cls)
        return None
    if auth_at > now + _CLOCK_SKEW_S:
        return "inconsistent"
    origin = _as_time(session.get(AUTH_ORIGIN_AT))
    if session.get(AUTH_ORIGIN_AT) is not None and origin is None:
        return "inconsistent"
    signed_in_at = auth_at if origin is None else min(origin, auth_at)

    if cls == CLASS_ADMIN:
        if now - signed_in_at > session_hours_admin() * 3600.0:
            return "expired"
    elif cls in (CLASS_DEALER, CLASS_STAFF):
        if now - signed_in_at > session_days_staff() * 86400.0:
            return "expired"
    else:
        seen = _as_time(session.get(AUTH_SEEN_AT))
        if seen is None or seen < auth_at or seen > now + _CLOCK_SKEW_S:
            seen = auth_at
        if now - seen > session_days_shopper() * 86400.0:
            return "expired"

    why = _epoch_refuses(uid, auth_at, strict=strict)
    if why:
        return why

    # The authenticator flag obeys the admin absolute limit for every role.
    if session.get("mfa_ok") and now - signed_in_at > session_hours_admin() * 3600.0:
        session["mfa_ok"] = False
    if session.get(AUTH_ROLE) != cls:
        session[AUTH_ROLE] = cls  # promotion or demotion applies from this request on
    db_role = normalize_role(u.get("role"))
    if session.get("user_role") != db_role:
        session["user_role"] = db_role  # a demoted admin loses cookie-trusting checks (/dev pass-through) at once
    if cls == CLASS_SHOPPER:
        seen = _as_time(session.get(AUTH_SEEN_AT))
        if seen is None or now - seen >= _SEEN_WRITE_EVERY_S:
            session[AUTH_SEEN_AT] = now
    return None


def enforce_session_policy():
    """``before_request``: end expired, revoked or inconsistent sessions (card S10).

    Registered first among the app hooks (before the CSP nonce, CSRF and device-cookie hooks).
    A request without a session cookie is not touched (no Vary: Cookie, no query). Never blocks
    the request itself: a refused session is cleared and the request goes on signed out, so each
    route answers as it does for a signed-out visitor.
    """
    if request.endpoint == "static":
        return None
    if not request.cookies.get(current_app.config.get("SESSION_COOKIE_NAME") or "session"):
        return None
    has_app = session.get("user_id") is not None
    has_dev = session.get("admin_user_id") is not None
    if not (has_app or has_dev):
        return None
    now = _now()
    why = None
    if has_dev:
        why = _check_dev_session(now)
    if why is None and has_app:
        why = _check_app_session(now)
    if why:
        _end_session(why)
    return None


def register_session_policy(app) -> None:
    app.before_request(enforce_session_policy)


def sign_out_everywhere(user_id: int, *, reason: str = "sign_out_everywhere") -> bool:
    """Void every session of ``user_id`` and give this browser a fresh one. False if not stored.

    The new session is minted after ``valid_after`` is written, with ``auth_at >= valid_after``.
    """
    from backend.auth.phone.session_epoch import revoke_user_sessions

    method = session.get(AUTH_METHOD) or "password"
    # The original sign-in time and authenticator state survive the re-mint: this form must never
    # restart the admin 12-hour or staff 14-day limit, nor turn mfa_ok on.
    olds = [t for t in (_as_time(session.get(AUTH_AT)), _as_time(session.get(AUTH_ORIGIN_AT))) if t is not None]
    old_mfa = bool(session.get("mfa_ok"))
    stored = revoke_user_sessions(int(user_id), reason=reason)
    if stored is None:
        return False
    session.clear()
    if not finalize_app_session(int(user_id), auth_method=str(method)):
        return False
    # auth_at must not be older than valid_after (both come from this process's clock).
    if _as_time(session.get(AUTH_AT)) is not None and session[AUTH_AT] < stored:
        session[AUTH_AT] = stored
        session[AUTH_SEEN_AT] = stored
    session[AUTH_ORIGIN_AT] = min(olds) if olds else session[AUTH_AT]
    session["mfa_ok"] = old_mfa
    return True
