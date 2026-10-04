"""App login session: populate it after a successful login and pick the redirect.

Moved out of ``backend/main.py`` (monolith audit 2026-10-01, W1) so the Google,
Apple and dealer login flows import it directly instead of reaching back into
``backend.main`` lazily. Also holds the paid-org session checks the post-login
redirect and main's billing gate share.
"""

from __future__ import annotations

from flask import redirect, session, url_for

from backend.billing import access as paid_access
from backend.billing.entitlements import entitlements_from_session
from backend.config import Config
from backend.db.users_db import get_user_profile
from backend.utils.roles import normalize_role


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
    if post_intent == "premium":
        return redirect(url_for("premium_page"))
    return redirect(url_for("app_home"))


def finalize_app_session(user_id: int) -> bool:
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
    session["user_email"] = (u.get("email") or "").strip()
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
