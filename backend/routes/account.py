"""Account pages: profile + password, billing summary and Stripe portal entry.

Moved out of ``backend/main.py`` (monolith audit W1). ``register(app)`` keeps the
original bare endpoint names (templates and the CSRF hook match them).
"""

from __future__ import annotations

import logging

from flask import redirect, render_template, request, session, url_for

from backend.auth.phone.accounts import is_placeholder_email, public_email
from backend.billing import access as paid_access
from backend.billing.catalog import FEATURE_SAVED_SEARCHES
from backend.config import Config
from backend.db.inventory_db import list_hidden_dealers, list_saved_searches, list_search_history
from backend.db.users_db import (
    change_user_password,
    get_user_profile,
    sync_env_admin_user_row,
    update_user_profile,
)
from backend.utils.roles import normalize_role

_logger = logging.getLogger(__name__)


def _account_profile_context(uid: int, **extra):
    u = get_user_profile(int(uid)) or {}
    role = normalize_role(u.get("role"))
    role_labels = {
        "admin": "Site administrator",
        "general_user": "Member",
        "dealership_owner": "Dealership owner",
        "dealership_admin": "Dealership admin",
        "dealership_member": "Dealership member",
    }
    ctx = {
        "username": (u.get("username") or "").strip(),
        "email": public_email((u.get("email") or "").strip()),
        "phone_account": is_placeholder_email(u.get("email")),
        "role": role,
        "role_label": role_labels.get(role, role.replace("_", " ").title()),
        "is_premium": bool(u.get("is_premium")),
        "billing_on": Config.billing_stripe_enabled(),
        "min_password_len": Config.MIN_PASSWORD_LENGTH,
        "hidden_dealers": _hidden_dealers_for_profile(uid),
        "recent_searches": _recent_searches_for_profile(uid),
        "saved_searches": _saved_searches_for_profile(uid),
        "saved_searches_enabled": paid_access.check_feature(FEATURE_SAVED_SEARCHES)[0],
        "recent_searches_limit": _PROFILE_RECENT_SEARCHES,
    }
    ctx.update(extra)
    return ctx


def _hidden_dealers_for_profile(uid: int) -> list[dict]:
    """Server-rendered rows for the profile's Hidden dealerships section ([] on any error)."""
    try:
        return list_hidden_dealers(int(uid))
    except Exception:
        _logger.debug("hidden dealers lookup failed for user %s", uid, exc_info=True)
        return []


_PROFILE_RECENT_SEARCHES = 20


def _recent_searches_for_profile(uid: int) -> list[dict]:
    """Server-rendered rows for the profile's Recent searches section ([] on any error).

    Each row carries ``label`` / ``url`` / ``when`` from search_history_format on top
    of the repo fields, so the template and account_profile.js render the same text."""
    try:
        from backend.utils.search_history_format import decorate_search_rows

        return decorate_search_rows(list_search_history(int(uid), _PROFILE_RECENT_SEARCHES))
    except Exception:
        _logger.debug("search history lookup failed for user %s", uid, exc_info=True)
        return []


def _saved_searches_for_profile(uid: int) -> list[dict]:
    """Server-rendered rows for the profile's Saved searches section ([] on any error)."""
    try:
        from backend.utils.search_history_format import decorate_search_rows

        return decorate_search_rows(list_saved_searches(int(uid)))
    except Exception:
        _logger.debug("saved searches lookup failed for user %s", uid, exc_info=True)
        return []


_EMAIL_CHANGE_FRESH_SIGN_IN_S = 600.0  # accounts with no usable password: Google/Apple sign-in this recent


def _email_change_reauth_error(uid: int) -> str | None:
    """Why this email change may not go ahead, or None. Email change confirmation: a change needs
    the current password. A Google or Apple account (random password hash) instead needs a Google
    or Apple sign-in in the last 10 minutes."""
    from backend.auth.session import AUTH_AT, AUTH_METHOD, AUTH_ORIGIN_AT, _as_time, _now
    from backend.db.users_db.auth import user_has_oauth_identity, verify_user_password
    from backend.utils.ip_rate_limit import allow_request

    pw = request.form.get("current_password") or ""
    if pw.strip():
        if not allow_request(f"email_change_pw:{int(uid)}", max_events=10, window_seconds=900.0):
            return "Too many attempts. Try again in 15 minutes."
        if verify_user_password(uid, pw):
            return None
        return "Current password is incorrect."
    if user_has_oauth_identity(uid):
        times = [t for t in (_as_time(session.get(AUTH_AT)), _as_time(session.get(AUTH_ORIGIN_AT))) if t is not None]
        signed_in_at = min(times) if times else None  # sign out everywhere never refreshes it
        if (
            session.get(AUTH_METHOD) in ("google", "apple")
            and signed_in_at is not None
            and 0 <= _now() - signed_in_at <= _EMAIL_CHANGE_FRESH_SIGN_IN_S
        ):
            return None
        return (
            "To change the email of an account that signs in with Google or Apple, sign out, "
            "sign in again with Google or Apple, and change it within 10 minutes."
        )
    return "Enter your current password to change your email."


def _notify_old_email(old_email: str) -> None:
    """Tell the previous address that the account email changed. Never fails the request."""
    try:
        em = (old_email or "").strip()
        if not em or "@" not in em or is_placeholder_email(em):
            return
        from backend.utils.mfa_delivery import send_transactional_email

        text = (
            "The email address on your Sarrafi Collection account was just changed, and this "
            "address will no longer receive messages about it.\n\n"
            "If you did not make this change, reply to this message or contact support right away.\n"
        )
        # Inline, never the background pool (which drops mail when full): this request is signed
        # in, and the provider calls have their own timeouts.
        send_transactional_email(
            to_email=em,
            subject="Your Sarrafi Collection email was changed",
            text=text,
            log_surface="email_change_notice",
        )
    except Exception as exc:  # noqa: BLE001 - a notice never fails the change
        _logger.warning("email change notice failed: %s", type(exc).__name__)


def account_password_page():
    """Backward-compatible alias for the password section on the profile page."""
    if request.method == "POST":
        return account_profile_page()
    return redirect(url_for("account_profile_page", _anchor="password"))


def account_profile_page():
    """Signed-in users can update profile info and change password."""
    uid = session.get("user_id")
    if not uid:
        return redirect(url_for("login_page"))
    uid = int(uid)

    if request.method == "POST":
        action = (request.form.get("form_action") or "profile").strip().lower()
        if action == "sign_out_everywhere":
            from backend.auth.session import sign_out_everywhere

            if sign_out_everywhere(uid):
                return render_template(
                    "account_profile.html",
                    **_account_profile_context(uid, signout_success=True),
                )
            return render_template(
                "account_profile.html",
                **_account_profile_context(uid, signout_error=True),
            )
        if action == "password":
            current_pw = (request.form.get("current_password") or "").strip()
            new_pw = (request.form.get("new_password") or "").strip()
            confirm_pw = (request.form.get("confirm_password") or "").strip()
            if new_pw != confirm_pw:
                return render_template(
                    "account_profile.html",
                    **_account_profile_context(uid, password_error="New passwords do not match."),
                )
            from backend.utils.registration_validation import registration_form_error

            u = get_user_profile(uid) or {}
            fmt_err = registration_form_error(
                u.get("username") or "user",
                u.get("email") or "user@local",
                new_pw,
                min_password_len=Config.MIN_PASSWORD_LENGTH,
            )
            if fmt_err:
                return render_template(
                    "account_profile.html",
                    **_account_profile_context(uid, password_error=fmt_err),
                )
            err = change_user_password(uid, current_pw, new_pw)
            if err:
                return render_template(
                    "account_profile.html",
                    **_account_profile_context(uid, password_error=err),
                )
            # Card S10: a new password signs out every other browser; this one keeps a fresh session.
            from backend.auth.phone.session_epoch import REASON_PASSWORD_CHANGE
            from backend.auth.session import sign_out_everywhere

            sign_out_everywhere(uid, reason=REASON_PASSWORD_CHANGE)
            return render_template(
                "account_profile.html",
                **_account_profile_context(uid, password_success=True),
            )

        username_in = (request.form.get("username") or "").strip()
        email_in = (request.form.get("email") or "").strip()
        _before = get_user_profile(uid) or {}
        if is_placeholder_email(_before.get("email")):
            # A phone account has no email to edit (card S7): the form has no email field, and a
            # posted one is ignored, so the placeholder is neither shown nor replaced.
            email_in = (_before.get("email") or "").strip()
        old_email = (_before.get("email") or "").strip()
        email_changing = email_in.strip().lower() != old_email.lower()
        err = _email_change_reauth_error(uid) if email_changing else None
        if not err:
            err = update_user_profile(uid, username_in, email_in)
        if err:
            return render_template(
                "account_profile.html",
                **_account_profile_context(
                    uid,
                    profile_error=err,
                    username=username_in,
                    email=public_email(email_in),
                ),
            )
        sync_env_admin_user_row(uid)
        u = get_user_profile(uid) or {}
        if email_changing and (u.get("email") or "").strip().lower() != old_email.lower():
            _notify_old_email(old_email)
        session["username"] = u.get("username")
        session["user_email"] = public_email((u.get("email") or "").strip())
        session["user_role"] = normalize_role(u.get("role"))
        return render_template(
            "account_profile.html",
            **_account_profile_context(uid, profile_success=True),
        )

    return render_template("account_profile.html", **_account_profile_context(uid))


def account_billing_page():
    """Plan summary and Stripe Customer Portal entry (C3 scaffold)."""
    uid = session.get("user_id")
    if not uid:
        return redirect(url_for("login_page", next="/account/billing"))
    uid = int(uid)
    from backend.billing.stripe_billing import billing_enabled

    if not billing_enabled():
        # The site is free: no plan, price or portal to show. (Everything below is for billing on.)
        return redirect(url_for("account_profile_page"))
    from backend.billing.catalog import get_plan
    from backend.billing.entitlements import FEATURE_LABELS
    from backend.db.users_db import get_user_billing_snapshot

    billing = get_user_billing_snapshot(uid) or {}
    plan_id = (billing.get("subscription_plan_id") or "").strip().lower()
    if not plan_id:
        plan_id = "complete" if billing.get("is_premium") else "free"
    plan = get_plan(plan_id)
    feats = sorted(paid_access.current_access().entitlements())
    feat_labels = [FEATURE_LABELS.get(f, f.replace("_", " ").title()) for f in feats]
    has_portal = bool(
        billing_enabled()
        and (billing.get("premium_stripe_customer_id") or "").strip()
    )
    return render_template(
        "account_billing.html",
        billing_enabled=billing_enabled(),
        plan=plan,
        plan_id=plan_id,
        is_premium=bool(billing.get("is_premium")),
        feature_labels=feat_labels,
        has_portal=has_portal,
        stripe_configured=billing_enabled(),
    )


def account_billing_portal():
    uid = session.get("user_id")
    if not uid:
        return redirect(url_for("login_page", next="/account/billing"))
    from backend.billing.stripe_billing import billing_enabled, create_customer_portal_session
    from backend.db.users_db import get_user_billing_snapshot

    if not billing_enabled():
        return redirect(url_for("account_billing_page"))
    billing = get_user_billing_snapshot(int(uid)) or {}
    customer_id = (billing.get("premium_stripe_customer_id") or "").strip()
    if not customer_id:
        return redirect(url_for("account_billing_page"))
    try:
        portal = create_customer_portal_session(request=request, customer_id=customer_id)
        url = (portal.get("url") or "").strip()
        if url:
            return redirect(url)
    except Exception:
        _logger.exception("customer portal session failed user_id=%s", uid)
    return redirect(url_for("account_billing_page"))


def register(app) -> None:
    """Attach the account routes to ``app`` keeping the original bare endpoint names."""
    app.add_url_rule("/account/password", endpoint="account_password_page", view_func=account_password_page, methods=["GET", "POST"])
    app.add_url_rule("/account/profile", endpoint="account_profile_page", view_func=account_profile_page, methods=["GET", "POST"])
    app.add_url_rule("/account/billing", endpoint="account_billing_page", view_func=account_billing_page)
    app.add_url_rule("/account/billing/portal", endpoint="account_billing_portal", view_func=account_billing_portal)
