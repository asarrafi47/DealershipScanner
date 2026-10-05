"""Account pages: profile + password, billing summary and Stripe portal entry.

Moved out of ``backend/main.py`` (monolith audit W1). ``register(app)`` keeps the
original bare endpoint names (templates and the CSRF hook match them).
"""

from __future__ import annotations

import logging

from flask import redirect, render_template, request, session, url_for

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
        "email": (u.get("email") or "").strip(),
        "role": role,
        "role_label": role_labels.get(role, role.replace("_", " ").title()),
        "is_premium": bool(u.get("is_premium")),
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
            return render_template(
                "account_profile.html",
                **_account_profile_context(uid, password_success=True),
            )

        username_in = (request.form.get("username") or "").strip()
        email_in = (request.form.get("email") or "").strip()
        err = update_user_profile(uid, username_in, email_in)
        if err:
            return render_template(
                "account_profile.html",
                **_account_profile_context(
                    uid,
                    profile_error=err,
                    username=username_in,
                    email=email_in,
                ),
            )
        sync_env_admin_user_row(uid)
        u = get_user_profile(uid) or {}
        session["username"] = u.get("username")
        session["user_email"] = (u.get("email") or "").strip()
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
    from backend.billing.catalog import get_plan
    from backend.billing.entitlements import FEATURE_LABELS
    from backend.billing.stripe_billing import billing_enabled
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
